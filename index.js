const mysql = require('mysql2');
const util = require('util');
const EventEmitter = require('events').EventEmitter;
const initBinlogClass = require('./lib/sequence/binlog');

const TableInfoQueryTemplate = `SELECT 
  COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, 
  COLUMN_COMMENT, COLUMN_TYPE 
  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' 
  ORDER BY ORDINAL_POSITION`;

function ZongJi(dsn) {
  EventEmitter.call(this);

  this._options({});
  this._filters({});
  this.ctrlCallbacks = [];
  this.tableMap = {};
  this.ready = false;
  this.stopped = false;
  this.useChecksum = false;

  this._establishConnection(dsn);
}

util.inherits(ZongJi, EventEmitter);

// Crear conexión mysql2 con soporte caching_sha2_password
ZongJi.prototype._establishConnection = function (dsn) {
  const createConnection = (options) => {
    const connection = mysql.createConnection({
      ...options,
      authPlugins: {
        caching_sha2_password: mysql.authPlugins.caching_sha2_password
      }
    });

    connection.on('error', this.emit.bind(this, 'error'));
    connection.on('unhandledError', this.emit.bind(this, 'error'));
    return connection;
  };

  this.ctrlConnectionOwner = true;
  this.ctrlConnection = createConnection(dsn);
  this.connection = createConnection(dsn);
};

// Checksum
ZongJi.prototype._isChecksumEnabled = function (next) {
  const SelectChecksumParamSql = 'select @@GLOBAL.binlog_checksum as checksum';
  const SetChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';

  const query = (conn, sql) => {
    return new Promise((resolve, reject) => {
      conn.query(sql, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });
  };

  let checksumEnabled = true;

  query(this.ctrlConnection, SelectChecksumParamSql)
    .then((rows) => {
      if (rows[0].checksum === 'NONE') {
        checksumEnabled = false;
        return query(this.connection, 'SELECT 1');
      }

      if (checksumEnabled) {
        return query(this.connection, SetChecksumSql);
      }
    })
    .catch((err) => {
      if (err.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)) {
        checksumEnabled = false;
        return query(this.connection, 'SELECT 1');
      } else {
        next(err);
      }
    })
    .then(() => {
      next(null, checksumEnabled);
    });
};

// Encuentra el último binlog
ZongJi.prototype._findBinlogEnd = function (next) {
  this.ctrlConnection.query('SHOW BINARY LOGS', (err, rows) => {
    if (err) return next(err);
    next(null, rows.length > 0 ? rows[rows.length - 1] : null);
  });
};

// Información de la tabla
ZongJi.prototype._fetchTableInfo = function (tableMapEvent, next) {
  const sql = util.format(TableInfoQueryTemplate, tableMapEvent.schemaName, tableMapEvent.tableName);

  this.ctrlConnection.query(sql, (err, rows) => {
    if (err) return this.emit('error', err);

    if (rows.length === 0) {
      this.emit(
        'error',
        new Error(
          `Insufficient permissions to access [${tableMapEvent.schemaName}.${tableMapEvent.tableName}], or the table has been dropped.`
        )
      );
      return;
    }

    this.tableMap[tableMapEvent.tableId] = {
      columnSchemas: rows,
      parentSchema: tableMapEvent.schemaName,
      tableName: tableMapEvent.tableName
    };

    next();
  });
};

// Opciones y filtros
ZongJi.prototype._options = function ({ serverId, filename, position, startAtEnd } = {}) {
  this.options = { serverId, filename, position, startAtEnd };
};
ZongJi.prototype._filters = function ({ includeEvents, excludeEvents, includeSchema, excludeSchema } = {}) {
  this.filters = { includeEvents, excludeEvents, includeSchema, excludeSchema };
};

// Obtener opción
ZongJi.prototype.get = function (name) {
  if (typeof name === 'string') return this.options[name];
  if (Array.isArray(name)) {
    return name.reduce((acc, cur) => {
      acc[cur] = this.options[cur];
      return acc;
    }, {});
  }
};

// Start ZongJi
ZongJi.prototype.start = function (options = {}) {
  this._options(options);
  this._filters(options);

  const testChecksum = (resolve, reject) => {
    if (this.stopped) return resolve();
    this._isChecksumEnabled((err, checksumEnabled) => {
      if (err) reject(err);
      else {
        this.useChecksum = checksumEnabled;
        resolve();
      }
    });
  };

  const findBinlogEnd = (resolve, reject) => {
    if (this.stopped) return resolve();
    this._findBinlogEnd((err, result) => {
      if (err) return reject(err);
      if (result) {
        this._options(
          Object.assign({}, options, {
            filename: result.Log_name,
            position: result.File_size
          })
        );
      }
      resolve();
    });
  };

  Promise.all([new Promise(testChecksum), options.startAtEnd ? new Promise(findBinlogEnd) : null].filter(Boolean))
    .then(() => {
      this.BinlogClass = initBinlogClass(this);
      if (!this.stopped) {
        // Llamamos a start() del binlog stream en lugar de _protocol._enqueue
        this.binlogStream = new this.BinlogClass((err, evt) => this.emit('binlog', evt, err));
        if (typeof this.binlogStream.start === 'function') {
          this.binlogStream.start(this.connection);
        }
        this.ready = true;
        this.emit('ready');
      }
    })
    .catch((err) => this.emit('error', err));
};

// Stop / pause / resume
ZongJi.prototype.stop = function () {
  if (!this.stopped) {
    this.stopped = true;
    if (this.connection.destroy) this.connection.destroy();
    if (this.ctrlConnection.query) {
      this.ctrlConnection.query('KILL ' + this.connection.threadId, () => {
        if (this.ctrlConnectionOwner && this.ctrlConnection.destroy) this.ctrlConnection.destroy();
        this.emit('stopped');
      });
    }
  }
};
ZongJi.prototype.pause = function () { if (!this.stopped && this.connection.pause) this.connection.pause(); };
ZongJi.prototype.resume = function () { if (!this.stopped && this.connection.resume) this.connection.resume(); };

// Skip events / schemas
ZongJi.prototype._skipEvent = function (name) {
  const includes = this.filters.includeEvents;
  const excludes = this.filters.excludeEvents;
  const included = !includes || includes.includes(name);
  const excluded = excludes && excludes.includes(name);
  return excluded || !included;
};
ZongJi.prototype._skipSchema = function (db, table) {
  const includes = this.filters.includeSchema;
  const excludes = this.filters.excludeSchema || {};
  const included =
    !includes ||
    (db in includes &&
      (includes[db] === true ||
        (Array.isArray(includes[db]) && includes[db].includes(table)) ||
        (typeof includes[db] === 'function' && includes[db](table))));
  const excluded =
    db in excludes &&
    (excludes[db] === true ||
      (Array.isArray(excludes[db]) && excludes[db].includes(table)) ||
      (typeof excludes[db] === 'function' && excludes[db](table)));
  return excluded || !included;
};

module.exports = ZongJi;
module.exports.ZongJi = ZongJi;
