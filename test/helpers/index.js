const mysql = require('mysql2');

const settings = require('../settings/mysql');
const querySequence = require('./querySequence');

const SCHEMA_NAME = settings.connection.database;

exports.SCHEMA_NAME = SCHEMA_NAME;

exports.init = async function (done) {
  const connObj = { ...settings.connection };
  // database doesn't exist at this time
  delete connObj.database;
  const conn = mysql.createConnection(connObj);

  // Syntax for resetting the binlog changed with MySQL 8.4
  const version = await getVersion();
  const cleanupQuery = version >= '8.4' ? 'RESET BINARY LOGS AND GTIDS' : 'RESET MASTER';

  querySequence(
    conn,
    [
      "SET GLOBAL sql_mode = '" + settings.sessionSqlMode + "'",
      `DROP DATABASE IF EXISTS ${SCHEMA_NAME}`,
      `CREATE DATABASE IF NOT EXISTS ${SCHEMA_NAME}`,
      `USE ${SCHEMA_NAME}`,
      cleanupQuery
      // 'SELECT VERSION() AS version'
    ],
    (error) => {
      conn.destroy();
      done(error);
    }
  );
};

exports.execute = function (queries, done) {
  const conn = mysql.createConnection(settings.connection);
  querySequence(conn, queries, (error, result) => {
    conn.destroy();
    done(error, result);
  });
};

const getVersion = function () {
  const connObj = { ...settings.connection };
  // database doesn't exist at this time
  delete connObj.database;
  const conn = mysql.createConnection(connObj);

  return new Promise((resolve, reject) => {
    querySequence(conn, ['SELECT VERSION() AS version'], (err, results) => {
      conn.destroy();

      if (err) {
        console.log('Error getting version:', err);
        reject(err);
      }

      resolve(results[results.length - 1][0].version);
    });
  });
};

const checkVersion = function (expected, actual) {
  const parts = expected.split('.').map((part) => parseInt(part, 10));
  for (let i = 0; i < parts.length; i++) {
    if (actual[i] == parts[i]) {
      continue;
    }
    return actual[i] > parts[i];
  }
  return true;
};

const requireVersion = function (expected, done) {
  const connObj = { ...settings.connection };
  // database doesn't exist at this time
  delete connObj.database;
  const conn = mysql.createConnection(connObj);
  querySequence(conn, ['SELECT VERSION() AS version'], (err, results) => {
    conn.destroy();

    if (err) {
      throw err;
    }

    let ver = results[results.length - 1][0].version
      .split('-')[0]
      .split('.')
      .map((part) => parseInt(part, 10));

    if (checkVersion(expected, ver)) {
      done();
    }
  });
};

exports.requireVersion = requireVersion;

let id = 100;
exports.serverId = function () {
  id++;
  return id;
};

exports.strRepeat = function (pattern, count) {
  if (count < 1) return '';
  let result = '';
  let pos = 0;
  while (pos < count) {
    result += pattern.replace(/##/g, pos);
    pos++;
  }
  return result;
};
