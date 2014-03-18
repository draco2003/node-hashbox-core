'use strict';
var _ = require('lodash')
  , moment = require('moment')
  , debug = require('debug')('hashbox-core');

var sqlite3 = require('sqlite3').verbose();

var db;
var HashBoxCore = {};

var queryDefaults = {
  daysBeforeStale: 1,
  daysBeforeExpires: 365,
  page: 1,
  count: 100
};

var daysBeforeExpires, daysBeforeStale;

HashBoxCore.init = function(options) {
  var dbFile = options.database || './database/hashes.db';
  daysBeforeExpires = options.daysBeforeExpires || queryDefaults.daysBeforeExpires;
  daysBeforeStale = options.daysBeforeStale || queryDefaults.daysBeforeStale;

  function createDb() {
    debug('setting up DB');
    db = new sqlite3.Database(dbFile, createTable);
  }

  function createTable() {
    debug('creating tables if needed');
    db.run(
      'CREATE TABLE IF NOT EXISTS Hash ( ' +
      'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
      'key VARCHAR(255) NOT NULL,' +
      'hash VARCHAR(255) NOT NULL,' +
      'createdAt INTEGER NOT NULL)'
    );
    db.run(
      'CREATE TABLE IF NOT EXISTS HashAudit ( ' +
      'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
      'hash VARCHAR(255) NOT NULL,' +
      'createdAt INTEGER NOT NULL,' +
      'updatedAt INTERGER NOT NULL,' +
      'confirmedAt INTEGER NULL,' +
      'hashId INTEGER NOT NULL)'
    );
    db.run(
      'CREATE TABLE IF NOT EXISTS HashVerify ( ' +
      'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
      'createdAt INTEGER NOT NULL,' +
      'updatedAt INTEGER NOT NULL,' +
      'confirmedStaleAt INTEGER NULL,' +
      'hashId INTEGER NOT NULL)'
    );
  }

  createDb();
};


HashBoxCore.listStaleHashes = function(options, callback) {
  debug('listStaleHashes');
  if (typeof options === 'function') {
    callback = options;
    options =  queryDefaults;
  }

  var staleTimestamp = moment().subtract('days', daysBeforeStale).unix();
  var expiredTimestamp = moment().subtract('days', daysBeforeExpires).unix();
  var limit = options.count || queryDefaults.count;
  var page = options.page || queryDefaults.page;
  var offset = limit * (page - 1);

  db.all(
    'SELECT * FROM Hash ' +
    'inner join HashVerify on Hash.id = HashVerify.hashId ' +
    'WHERE confirmedStaleAt is NULL ' +
    'AND createdAt > ? AND updatedAt < ? ' +
    'LIMIT ? OFFSET ?',
    [expiredTimestamp, staleTimestamp, limit, offset], function(err, rows) {
    if (err) {
      callback(err, null);
    } else {
      callback(err, rows);
    }
  });
};

HashBoxCore.hashVerify = function(key, hash, callback) {

  var fnErrors = function(err) {
                    debug('DB Error');
                    console.error(err);
                    callback(err);
                  };

  var fnSuccess = function() {
                    callback(null);
                  };

  // Find the most recent Hash Entry via the Key.
  // If it doesn't exist, create it, with the provided Hash
  // If it does exist, check to see if the hashes match.
  // If they don't insert the new Hash entry, and log a HashAudit entry.
  db.get('SELECT * FROM Hash WHERE key = ? Order By createdAt DESC',
    [key], function(err, hashRow) {
    var newDate = new Date();
    var newTime = newDate.getTime();

    if (err) {
      fnErrors(err);
    } else if (_.isUndefined(hashRow)) {
      debug('new file');
      db.run('INSERT INTO Hash (key, hash, createdAt) VALUES (?, ?, ?);',
        [key, hash, newTime], function(err) {
        if (err) {
          fnErrors(err);
        } else {
          fnSuccess();
        }
      });
    } else {
      debug('file exists');
      if (_.isEqual(hashRow.hash, hash)) {
        debug('hashes are the same');
        db.get('SELECT id, createdAt, updatedAt, HashId FROM HashVerify WHERE hashId = ?', [hashRow.id], function(err, verifyRow) {
          if (err) {
            fnErrors(err);
          } else if (_.isUndefined(verifyRow)) {
            debug('no verify entry yet');
            db.run('INSERT INTO HashVerify (createdAt, updatedAt, hashId) VALUES (?, ?, ?);', [newTime, newTime, hashRow.id], function(err) {
              if (err) {
                fnErrors(err);
              } else {
                fnSuccess();
              }
            });
          } else {
            debug('updating verify entry');
            db.run('UPDATE HashVerify SET updatedAt = ? WHERE hashId = ?;', [newTime, hashRow.id], function(err) {
              if (err) {
                fnErrors(err);
              } else {
                fnSuccess();
              }
            });
          }
        });
      } else {
        debug('hashes are different');
        db.get('SELECT id, hash, createdAt, updatedAt, HashId FROM HashAudit WHERE HashId = ? AND hash = ?',
          [hashRow.id, hash], function(err, auditRow) {
          if (err) {
            fnErrors(err);
          } else if (_.isUndefined(auditRow)) {
            debug('no audit entry yet');
            db.run('INSERT INTO HashAudit (hash, createdAt, updatedAt, hashId) VALUES (?, ?, ?, ?);',
              [hash, newTime, newTime, hashRow.id], function(err) {
              if (err) {
                fnErrors(err);
              } else {
                fnSuccess();
              }
            });
          } else {
            debug('updating audit entry');
            db.run('UPDATE `HashAudit` SET `updatedAt` = ? WHERE `HashId` = ?;',
              [newTime, hashRow.id], function(err) {
              if (err) {
                fnErrors(err);
              } else {
                fnSuccess();
              }
            });
          }
        });
      }
    }
  });
};

HashBoxCore.listAuditRecords = function(options, callback) {
  debug('listAuditRecords');
  if (typeof options === 'function') {
    callback = options;
    options =  queryDefaults;
  }

  var staleTimestamp = moment().subtract('days', daysBeforeStale).unix();
  var expiredTimestamp = moment().subtract('days', daysBeforeExpires).unix();

  db.all(
    'SELECT * FROM Hash ' +
    'inner join HashAudit on Hash.id = HashAudit.hashId ' +
    'WHERE confirmedAt is NULL ' +
    'AND createdAt > ?',
    [expiredTimestamp], function(err, rows) {
    if (err) {
      callback(err, null);
    } else {
      callback(err, rows);
    }
  });

};

HashBoxCore.listHashes = function(options, callback) {
  debug('listHashes');
  if (typeof options === 'function') {
    callback = options;
    options =  queryDefaults;
  }

  var limit = options.count || queryDefaults.count;
  var page = options.page || queryDefaults.page;
  var offset = limit * (page - 1);

  var staleTimestamp = moment().subtract('days', daysBeforeStale).unix();
  var expiredTimestamp = moment().subtract('days', daysBeforeExpires).unix();

  db.all(
    'SELECT * FROM Hash WHERE createdAt > ? ' +
    'LIMIT ? OFFSET ?', [expiredTimestamp, limit, offset], function(err, rows) {
    if (err) {
      callback(err, null);
    } else {
      callback(err, rows);
    }
  });

};

HashBoxCore.hashDetail = function(options, hashId, callback) {
  debug('hashDetails');
  if (typeof options === 'function') {
    callback = options;
    options =  queryDefaults;
  }

  db.get('SELECT * FROM Hash WHERE id = ?', [hashId], function(err, hashRow) {
    if (err) {
      callback(err, null);
    } else {
      db.all(
        'SELECT Hash.id, Hash.key, Hash.hash, Hash.createdAt, ' +
        'HashVerify.createdAt as verifyCreatedAt, ' +
        'HashVerify.updatedAt as verifyUpdatedAt, ' +
        'HashVerify.confirmedStaleAt, ' +
        'HashAudit.hash as auditHash, ' +
        'HashAudit.createdAt as auditCreatedAt, ' +
        'HashAudit.updatedAt as auditUpdatedAt, ' +
        'HashAudit.confirmedAt as auditConfirmedAt ' +
        'FROM Hash ' +
        'LEFT JOIN HashVerify on Hash.id = HashVerify.hashId ' +
        'LEFT JOIN HashAudit on Hash.id = HashAudit.hashId ' +
        'WHERE key = ?', [hashRow.key], function(err, keyRows) {
        if (err) {
          callback(err, null);
        } else {
          callback(err, keyRows);
        }
      });
    }
  });
};

module.exports = HashBoxCore;