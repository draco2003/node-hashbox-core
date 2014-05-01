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

var daysBeforeExpires, daysBeforeStale, alertLastSent;

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

HashBoxCore.triggerAlert = function(options, callback) {
  var alertInterval = options.alertInterval || 1000;
  var now = moment().unix();
  if (_.isUndefined(alertLastSent)) {
    debug('Sending Alert');
    alertLastSent = now;
  } else if ((now - alertLastSent) > alertInterval) {
    debug('Its time to send a new alert');
    alertLastSent = now;
  } else {
    debug('Alert triggered, but sent to recently.');
  }

  var err = false;
  if (err) {
    callback(err, null);
  } else {
    callback(err, null);
  }
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
  debug('StaleTimestamp: ' + staleTimestamp + ' & ExpiredTimestamp: ' + expiredTimestamp );
  db.all(
    'SELECT * FROM Hash ' +
    'inner join HashVerify on Hash.id = HashVerify.hashId ' +
    'WHERE confirmedStaleAt is NULL ' +
    'AND ( Hash.createdAt > ? OR HashVerify.updatedAt < ? ) ' +
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
                HashBoxCore.triggerAlert({}, function(err, results) {
                  console.log(err);
                  console.log(results);
                  fnSuccess();
                });
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
  var limit = options.count || queryDefaults.count;
  var page = options.page || queryDefaults.page;
  var offset = limit * (page - 1);

  var expiredTimestamp = moment().subtract('days', daysBeforeExpires).unix();

  db.all(
    'SELECT * FROM Hash ' +
    'inner join HashAudit on Hash.id = HashAudit.hashId ' +
    'WHERE confirmedAt is NULL ' +
    'AND Hash.createdAt > ? ' +
    'LIMIT ? OFFSET ?',
    [expiredTimestamp, limit, offset], function(err, rows) {
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

  var expiredTimestamp = moment().subtract('days', daysBeforeExpires).unix();
  db.all(
    'SELECT Hash.*, HashVerify.confirmedStaleAt, HashAudit.confirmedAt FROM Hash ' +
    'LEFT JOIN HashVerify on Hash.id = HashVerify.hashId ' +
    'LEFT JOIN HashAudit on Hash.id = HashAudit.hashId ' +
    'WHERE Hash.createdAt > ? ' +
    'AND confirmedAt is NULL ' +
    'AND confirmedStaleAt is NULL ' +
    'LIMIT ? OFFSET ?', [expiredTimestamp, limit, offset], function(err, rows) {
    if (err) {
      callback(err, null);
    } else {
      callback(err, rows);
    }
  });

};

HashBoxCore.acknowledge = function(options, hashIds, state, callback) {
  debug('acknowledge');
  var err = ''
  , errors = [];

  var asyncReturn = _.after(hashIds.length, function() {
    console.log('returned');
    callback(err, errors);
  });

  if (state === 'stale' || state === 'invalid') {
    if (_.isArray(hashIds) && !_.isEmpty(hashIds)) {
      console.log('everything looks good');
      var newDate = new Date();
      var newTime = newDate.getTime();
      _.forEach(hashIds, function(hashId) {
        console.log(hashId);
        console.log(state);
        if (state === 'stale') {
          debug('updating verify entry');
          db.run('UPDATE HashVerify SET confirmedStaleAt = ? WHERE id = ?;', [newTime, hashId], function(err) {
            if (err) {
              fnErrors(err);
            } else {
              asyncReturn();
            }
          });
        } else {
          debug('updating audit entry');
          db.run('UPDATE HashAudit SET confirmedAt = ? WHERE id = ?;', [newTime, hashId], function(err) {
            if (err) {
              fnErrors(err);
            } else {
              // Get the New confirmed Hash and Key based off of the HashAudit Entry
              db.get('SELECT key, HashAudit.hash FROM Hash JOIN HashAudit ON HashAudit.hashId = Hash.id WHERE HashAudit.id = ?',
                [hashId], function(err, hashAuditRow) {
                var newDate = new Date();
                var newTime = newDate.getTime();
                if (err) {
                  fnErrors(err);
                } else {
                  // Create a new record  in the Hash Table with the associated new key and hash
                  db.run('INSERT INTO Hash (key, hash, createdAt) VALUES (?, ?, ?);',
                      [hashAuditRow.key, hashAuditRow.hash, newTime], function(err) {
                    if (err) {
                      fnErrors(err);
                    } else {
                      asyncReturn();
                    }
                  });
                }
              });
            }
          });
        }
      });
    } else {
      console.log('bad input');
    }
  } else {
    console.log('bad input');
  }
};

HashBoxCore.hashDetail = function(options, hashId, callback) {
  debug('hashDetails');
  if (typeof options === 'function') {
    callback = options;
    options =  queryDefaults;
  }
  var results = {};

  if (_.isNumber(hashId) && hashId > 0) {
    db.get('SELECT * FROM Hash WHERE id = ?', [hashId], function(err, hashRow) {
      if (err || _.isUndefined(hashRow)) {
        if (!err) {
          var err = 'not a valid hashId';
        }
        callback(err, null);
      } else {
        results.hashRow = hashRow;
        db.all('SELECT * FROM Hash WHERE key = ? AND id != ?' , [hashRow.key, hashId], function(err, keyRows) {
          if (err) {
            callback(err, null);
          } else {
            results.keyRows = keyRows;
            db.all('SELECT * FROM HashVerify WHERE hashId IN (SELECT id FROM Hash WHERE key = ?)' , [hashRow.key], function(err, hashVerifyRows) {
              if (err) {
                callback(err, null);
              } else {
                results.hashVerifyRows = hashVerifyRows;
                db.all('SELECT * FROM HashAudit WHERE hashId IN (SELECT id FROM Hash WHERE key = ?)' , [hashRow.key], function(err, hashAuditRows) {
                  if (err) {
                    callback(err, null);
                  } else {
                    results.hashAuditRows = hashAuditRows;
                    console.log(results);
                    callback(err, results);
                  }
                });
              }
            });
          }
        });
      }
    });
  } else {
    callback('Id not a valid HashId', null);
  }
};

module.exports = HashBoxCore;