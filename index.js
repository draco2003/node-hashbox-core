var _ = require('lodash')
  , debug = require('debug')('hashbox-core');

var db;
var HashBoxCore = {};

HashBoxCore.init = function(options) {
  db = require('./models')(options);
  db.sequelize
  .sync()
  //.sync({ force: true }) // this would drop data from our table. better error instead
  .error(function(err) {
          debug('DB Error');
          console.log(err);
        });
};

HashBoxCore.verify_hash = function(key, hash, callback) {

  var fnErrors = function(err) {
                    debug('DB Error');
                    console.log(err);
                    callback(err);
                  };

  var fnSuccess = function() {
                    callback(null);
                  };

  // Find the Hash Entry via the Key.
  // If it doesn't exist, create it, with the provided Hash
  db.Hash.findOrCreate({key: key}, {hash: hash})
  .error(fnErrors)
  .success(function(hashObj, created) {
    // Check to see if we had to create an entry for this hash
    if (created) {
      debug('Created new Hash');
      fnSuccess();
    } else {
      if (_.isEqual(hashObj.hash, hash)) {
        debug('Hashes are equal');
        // Attempt to get the existing HashVerify Entry
        hashObj.getHashVerify()
        .error(fnErrors)
        .success(function(hashVerify) {
          // Check to see if a hashVerify Entry exists
          if (_.isEmpty(hashVerify)) {
            debug('HashVerify entry not found');
            hashObj.createHashVerify()
            .success(fnSuccess)
            .error(fnErrors);
          } else {
            debug('HashVerify entry found');
            hashVerify.save()
            .success(fnSuccess)
            .error(fnErrors);
          }
        });
      } else {
        debug('Hashes are not equal');
        // Get HashAudit entries for this Hash with the same hash value
        hashObj.getHashAudits({where: {hash: hash}})
        .error(fnErrors)
        .success(function(hashAudit) {
          // Check to see if a hashAudit Entry exists for this hash
          if (_.isEmpty(hashAudit)) {
            debug('HashAudit entry not found');
            hashObj.createHashAudit({hash: hash})
            .success(fnSuccess)
            .error(fnErrors);
          } else {
            debug('HashAudit entry found');
            hashAudit[0].save()
            .success(fnSuccess)
            .error(fnErrors);
          }
        });
      }
    }
  });
}

module.exports = HashBoxCore;