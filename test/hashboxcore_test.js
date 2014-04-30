
var options = {
  database: "./database/hash_test.db"
};

var assert = require('assert')
  , HashBoxCore = require('..');

HashBoxCore.init(options);

describe('HashBoxCore', function() {
  describe('verify_hash', function() {
    it('should handle inserting hash', function(done) {
      HashBoxCore.hashVerify('filename', 'hashtext', function(err) {
        assert.equal(err, null);
        done();
      });
    });
    it('should handle inserting same hash again', function(done) {
      HashBoxCore.hashVerify('filename', 'hashtext', function(err) {
        assert.equal(err, null);
        done();
      });
    });
    it('should handle inserting same file different hash', function(done) {
      HashBoxCore.hashVerify('filename', 'hashtext1', function(err) {
        assert.equal(err, null);
        done();
      });
    });
    it('should handle inserting new file existing hash', function(done) {
      HashBoxCore.hashVerify('filename', 'hashtext1', function(err) {
        assert.equal(err, null);
        done();
      });
    });
  });
});