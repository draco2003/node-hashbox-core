var fs        = require('fs')
  , path      = require('path')
  , Sequelize = require('sequelize')
  , _         = require('lodash');

var HashBoxModels = function(options) {
  var dbFile = options.database || "./database/hashes.db";
  var sequelize = new Sequelize('hashes', null, null, {
      dialect: "sqlite",
      storage: dbFile
    })
  , db = {}

  fs
    .readdirSync(__dirname)
    .filter(function(file) {
      return (file.indexOf('.') !== 0) && (file !== 'index.js')
    })
    .forEach(function(file) {
      var model = sequelize.import(path.join(__dirname, file))
      db[model.name] = model
    })

  //Setup Relationships
  //ToDo: Would be nice if this was dynamic
  db.Hash.hasMany(db.HashAudit);
  db.Hash.hasOne(db.HashVerify);

  return  _.extend({
    sequelize: sequelize,
    Sequelize: Sequelize
  }, db);
}

module.exports = HashBoxModels;