module.exports = function(sequelize, DataTypes) {
  var Hash = sequelize.define('Hash', {
    key: {type: DataTypes.STRING, allowNull: false, unique: true},
    hash: {type: DataTypes.STRING, allowNull: false},
  });
  return Hash;
}
