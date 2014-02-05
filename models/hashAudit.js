module.exports = function(sequelize, DataTypes) {
  var HashAudit = sequelize.define('HashAudit', {
    hash: {type: DataTypes.STRING, allowNull: false},
    confirmed: {type: DataTypes.BOOLEAN, allowNull: false, defaultValue: false}
  });

  return HashAudit;
}
