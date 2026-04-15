const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Zone = sequelize.define('Zone', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: true },
  warehouseId: { type: DataTypes.INTEGER, allowNull: false },
  name: { type: DataTypes.STRING, allowNull: false },
  code: { type: DataTypes.STRING },
  zoneType: { type: DataTypes.STRING },
}, {
  tableName: 'zones',
  timestamps: true,
  underscored: true,
});

module.exports = Zone;
