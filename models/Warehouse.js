const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Warehouse = sequelize.define('Warehouse', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  name: { type: DataTypes.STRING, allowNull: false },
  code: { type: DataTypes.STRING, allowNull: false },
  warehouseType: { type: DataTypes.STRING },
  address: { type: DataTypes.TEXT },
  phone: { type: DataTypes.STRING },
  capacity: { type: DataTypes.INTEGER },
  status: { type: DataTypes.STRING, defaultValue: 'ACTIVE' },
}, {
  tableName: 'warehouses',
  timestamps: true,
  underscored: true,
});

module.exports = Warehouse;
