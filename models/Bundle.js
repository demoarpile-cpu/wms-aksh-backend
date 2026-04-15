const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Bundle = sequelize.define('Bundle', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  sku: { type: DataTypes.STRING, allowNull: false },
  name: { type: DataTypes.STRING, allowNull: false },
  description: { type: DataTypes.TEXT, allowNull: true },
  costPrice: { type: DataTypes.DECIMAL(12, 2), defaultValue: 0 },
  sellingPrice: { type: DataTypes.DECIMAL(12, 2), defaultValue: 0 },
  status: { type: DataTypes.STRING, defaultValue: 'ACTIVE' },
}, {
  tableName: 'bundles',
  timestamps: true,
  underscored: true,
});

module.exports = Bundle;
