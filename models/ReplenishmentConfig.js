const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const ReplenishmentConfig = sequelize.define('ReplenishmentConfig', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  minStockLevel: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
  maxStockLevel: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
  reorderPoint: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
  reorderQuantity: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
  autoCreateTasks: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true },
  status: { type: DataTypes.STRING, allowNull: true, defaultValue: 'ACTIVE' }, // ACTIVE, INACTIVE
}, {
  tableName: 'replenishment_configs',
  timestamps: true,
  underscored: true,
});

module.exports = ReplenishmentConfig;
