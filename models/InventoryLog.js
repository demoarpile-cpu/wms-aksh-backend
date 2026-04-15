const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const InventoryLog = sequelize.define('InventoryLog', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  warehouseId: { type: DataTypes.INTEGER, allowNull: false },
  type: {
    type: DataTypes.STRING,
    allowNull: false,
    validate: { isIn: [['IN', 'OUT', 'TRANSFER']] },
  },
  quantity: { type: DataTypes.INTEGER, allowNull: false },
  referenceId: { type: DataTypes.STRING, allowNull: true },
  locationId: { type: DataTypes.INTEGER, allowNull: true },
  batchId: { type: DataTypes.INTEGER, allowNull: true },
  batchNumber: { type: DataTypes.STRING, allowNull: true },
  bestBeforeDate: { type: DataTypes.DATEONLY, allowNull: true },
  userId: { type: DataTypes.INTEGER, allowNull: true },
  reason: { type: DataTypes.STRING, allowNull: true },
  clientId: { type: DataTypes.INTEGER, allowNull: true },
}, {
  tableName: 'inventory_logs',
  timestamps: true,
  underscored: true,
});

module.exports = InventoryLog;
