const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const InventoryAdjustment = sequelize.define('InventoryAdjustment', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  referenceNumber: { type: DataTypes.STRING, allowNull: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  warehouseId: { type: DataTypes.INTEGER, allowNull: false },
  type: {

    type: DataTypes.STRING,
    allowNull: false,
    validate: { isIn: [['INCREASE', 'DECREASE']] },
  },
  quantity: { type: DataTypes.INTEGER, allowNull: false },
  reason: { type: DataTypes.STRING, allowNull: true },
  notes: { type: DataTypes.TEXT, allowNull: true },
  locationId: { type: DataTypes.INTEGER, allowNull: true },
  batchId: { type: DataTypes.INTEGER, allowNull: true },
  batchNumber: { type: DataTypes.STRING, allowNull: true },
  bestBeforeDate: { type: DataTypes.DATEONLY, allowNull: true },
  clientId: { type: DataTypes.INTEGER, allowNull: true },
  status: {
    type: DataTypes.STRING,
    defaultValue: 'PENDING',
    validate: { isIn: [['PENDING', 'COMPLETED']] },
  },
  createdBy: { type: DataTypes.INTEGER, allowNull: true },
}, {
  tableName: 'inventory_adjustments',
  timestamps: true,
  underscored: true,
});

module.exports = InventoryAdjustment;
