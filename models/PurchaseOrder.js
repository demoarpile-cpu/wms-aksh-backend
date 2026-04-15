const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const PurchaseOrder = sequelize.define('PurchaseOrder', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  supplierId: { type: DataTypes.INTEGER, allowNull: false },
  poNumber: { type: DataTypes.STRING, allowNull: false },
  status: {
    type: DataTypes.STRING,
    defaultValue: 'pending',
    validate: { isIn: [['draft', 'pending', 'approved', 'rejected', 'received']] },
  },
  totalAmount: { type: DataTypes.DECIMAL(12, 2), defaultValue: 0 },
  expectedDelivery: { type: DataTypes.DATE, allowNull: true },
  warehouseId: { type: DataTypes.INTEGER, allowNull: true },
  clientId: { type: DataTypes.INTEGER, allowNull: true },
  notes: { type: DataTypes.TEXT, allowNull: true },

}, {
  tableName: 'purchase_orders',
  timestamps: true,
  underscored: true,
});

module.exports = PurchaseOrder;
