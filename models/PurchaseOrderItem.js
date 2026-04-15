const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const PurchaseOrderItem = sequelize.define('PurchaseOrderItem', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  purchaseOrderId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  productName: { type: DataTypes.STRING, allowNull: true },
  productSku: { type: DataTypes.STRING, allowNull: true },
  quantity: { type: DataTypes.INTEGER, allowNull: false },
  supplierQuantity: { type: DataTypes.INTEGER, defaultValue: 0 },
  packSize: { type: DataTypes.INTEGER, defaultValue: 1 },
  unitPrice: { type: DataTypes.DECIMAL(12, 2), defaultValue: 0 },
  totalPrice: { type: DataTypes.DECIMAL(12, 2), defaultValue: 0 },

}, {
  tableName: 'purchase_order_items',
  timestamps: true,
  underscored: true,
});

module.exports = PurchaseOrderItem;
