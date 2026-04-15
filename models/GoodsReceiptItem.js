const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const GoodsReceiptItem = sequelize.define('GoodsReceiptItem', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  goodsReceiptId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  productName: { type: DataTypes.STRING, allowNull: true },
  productSku: { type: DataTypes.STRING, allowNull: true },
  expectedQty: { type: DataTypes.INTEGER, defaultValue: 0 },
  receivedQty: { type: DataTypes.INTEGER, defaultValue: 0 },
  qtyToBook: { type: DataTypes.INTEGER, defaultValue: 0 },
  batchId: { type: DataTypes.STRING, allowNull: true },
  bestBeforeDate: { type: DataTypes.DATE, allowNull: true },
  locationId: { type: DataTypes.INTEGER, allowNull: true },
  qualityStatus: { type: DataTypes.STRING, defaultValue: 'GOOD' }, // GOOD, DAMAGED
  unitCost: { type: DataTypes.DECIMAL(12, 2), allowNull: true },
}, {
  tableName: 'goods_receipt_items',
  timestamps: true,
  underscored: true,
});

module.exports = GoodsReceiptItem;
