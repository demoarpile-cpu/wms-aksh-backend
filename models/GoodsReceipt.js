const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const GoodsReceipt = sequelize.define('GoodsReceipt', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  purchaseOrderId: { type: DataTypes.INTEGER, allowNull: false },
  grNumber: { type: DataTypes.STRING, allowNull: false },
  status: {
    type: DataTypes.STRING,
    defaultValue: 'pending',
    validate: { isIn: [['pending', 'in_progress', 'completed']] },
  },
  warehouseId: { type: DataTypes.INTEGER, allowNull: true },
  deliveryType: {
    type: DataTypes.STRING,
    allowNull: true,
    defaultValue: 'carton',
    validate: { isIn: [['pallet', 'carton']] },
  },
  eta: { type: DataTypes.DATE, allowNull: true },
  notes: { type: DataTypes.TEXT, allowNull: true },
  totalExpected: { type: DataTypes.INTEGER, defaultValue: 0 },
  totalReceived: { type: DataTypes.INTEGER, defaultValue: 0 },
  totalToBook: { type: DataTypes.INTEGER, defaultValue: 0 },
}, {
  tableName: 'goods_receipts',
  timestamps: true,
  underscored: true,
});

module.exports = GoodsReceipt;
