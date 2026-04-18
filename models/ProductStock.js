const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const ProductStock = sequelize.define('ProductStock', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  warehouseId: { type: DataTypes.INTEGER, allowNull: false },
  locationId: { type: DataTypes.INTEGER, allowNull: true },
  quantity: { type: DataTypes.INTEGER, defaultValue: 0 },
  reserved: { type: DataTypes.INTEGER, defaultValue: 0 },
  status: { type: DataTypes.STRING, defaultValue: 'ACTIVE' },
  lotNumber: { type: DataTypes.STRING, allowNull: true },
  batchId: { type: DataTypes.INTEGER, allowNull: true },
  batchNumber: { type: DataTypes.STRING, allowNull: true },
  serialNumber: { type: DataTypes.STRING, allowNull: true },
  bestBeforeDate: { type: DataTypes.DATEONLY, allowNull: true },
  clientId: { type: DataTypes.INTEGER, allowNull: true },
  reason: { type: DataTypes.STRING, allowNull: true },
  userId: { type: DataTypes.INTEGER, allowNull: true },
}, {
  tableName: 'product_stocks',
  timestamps: true,
  underscored: true,
  indexes: [
    { name: 'idx_product_stocks_client', fields: ['client_id'] },
    { name: 'idx_product_stocks_product', fields: ['product_id'] },
    { name: 'idx_product_stocks_warehouse', fields: ['warehouse_id'] },
  ],
});

module.exports = ProductStock;
