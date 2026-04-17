const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Product = sequelize.define('Product', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  categoryId: { type: DataTypes.INTEGER, allowNull: true },
  name: { type: DataTypes.STRING, allowNull: false },
  sku: { type: DataTypes.STRING, allowNull: false },
  barcode: { type: DataTypes.STRING },
  description: { type: DataTypes.TEXT, allowNull: true },
  color: { type: DataTypes.STRING, allowNull: true },
  productType: { type: DataTypes.STRING, allowNull: true },
  unitOfMeasure: { type: DataTypes.STRING, allowNull: true },
  price: { type: DataTypes.DECIMAL(12, 2), defaultValue: 0 },
  costPrice: { type: DataTypes.DECIMAL(12, 2), allowNull: true },
  vatRate: { type: DataTypes.DECIMAL(5, 2), allowNull: true },
  vatCode: { type: DataTypes.STRING, allowNull: true },
  customsTariff: { type: DataTypes.STRING, allowNull: true },
  marketplaceSkus: { type: DataTypes.JSON, allowNull: true },
  heatSensitive: { type: DataTypes.STRING, allowNull: true },
  perishable: { type: DataTypes.STRING, allowNull: true },
  requireBatchTracking: { type: DataTypes.STRING, allowNull: true },
  shelfLifeDays: { type: DataTypes.INTEGER, allowNull: true },
  length: { type: DataTypes.DECIMAL(10, 2), allowNull: true },
  width: { type: DataTypes.DECIMAL(10, 2), allowNull: true },
  height: { type: DataTypes.DECIMAL(10, 2), allowNull: true },
  dimensionUnit: { type: DataTypes.STRING, allowNull: true },
  weight: { type: DataTypes.DECIMAL(10, 3), allowNull: true },
  weightUnit: { type: DataTypes.STRING, allowNull: true },
  reorderLevel: { type: DataTypes.INTEGER, defaultValue: 0 },
  reorderQty: { type: DataTypes.INTEGER, allowNull: true },
  maxStock: { type: DataTypes.INTEGER, allowNull: true },
  status: { type: DataTypes.STRING, defaultValue: 'ACTIVE' },
  images: { type: DataTypes.JSON, allowNull: true },
  supplierId: { type: DataTypes.INTEGER, allowNull: true },
  cartons: { type: DataTypes.JSON, allowNull: true },
  priceLists: { type: DataTypes.JSON, allowNull: true },
  supplierProducts: { type: DataTypes.JSON, allowNull: true },
  alternativeSkus: { type: DataTypes.JSON, allowNull: true },
  packSize: { type: DataTypes.INTEGER, defaultValue: 1 },
  bestBeforeDateWarningPeriodDays: { type: DataTypes.INTEGER, defaultValue: 0 },
}, {

  tableName: 'products',
  timestamps: true,
  underscored: true,
  indexes: [
    { name: 'idx_products_company', fields: ['company_id'] },
    { name: 'idx_products_sku', fields: ['sku'] },
  ],
});

module.exports = Product;
