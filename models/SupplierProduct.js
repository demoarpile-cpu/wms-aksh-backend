const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const SupplierProduct = sequelize.define('SupplierProduct', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  supplierId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  supplierSku: { type: DataTypes.STRING, allowNull: true },
  supplierProductName: { type: DataTypes.STRING, allowNull: true },
  packSize: { type: DataTypes.INTEGER, defaultValue: 1 },
  costPrice: { type: DataTypes.DECIMAL(10, 2), defaultValue: 0 },
  effectiveDate: { type: DataTypes.DATEONLY, allowNull: true },
}, {
  tableName: 'supplier_products',
  timestamps: true,
  underscored: true,
});

module.exports = SupplierProduct;
