const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const BundleItem = sequelize.define('BundleItem', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  bundleId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  quantity: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 1 },
}, {
  tableName: 'bundle_items',
  timestamps: true,
  underscored: true,
});

module.exports = BundleItem;
