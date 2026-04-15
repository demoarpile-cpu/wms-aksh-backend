const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Batch = sequelize.define('Batch', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  batchNumber: { type: DataTypes.STRING, allowNull: false },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  warehouseId: { type: DataTypes.INTEGER, allowNull: false },
  locationId: { type: DataTypes.INTEGER, allowNull: true },
  quantity: { type: DataTypes.INTEGER, defaultValue: 0 },
  reserved: { type: DataTypes.INTEGER, defaultValue: 0 },
  unitCost: { type: DataTypes.DECIMAL(12, 2), allowNull: true },
  receivedDate: { type: DataTypes.DATEONLY, allowNull: true },
  expiryDate: { type: DataTypes.DATEONLY, allowNull: true },
  manufacturingDate: { type: DataTypes.DATEONLY, allowNull: true },
  supplierId: { type: DataTypes.INTEGER, allowNull: true },
  status: {
    type: DataTypes.STRING,
    defaultValue: 'ACTIVE',
    validate: { isIn: [['ACTIVE', 'DEPLETED', 'EXPIRED', 'QUARANTINED']] },
  },
}, {
  tableName: 'batches',
  timestamps: true,
  underscored: true,
});

module.exports = Batch;
