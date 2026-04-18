const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Batch = sequelize.define('Batch', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  batchNumber: { type: DataTypes.STRING, allowNull: false },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  clientId: { type: DataTypes.INTEGER, allowNull: true },
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
    validate: {
      isIn: {
        args: [['ACTIVE', 'DEPLETED', 'EXPIRED', 'QUARANTINED']],
        msg: "Status must be ACTIVE, DEPLETED, EXPIRED, or QUARANTINED"
      }
    }
  },
  grnId: { type: DataTypes.INTEGER, allowNull: true },
  notes: { type: DataTypes.TEXT, allowNull: true },
}, {
  tableName: 'batches',
  timestamps: true,
  underscored: true,
  indexes: [
    {
      unique: true,
      fields: ['product_id', 'batch_number', 'grn_id'],
      name: 'unique_product_batch_grn'
    }
  ]
});

module.exports = Batch;
