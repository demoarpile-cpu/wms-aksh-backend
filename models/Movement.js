const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Movement = sequelize.define('Movement', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  type: {
    type: DataTypes.STRING,
    allowNull: false,
    validate: { isIn: [['RECEIVE', 'PICK', 'TRANSFER', 'ADJUST', 'RETURN']] },
  },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  batchId: { type: DataTypes.INTEGER, allowNull: true },
  fromWarehouseId: { type: DataTypes.INTEGER, allowNull: true },
  toWarehouseId: { type: DataTypes.INTEGER, allowNull: true },
  fromLocationId: { type: DataTypes.INTEGER, allowNull: true },
  toLocationId: { type: DataTypes.INTEGER, allowNull: true },

  quantity: { type: DataTypes.INTEGER, allowNull: false },
  reason: { type: DataTypes.STRING, allowNull: true },
  notes: { type: DataTypes.TEXT, allowNull: true },
  createdBy: { type: DataTypes.INTEGER, allowNull: true },
}, {
  tableName: 'movements',
  timestamps: true,
  underscored: true,
});

module.exports = Movement;
