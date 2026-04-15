const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const ReplenishmentTask = sequelize.define('ReplenishmentTask', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  fromLocationId: { type: DataTypes.INTEGER, allowNull: false },
  toLocationId: { type: DataTypes.INTEGER, allowNull: false },
  taskNumber: { type: DataTypes.STRING, allowNull: false },
  quantityNeeded: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
  quantityCompleted: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
  priority: { type: DataTypes.STRING, allowNull: true, defaultValue: 'MEDIUM' }, // LOW, MEDIUM, HIGH
  notes: { type: DataTypes.TEXT, allowNull: true },
  status: { type: DataTypes.STRING, allowNull: true, defaultValue: 'PENDING' }, // PENDING, IN_PROGRESS, COMPLETED, CANCELLED
}, {
  tableName: 'replenishment_tasks',
  timestamps: true,
  underscored: true,
});

module.exports = ReplenishmentTask;
