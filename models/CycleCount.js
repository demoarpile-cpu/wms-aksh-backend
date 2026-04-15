const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const CycleCount = sequelize.define('CycleCount', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  referenceNumber: { type: DataTypes.STRING, allowNull: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  countName: { type: DataTypes.STRING, allowNull: false },
  countType: { type: DataTypes.STRING, allowNull: true },
  locationId: { type: DataTypes.INTEGER, allowNull: true },
  scheduledDate: { type: DataTypes.DATEONLY, allowNull: true },
  notes: { type: DataTypes.TEXT, allowNull: true },
  status: {
    type: DataTypes.STRING,
    defaultValue: 'PENDING',
    validate: { isIn: [['PENDING', 'IN_PROGRESS', 'COMPLETED']] },
  },
  itemsCount: { type: DataTypes.INTEGER, defaultValue: 0 },
  discrepancies: { type: DataTypes.INTEGER, defaultValue: 0 },
  countedBy: { type: DataTypes.INTEGER, allowNull: true },
}, {
  tableName: 'cycle_counts',
  timestamps: true,
  underscored: true,
});

module.exports = CycleCount;
