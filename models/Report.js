const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Report = sequelize.define('Report', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  reportName: { type: DataTypes.STRING, allowNull: false },
  reportType: {
    type: DataTypes.STRING,
    allowNull: false,
    validate: { isIn: [['INVENTORY', 'ORDERS', 'FINANCIAL', 'PERFORMANCE']] },
  },
  category: { type: DataTypes.STRING, allowNull: true },
  startDate: { type: DataTypes.DATEONLY, allowNull: true },
  endDate: { type: DataTypes.DATEONLY, allowNull: true },
  format: { type: DataTypes.STRING, defaultValue: 'PDF', validate: { isIn: [['PDF', 'CSV', 'EXCEL']] } },
  schedule: { type: DataTypes.STRING, defaultValue: 'ONCE', validate: { isIn: [['ONCE', 'DAILY', 'WEEKLY', 'MONTHLY']] } },
  status: { type: DataTypes.STRING, defaultValue: 'COMPLETED', validate: { isIn: [['PENDING', 'COMPLETED', 'FAILED']] } },
  content: { type: DataTypes.TEXT, allowNull: true },
  lastRunAt: { type: DataTypes.DATE, allowNull: true },
}, {
  tableName: 'reports',
  timestamps: true,
  underscored: true,
});

module.exports = Report;
