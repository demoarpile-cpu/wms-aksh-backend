const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const AuditLog = sequelize.define('AuditLog', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  clientId: { type: DataTypes.INTEGER, allowNull: true },
  userId: { type: DataTypes.INTEGER, allowNull: false },
  action: { type: DataTypes.STRING, allowNull: false }, // CREATE, UPDATE, DELETE, FINALIZE, etc.
  module: { type: DataTypes.STRING, allowNull: false }, // PRODUCT, PO, GRN, INVENTORY, etc.
  referenceId: { type: DataTypes.STRING, allowNull: true }, // ID of the related object
  referenceNumber: { type: DataTypes.STRING, allowNull: true }, // e.g. PO-123, GRN-001
  timestamp: { type: DataTypes.DATE, defaultValue: DataTypes.NOW },
  details: { type: DataTypes.JSON, allowNull: true }, // For storing what changed
  ipAddress: { type: DataTypes.STRING, allowNull: true },
}, {
  tableName: 'audit_logs',
  timestamps: false,
  underscored: true,
});

module.exports = AuditLog;
