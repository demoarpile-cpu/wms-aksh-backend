const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const VatCode = sequelize.define('VatCode', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: true },
  code: { type: DataTypes.STRING, allowNull: false },
  label: { type: DataTypes.STRING },
  ratePercent: { type: DataTypes.DECIMAL(5, 2), allowNull: false, defaultValue: 0 },
  countryCode: { type: DataTypes.STRING(10), defaultValue: 'UK' },
}, {
  tableName: 'vat_codes',
  timestamps: true,
  underscored: true,
});

module.exports = VatCode;
