const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Customer = sequelize.define('Customer', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  code: { type: DataTypes.STRING, allowNull: true },
  name: { type: DataTypes.STRING, allowNull: false },
  type: { type: DataTypes.STRING, allowNull: true }, // B2B, B2C
  contactPerson: { type: DataTypes.STRING, allowNull: true },
  email: { type: DataTypes.STRING, allowNull: true },
  phone: { type: DataTypes.STRING, allowNull: true },
  country: { type: DataTypes.STRING, allowNull: true },
  state: { type: DataTypes.STRING, allowNull: true },
  city: { type: DataTypes.STRING, allowNull: true },
  address: { type: DataTypes.TEXT, allowNull: true },
  postcode: { type: DataTypes.STRING, allowNull: true },
  tier: { type: DataTypes.STRING, allowNull: true }, // Gold, Premium, Standard, etc.
  segment: { type: DataTypes.STRING, allowNull: true },
  creditLimit: { type: DataTypes.DECIMAL(15, 2), allowNull: true, defaultValue: 0 },
  paymentTerms: { type: DataTypes.STRING, allowNull: true }, // COD, Net 30, etc.
  status: { type: DataTypes.STRING, allowNull: true, defaultValue: 'ACTIVE' }, // ACTIVE, INACTIVE
}, {
  tableName: 'customers',
  timestamps: true,
  underscored: true,
});

module.exports = Customer;
