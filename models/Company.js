const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Company = sequelize.define('Company', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  name: { type: DataTypes.STRING, allowNull: false },
  code: { type: DataTypes.STRING, allowNull: false },
  email: { type: DataTypes.STRING },
  phone: { type: DataTypes.STRING },
  address: { type: DataTypes.TEXT },
  status: { type: DataTypes.STRING, defaultValue: 'ACTIVE', validate: { isIn: [['ACTIVE', 'INACTIVE']] } },
}, {
  tableName: 'companies',
  timestamps: true,
  underscored: true,
});

module.exports = Company;
