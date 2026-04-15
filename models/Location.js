const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Location = sequelize.define('Location', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  zoneId: { type: DataTypes.INTEGER, allowNull: false },
  name: { type: DataTypes.STRING, allowNull: false },
  code: { type: DataTypes.STRING },
  aisle: { type: DataTypes.STRING },
  rack: { type: DataTypes.STRING },
  shelf: { type: DataTypes.STRING },
  bin: { type: DataTypes.STRING },
  locationType: { type: DataTypes.STRING },
  pickSequence: { type: DataTypes.INTEGER },
  maxWeight: { type: DataTypes.DECIMAL(10, 2) },
  heatSensitive: { type: DataTypes.STRING, allowNull: true },
}, {
  tableName: 'locations',
  timestamps: true,
  underscored: true,
});

module.exports = Location;
