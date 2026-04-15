const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const PickList = sequelize.define('PickList', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  salesOrderId: { type: DataTypes.INTEGER, allowNull: false },
  warehouseId: { type: DataTypes.INTEGER, allowNull: false },
  assignedTo: { type: DataTypes.INTEGER, allowNull: true },
  status: {
    type: DataTypes.STRING,
    defaultValue: 'NOT_STARTED',
    validate: { isIn: [['NOT_STARTED', 'ASSIGNED', 'PARTIALLY_PICKED', 'PICKED']] },
  },
}, {
  tableName: 'pick_lists',
  timestamps: true,
  underscored: true,
});

module.exports = PickList;
