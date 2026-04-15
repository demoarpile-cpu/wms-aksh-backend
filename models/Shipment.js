const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Shipment = sequelize.define('Shipment', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  salesOrderId: { type: DataTypes.INTEGER, allowNull: false },
  companyId: { type: DataTypes.INTEGER, allowNull: false },
  packedBy: { type: DataTypes.INTEGER, allowNull: true },
  courierName: { type: DataTypes.STRING },
  trackingNumber: { type: DataTypes.STRING },
  weight: { type: DataTypes.DECIMAL(10, 2) },
  dispatchDate: { type: DataTypes.DATEONLY },
  deliveryStatus: {
    type: DataTypes.STRING,
    defaultValue: 'READY_TO_SHIP',
    validate: { isIn: [['READY_TO_SHIP', 'SHIPPED', 'IN_TRANSIT', 'DELIVERED', 'FAILED', 'RETURNED']] },
  },
  stockDeducted: { type: DataTypes.BOOLEAN, defaultValue: false },
}, {
  tableName: 'shipments',
  timestamps: true,
  underscored: true,
});

module.exports = Shipment;
