const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');
const SalesOrder = require('./SalesOrder');
const Shipment = require('./Shipment');
const Customer = require('./Customer');

const Return = sequelize.define('Return', {
    id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
    companyId: { type: DataTypes.INTEGER, allowNull: false },
    rmaNumber: { type: DataTypes.STRING, allowNull: false, unique: true },
    salesOrderId: {
        type: DataTypes.INTEGER,
        allowNull: false,
        references: { model: SalesOrder, key: 'id' }
    },
    shipmentId: {
        type: DataTypes.INTEGER,
        allowNull: false,
        references: { model: Shipment, key: 'id' }
    },
    customerId: {
        type: DataTypes.INTEGER,
        allowNull: true,
        references: { model: Customer, key: 'id' }
    },
    status: {
        type: DataTypes.STRING,
        defaultValue: 'RMA_CREATED',
        validate: {
            isIn: [['RMA_CREATED', 'AWAITING_RETURN', 'RECEIVED', 'IN_INSPECTION', 'APPROVED', 'REJECTED', 'REFUNDED', 'CLOSED']]
        }
    },
    returnType: {
        type: DataTypes.STRING,
        allowNull: false,
        validate: { isIn: [['REFUND', 'REPLACE', 'INSPECTION']] }
    },
    reason: { type: DataTypes.STRING, allowNull: false }, // Damaged, Wrong Item, etc.
    recoveryValue: { type: DataTypes.DECIMAL(10, 2), defaultValue: 0 }, // Expected amount to recover/refund
    refundAmount: { type: DataTypes.DECIMAL(10, 2), defaultValue: 0 }, // Actual refunded amount
    notes: { type: DataTypes.TEXT },

    // Timestamps for lifecycle tracking
    receivedAt: { type: DataTypes.DATE },
    inspectedAt: { type: DataTypes.DATE },
    completedAt: { type: DataTypes.DATE },

    createdBy: { type: DataTypes.INTEGER }
}, {
    tableName: 'returns',
    timestamps: true,
    underscored: true
});

// Define Relationships associated with Return
Return.belongsTo(SalesOrder, { foreignKey: 'salesOrderId' });
Return.belongsTo(Shipment, { foreignKey: 'shipmentId' });
Return.belongsTo(Customer, { foreignKey: 'customerId' });

// Also defining the inverse relationships here is good practice if not done in central index
SalesOrder.hasMany(Return, { foreignKey: 'salesOrderId' });
Shipment.hasMany(Return, { foreignKey: 'shipmentId' });

module.exports = Return;
