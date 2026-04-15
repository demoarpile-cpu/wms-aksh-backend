const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const Inventory = sequelize.define('Inventory', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  warehouseId: { type: DataTypes.INTEGER, allowNull: false },
  productId: { type: DataTypes.INTEGER, allowNull: false },
  quantity: { type: DataTypes.INTEGER, defaultValue: 0 },
  reservedQuantity: { type: DataTypes.INTEGER, defaultValue: 0 },
  availableQuantity: {
    type: DataTypes.VIRTUAL,
    get() {
      return this.quantity - this.reservedQuantity;
    },
    set(value) {
      throw new Error('Do not try to set the `availableQuantity` value!');
    }
  }
}, {
  tableName: 'inventory',
  timestamps: true,
  underscored: true,
  indexes: [
    {
      unique: true,
      fields: ['warehouse_id', 'product_id']
    }
  ]
});

module.exports = Inventory;
