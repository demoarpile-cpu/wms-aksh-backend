const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });
const { Sequelize } = require('sequelize');

const seq = new Sequelize(
    process.env.DB_NAME || 'warehouse_wms',
    process.env.DB_USER || 'root',
    process.env.DB_PASSWORD || '',
    {
        host: process.env.DB_HOST || 'localhost',
        dialect: 'mysql',
        logging: false,
    }
);

(async () => {
    try {
        await seq.authenticate();
        const [results] = await seq.query('DELETE FROM InventoryAdjustments');
        console.log('✅ All InventoryAdjustment records deleted!');
    } catch (err) {
        // Try lowercase table name
        try {
            const [results2] = await seq.query('DELETE FROM inventory_adjustments');
            console.log('✅ All adjustment records deleted (table: inventory_adjustments)!');
        } catch (err2) {
            console.error('❌ Error:', err.message, '\nAlso tried inventory_adjustments:', err2.message);
        }
    } finally {
        await seq.close();
    }
})();
