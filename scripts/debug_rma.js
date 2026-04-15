const { sequelize, SalesOrder, Shipment } = require('../models');

async function debug() {
    try {
        await sequelize.authenticate();
        console.log('DB Connected.');

        // 1. Fetch all Shipments
        const shipments = await Shipment.findAll();
        console.log('Total Shipments:', shipments.length);
        shipments.forEach(s => {
            console.log(`Shipment ID: ${s.id}, SalesOrderId: ${s.salesOrderId}, Status: ${s.deliveryStatus}`);
        });

        // 2. Fetch SalesOrder for SHI-003 (assuming id 3 or finding by tracking if needed, lets just take the salesOrderId from above)
        // Actually let's fetch ALL orders with their shipments using the same logic as OrderService
        console.log('\n--- Fetching Orders via Association ---');
        const orders = await SalesOrder.findAll({
            include: [{ association: 'Shipment' }]
        });

        orders.forEach(o => {
            const s = o.Shipment;
            console.log(`Order: ${o.orderNumber}, Status: ${o.status}, Shipment: ${s ? s.deliveryStatus : 'NULL'}`);
        });

    } catch (err) {
        console.error('Error:', err);
    } finally {
        process.exit();
    }
}

debug();
