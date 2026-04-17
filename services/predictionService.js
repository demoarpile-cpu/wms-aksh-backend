const { Product, SalesOrder, OrderItem, ProductStock, sequelize } = require('../models');
const { Op } = require('sequelize');
const dayjs = require('dayjs');

/** First displayable image URL or data URL; avoids passing JSON text as img src (causes 431 / huge GET). */
function firstProductImage(images) {
    if (images == null || images === '') return null;
    let list = images;
    if (typeof images === 'string') {
        const s = images.trim();
        if (s.startsWith('http://') || s.startsWith('https://') || s.startsWith('data:image/')) {
            return s;
        }
        try {
            list = JSON.parse(s);
        } catch {
            return null;
        }
    }
    if (!Array.isArray(list) || list.length === 0) return null;
    const first = list[0];
    if (typeof first !== 'string') return null;
    const u = first.trim();
    if (u.startsWith('http://') || u.startsWith('https://') || u.startsWith('data:image/')) return u;
    return null;
}

async function getPredictionData(companyId) {
    // 1. Fetch all active products
    const products = await Product.findAll({
        where: { companyId, status: 'ACTIVE' },
        include: [{ model: ProductStock, as: 'ProductStocks' }]
    });

    // 2. Define date range for historical sales (last 30 days)
    const daysBytes = 30;
    const startDate = dayjs().subtract(daysBytes, 'days').startOf('day').toDate();

    // 3. Fetch sales data (completed orders only)
    // We need to sum up quantities from OrderItems for Orders that are SHIPPED/DELIVERED/CONFIRMED
    // actually simpler to just query OrderItems with include SalesOrder
    const items = await OrderItem.findAll({
        include: [{
            model: SalesOrder,
            where: {
                companyId,
                status: { [Op.in]: ['CONFIRMED', 'PICKING_IN_PROGRESS', 'PICKED', 'PACKING_IN_PROGRESS', 'PACKED', 'SHIPPED', 'DELIVERED', 'COMPLETED'] },
                createdAt: { [Op.gte]: startDate }
            },
            attributes: ['createdAt'] // optimization
        }],
        attributes: ['productId', 'quantity']
    });

    // 4. Map sales query to product map
    const salesMap = {}; // productId -> totalQuantitySold
    items.forEach(item => {
        const pid = item.productId;
        salesMap[pid] = (salesMap[pid] || 0) + item.quantity;
    });

    // 5. Build prediction array
    const predictions = products.map(p => {
        // Current Stock (sum of all stocks in all warehouses/locations)
        // Note: ProductStocks might be an array if we have multiple entries, usually getting total quantity is safer
        const currentStock = (p.ProductStocks || []).reduce((sum, s) => sum + Number(s.quantity), 0);

        // Velocity (Units per Day)
        const totalSold = salesMap[p.id] || 0;
        const velocity = totalSold / daysBytes; // e.g. 10 sold in 30 days = 0.33 per day

        // Days until Stockout
        // If velocity is 0, infinite days (or null)
        let daysUntilStockout = null;
        if (velocity > 0) {
            daysUntilStockout = currentStock / velocity;
        }

        // Suggested Reorder
        // Formula: (Velocity * LeadTime) + (Velocity * SafetyStock) - CurrentStock
        // Defaults: LeadTime = 14 days, SafetyStock = 7 days -- eventually make these configurable per product
        const leadTime = 14;
        const safetyStockDays = 7;
        const needed = (velocity * (leadTime + safetyStockDays));
        let suggestedReorder = Math.ceil(needed - currentStock);
        if (suggestedReorder < 0) suggestedReorder = 0;

        // Status
        let status = 'HEALTHY';
        if (daysUntilStockout !== null) {
            if (daysUntilStockout < leadTime) {
                status = 'CRITICAL'; // Will run out before new stock arrives
            } else if (daysUntilStockout < (leadTime + safetyStockDays)) {
                status = 'LOW'; // Dipping into safety stock
            }
        } else if (currentStock === 0) {
            status = 'CRITICAL'; // No stock, no sales (or sales 0)
        }

        return {
            id: p.id,
            sku: p.sku,
            name: p.name,
            image: firstProductImage(p.images),
            currentStock,
            totalSoldLast30d: totalSold,
            velocity: Number(velocity.toFixed(2)),
            daysUntilStockout: daysUntilStockout === null ? 9999 : Number(daysUntilStockout.toFixed(1)),
            suggestedReorder,
            status,
            costPrice: p.costPrice
        };
    });

    // Sort: Critical first, then Low, then Healthy. Then by Days Left ascending.
    predictions.sort((a, b) => {
        const statusOrder = { 'CRITICAL': 0, 'LOW': 1, 'HEALTHY': 2 };
        if (statusOrder[a.status] !== statusOrder[b.status]) {
            return statusOrder[a.status] - statusOrder[b.status];
        }
        return a.daysUntilStockout - b.daysUntilStockout;
    });

    return predictions;
}

module.exports = {
    getPredictionData
};
