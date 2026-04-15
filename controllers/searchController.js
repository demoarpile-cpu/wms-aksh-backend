const { Product, SalesOrder, Customer } = require('../models');
const { Op } = require('sequelize');

const LIMIT = 5;

/**
 * GET /api/search?q=term
 * Global search: orders (orderNumber, referenceNumber), products (name, sku), customers (name, email, code)
 */
async function search(req, res, next) {
    try {
        const q = (req.query.q || '').trim();
        if (!q) {
            return res.json({ success: true, data: { orders: [], products: [], customers: [] } });
        }
        const companyId = req.user?.companyId;
        const whereCompany = companyId != null ? { companyId } : {};

        const term = `%${q}%`;

        const [orders, products, customers] = await Promise.all([
            SalesOrder.findAll({
                where: {
                    ...whereCompany,
                    [Op.or]: [
                        { orderNumber: { [Op.like]: term } },
                        { referenceNumber: { [Op.like]: term } },
                    ],
                },
                limit: LIMIT,
                order: [['createdAt', 'DESC']],
                attributes: ['id', 'orderNumber', 'referenceNumber', 'status', 'totalAmount'],
            }),
            Product.findAll({
                where: {
                    ...whereCompany,
                    [Op.or]: [
                        { name: { [Op.like]: term } },
                        { sku: { [Op.like]: term } },
                    ],
                },
                limit: LIMIT,
                attributes: ['id', 'name', 'sku'],
            }),
            Customer.findAll({
                where: {
                    ...whereCompany,
                    [Op.or]: [
                        { name: { [Op.like]: term } },
                        { email: { [Op.like]: term } },
                        { code: { [Op.like]: term } },
                    ],
                },
                limit: LIMIT,
                attributes: ['id', 'name', 'email', 'code'],
            }),
        ]);

        return res.json({
            success: true,
            data: {
                orders: orders.map((o) => ({ id: o.id, orderNumber: o.orderNumber, referenceNumber: o.referenceNumber, status: o.status, totalAmount: o.totalAmount })),
                products: products.map((p) => ({ id: p.id, name: p.name, sku: p.sku })),
                customers: customers.map((c) => ({ id: c.id, name: c.name, email: c.email, code: c.code })),
            },
        });
    } catch (err) {
        next(err);
    }
}

module.exports = { search };
