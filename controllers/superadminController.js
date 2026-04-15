const { Company, User, Warehouse, SalesOrder } = require('../models');

async function stats(req, res, next) {
  try {
    const [totalCompanies, activeUsers, totalOrders, totalWarehouses] = await Promise.all([
      Company.count(),
      User.count({ where: { status: 'ACTIVE' } }),
      SalesOrder.count(),
      Warehouse.count(),
    ]);
    res.json({
      success: true,
      data: {
        totalCompanies,
        activeUsers,
        totalOrders,
        totalWarehouses,
        systemHealth: 'Healthy',
      },
    });
  } catch (err) {
    next(err);
  }
}

async function reports(req, res, next) {
  try {
    const list = [];
    res.json({ success: true, data: list });
  } catch (err) {
    next(err);
  }
}

module.exports = { stats, reports };
