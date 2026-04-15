const {
  Warehouse,
  User,
  Product,
  ProductStock,
  SalesOrder,
  PickList,
  PackingTask,
  Customer,
} = require('../models');
const { Op } = require('sequelize');

/**
 * GET /api/dashboard/stats
 * Role-aware: company_admin/warehouse_manager/inventory_manager/viewer see company scope;
 * super_admin can pass ?companyId= for a company or get first-company stats;
 * picker/packer see their company scope.
 */
async function stats(req, res, next) {
  try {
    const user = req.user;
    let companyId = user.companyId || null;
    if (user.role === 'super_admin' && req.query.companyId) {
      companyId = parseInt(req.query.companyId, 10);
    }

    const baseWhere = companyId ? { companyId } : {};

    const counts = await Promise.all([
      Warehouse.count({ where: baseWhere }),
      User.count({ where: { ...baseWhere, status: 'ACTIVE' } }),
      Product.count({ where: { ...baseWhere, status: 'ACTIVE' } }),
      Customer.count({ where: baseWhere }),
      SalesOrder.count({
        where: {
          ...baseWhere,
          status: { [Op.in]: ['pending', 'pick_list_created', 'picking', 'packing'] },
        },
      }),
      SalesOrder.count({ where: baseWhere }),
      companyId
        ? PickList.count({
          where: { status: { [Op.in]: ['pending', 'in_progress'] } },
          include: [{ association: 'SalesOrder', where: { companyId }, required: true, attributes: [] }],
        })
        : PickList.count({ where: { status: { [Op.in]: ['pending', 'in_progress'] } } }),
      companyId
        ? PackingTask.count({
          where: { status: { [Op.in]: ['pending', 'packing'] } },
          include: [{ association: 'SalesOrder', where: { companyId }, required: true, attributes: [] }],
        })
        : PackingTask.count({ where: { status: { [Op.in]: ['pending', 'packing'] } } }),
    ]);

    // Total stock: sum of ProductStock.quantity (optionally scoped by company via Warehouse)
    let totalStock = 0;
    if (companyId) {
      const warehouses = await Warehouse.findAll({ where: { companyId }, attributes: ['id'] });
      const whIds = warehouses.map((w) => w.id);
      const result = await ProductStock.sum('quantity', {
        where: { warehouseId: { [Op.in]: whIds } },
      });
      totalStock = result || 0;
    } else {
      const result = await ProductStock.sum('quantity');
      totalStock = result || 0;
    }

    // Low stock: products where sum(stock) < reorderLevel (company-scoped)
    let lowStockCount = 0;
    if (companyId) {
      const products = await Product.findAll({
        where: { ...baseWhere, status: 'ACTIVE' },
        attributes: ['id', 'reorderLevel'],
      });
      const whIds = (await Warehouse.findAll({ where: { companyId }, attributes: ['id'] })).map((w) => w.id);
      for (const p of products) {
        const sum = await ProductStock.sum('quantity', {
          where: { productId: p.id, warehouseId: { [Op.in]: whIds } },
        });
        if ((sum || 0) < (p.reorderLevel || 0)) lowStockCount += 1;
      }
    }

    // Out of stock: products where sum(stock) <= 0
    let outOfStockCount = 0;
    if (companyId) {
      const products = await Product.findAll({
        where: { ...baseWhere, status: 'ACTIVE' },
        attributes: ['id'],
      });
      const whIds = (await Warehouse.findAll({ where: { companyId }, attributes: ['id'] })).map((w) => w.id);
      for (const p of products) {
        const sum = await ProductStock.sum('quantity', {
          where: { productId: p.id, warehouseId: { [Op.in]: whIds } },
        });
        if ((sum || 0) <= 0) outOfStockCount += 1;
      }
    }

    res.json({
      success: true,
      data: {
        warehouses: counts[0],
        users: counts[1],
        products: counts[2],
        customers: counts[3],
        pendingOrders: counts[4],
        totalOrders: counts[5],
        totalStock,
        lowStockCount,
        outOfStockCount,
        pickingPendingCount: counts[6],
        packingPendingCount: counts[7],
      },
    });
  } catch (err) {
    next(err);
  }
}

/**
 * GET /api/dashboard/charts
 * Returns chart-ready data: ordersByDay, ordersByStatus, stockByWarehouse, topProducts
 */
/**
 * GET /api/dashboard/charts
 * Returns chart-ready data: ordersByDay, ordersByStatus, stockByWarehouse, topProducts (by sales)
 */
async function charts(req, res, next) {
  try {
    const user = req.user;
    let companyId = user.companyId || null;
    if (user.role === 'super_admin' && req.query.companyId) {
      companyId = parseInt(req.query.companyId, 10);
    }
    const baseWhere = companyId ? { companyId } : {};

    // Use query param for days back or default to 30
    const daysBack = req.query.days ? parseInt(req.query.days, 10) : 30;
    const startDate = new Date();
    startDate.setHours(0, 0, 0, 0);
    startDate.setDate(startDate.getDate() - daysBack);

    // Fetch Orders for Sales Trend
    const orders = await SalesOrder.findAll({
      where: { ...baseWhere, createdAt: { [Op.gte]: startDate } },
      attributes: ['id', 'status', 'totalAmount', 'createdAt'],
      raw: true,
    });

    // Fetch Stock Distribution (keep existing logic)
    const warehouses = await Warehouse.findAll({
      where: baseWhere,
      attributes: ['id', 'name'],
      raw: true,
    });

    // Fetch Top Selling Products (OrderItems)
    const { OrderItem } = require('../models');
    const orderItems = await OrderItem.findAll({
      include: [
        {
          model: SalesOrder,
          where: {
            ...baseWhere,
            createdAt: { [Op.gte]: startDate },
            status: { [Op.notIn]: ['DRAFT', 'CANCELLED'] }
          },
          attributes: []
        },
        { model: Product, attributes: ['name', 'sku'] }
      ]
    });

    // Process Orders by Day
    const dateMap = {};
    const today = new Date();
    // Initialize last 30 days with 0
    for (let i = 0; i < daysBack; i++) {
      const d = new Date();
      d.setDate(today.getDate() - i);
      const dateStr = d.toISOString().slice(0, 10);
      dateMap[dateStr] = { date: dateStr, count: 0, revenue: 0 }; // revenue, not totalAmount for chart
    }

    orders.forEach((o) => {
      const d = o.createdAt ? new Date(o.createdAt).toISOString().slice(0, 10) : null;
      if (!d || !dateMap[d]) return;
      dateMap[d].count += 1;
      dateMap[d].revenue += Number(o.totalAmount) || 0;
    });
    const salesTrend = Object.values(dateMap).sort((a, b) => (a.date > b.date ? 1 : -1));

    // Process Orders by Status
    const statusMap = {};
    orders.forEach((o) => {
      const s = o.status || 'unknown';
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    const ordersByStatus = Object.entries(statusMap).map(([status, count]) => ({ status, count }));

    // Process Stock by Customer/Warehouse (Optional, simplified for now)
    // ...

    // Process Top Selling Products
    const productStats = {};
    orderItems.forEach(item => {
      const pid = item.productId;
      if (!productStats[pid]) {
        productStats[pid] = {
          name: item.Product?.name || 'Unknown',
          sku: item.Product?.sku || 'sku',
          qty: 0,
          revenue: 0
        };
      }
      productStats[pid].qty += (item.quantity || 0);
      productStats[pid].revenue += (Number(item.subtotal) || 0);
    });

    const topProducts = Object.values(productStats)
      .sort((a, b) => b.qty - a.qty)
      .slice(0, 5); // Start with Top 5 for dashboard widget

    res.json({
      success: true,
      data: {
        salesTrend,
        ordersByStatus,
        topProducts: topProducts.map(p => ({
          name: p.name,
          sku: p.sku,
          sold: p.qty,
          revenue: p.revenue
        })),
        // stockByWarehouse // Removed to save query time if not used on main dashboard yet
      },
    });
  } catch (err) {
    next(err);
  }
}

/**
 * GET /api/reports
 * Returns list of report entries for company (Operations, Orders, Inventory, Low Stock)
 */
async function reports(req, res, next) {
  try {
    const user = req.user;
    let companyId = user.companyId || null;
    if (user.role === 'super_admin' && req.query.companyId) {
      companyId = parseInt(req.query.companyId, 10);
    }
    const baseWhere = companyId ? { companyId } : {};

    const counts = await Promise.all([
      Warehouse.count({ where: baseWhere }),
      User.count({ where: { ...baseWhere, status: 'ACTIVE' } }),
      Product.count({ where: { ...baseWhere, status: 'ACTIVE' } }),
      Customer.count({ where: baseWhere }),
      SalesOrder.count({
        where: {
          ...baseWhere,
          status: { [Op.in]: ['pending', 'pick_list_created', 'picking', 'packing'] },
        },
      }),
      SalesOrder.count({ where: baseWhere }),
    ]);

    let totalStock = 0;
    let lowStockCount = 0;
    if (companyId) {
      const warehouses = await Warehouse.findAll({ where: { companyId }, attributes: ['id'] });
      const whIds = warehouses.map((w) => w.id);
      const result = await ProductStock.sum('quantity', { where: { warehouseId: { [Op.in]: whIds } } });
      totalStock = result || 0;
      const products = await Product.findAll({
        where: { ...baseWhere, status: 'ACTIVE' },
        attributes: ['id', 'reorderLevel'],
      });
      for (const p of products) {
        const sum = await ProductStock.sum('quantity', {
          where: { productId: p.id, warehouseId: { [Op.in]: whIds } },
        });
        if ((sum || 0) < (p.reorderLevel || 0)) lowStockCount += 1;
      }
      let oosCount = 0;
      for (const p of products) {
        const sum = await ProductStock.sum('quantity', {
          where: { productId: p.id, warehouseId: { [Op.in]: whIds } },
        });
        if ((sum || 0) <= 0) oosCount += 1;
      }
      outOfStockCount = oosCount;
    } else {
      const result = await ProductStock.sum('quantity');
      totalStock = result || 0;
    }

    const now = new Date().toISOString();
    const list = [
      {
        id: 'ops-summary',
        reportName: 'Operations Summary',
        name: 'Operations Summary',
        category: 'OPERATIONAL',
        schedule: 'LIVE',
        format: 'PDF',
        createdAt: now,
        metadata: {
          warehouses: counts[0],
          users: counts[1],
          products: counts[2],
          customers: counts[3],
          pendingOrders: counts[4],
          totalOrders: counts[5],
          totalStock,
          lowStockCount,
        },
      },
      {
        id: 'order-summary',
        reportName: 'Order Summary',
        name: 'Order Summary',
        category: 'ORDERS',
        schedule: 'LIVE',
        format: 'PDF',
        createdAt: now,
        metadata: { pendingOrders: counts[4], totalOrders: counts[5] },
      },
      {
        id: 'inventory-summary',
        reportName: 'Inventory Summary',
        name: 'Inventory Summary',
        category: 'INVENTORY',
        schedule: 'LIVE',
        format: 'PDF',
        createdAt: now,
        metadata: { products: counts[2], totalStock, lowStockCount },
      },
      {
        id: 'low-stock',
        reportName: 'Low Stock Alert',
        name: 'Low Stock Alert',
        category: 'INVENTORY',
        schedule: 'LIVE',
        format: 'PDF',
        createdAt: now,
        metadata: { lowStockCount, outOfStockCount },
      },
    ];

    res.json({ success: true, data: list });
  } catch (err) {
    next(err);
  }
}

module.exports = { stats, charts, reports };
