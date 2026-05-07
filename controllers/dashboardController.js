const {
  Warehouse,
  User,
  Product,
  ProductStock,
  SalesOrder,
  PickList,
  PackingTask,
  Customer,
  InventoryLog,
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
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    // Fetch warehouse IDs once to use in multiple queries
    let whIds = [];
    if (companyId) {
      const warehouses = await Warehouse.findAll({ where: { companyId }, attributes: ['id'] });
      whIds = warehouses.map(w => w.id);
    }

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
      // Orders completed today (shipped)
      SalesOrder.count({
        where: {
          ...baseWhere,
          status: 'shipped',
          updatedAt: { [Op.gte]: todayStart }
        }
      }),
      // Pending Picks
      companyId
        ? PickList.count({
          where: { status: { [Op.in]: ['pending', 'in_progress'] } },
          include: [{ association: 'SalesOrder', where: { companyId }, required: true, attributes: [] }],
        })
        : PickList.count({ where: { status: { [Op.in]: ['pending', 'in_progress'] } } }),
      // Pending Packs
      companyId
        ? PackingTask.count({
          where: { status: { [Op.in]: ['pending', 'packing'] } },
          include: [{ association: 'SalesOrder', where: { companyId }, required: true, attributes: [] }],
        })
        : PackingTask.count({ where: { status: { [Op.in]: ['pending', 'packing'] } } }),
      // Today's Movements (Inventory Logs)
      companyId
        ? InventoryLog.count({
          where: { 
            createdAt: { [Op.gte]: todayStart },
            warehouseId: { [Op.in]: whIds }
          }
        })
        : InventoryLog.count({ where: { createdAt: { [Op.gte]: todayStart } } }),
    ]);

    // Warehouse Utilization Calculation
    let utilization = 0;
    if (companyId && whIds.length > 0) {
      
      const { Location } = require('../models');
      const totalLocations = await Location.count({
        include: [{ association: 'Zone', where: { warehouseId: { [Op.in]: whIds } } }]
      });
      
      const occupiedLocations = await ProductStock.count({
        distinct: true,
        col: 'locationId',
        where: { warehouseId: { [Op.in]: whIds }, quantity: { [Op.gt]: 0 } }
      });
      
      utilization = totalLocations > 0 ? Math.round((occupiedLocations / totalLocations) * 100) : 0;
    }

    // Staff Performance Data (Recent Task Completions)
    let staffPerformance = [];
    if (companyId) {
      const activeStaff = await User.findAll({
        where: { 
          companyId, 
          role: { [Op.in]: ['picker', 'packer'] },
          status: 'ACTIVE'
        },
        attributes: ['id', 'name', 'role'],
        limit: 10
      });

      staffPerformance = await Promise.all(activeStaff.map(async (s) => {
        const completedPicks = await PickList.count({
          where: { assignedTo: s.id, status: 'completed', updatedAt: { [Op.gte]: todayStart } }
        });
        const completedPacks = await PackingTask.count({
          where: { assignedTo: s.id, status: 'completed', updatedAt: { [Op.gte]: todayStart } }
        });

        return {
          id: s.id,
          name: s.name,
          role: s.role, // frontend will capitalize
          ordersCompleted: completedPicks + completedPacks,
          efficiency: 85 + (completedPicks + completedPacks > 0 ? 10 : 0)
        };
      }));
    }

    // Total stock
    let totalStock = 0;
    if (companyId) {
      totalStock = await ProductStock.sum('quantity', { where: { warehouseId: { [Op.in]: whIds } } }) || 0;
    } else {
      totalStock = await ProductStock.sum('quantity') || 0;
    }

    // Low stock count
    let lowStockCount = 0;
    if (companyId && whIds.length > 0) {
      const products = await Product.findAll({ where: { ...baseWhere, status: 'ACTIVE' }, attributes: ['id', 'reorderLevel'] });
      for (const p of products) {
        const sum = await ProductStock.sum('quantity', { where: { productId: p.id, warehouseId: { [Op.in]: whIds } } });
        if ((sum || 0) < (p.reorderLevel || 0)) lowStockCount += 1;
      }
    }

    // Picker/Packer specific stats
    let ordersPickedToday = 0;
    let ordersPackedToday = 0;
    if (user.role === 'picker') {
      ordersPickedToday = await PickList.count({ where: { assignedTo: user.id, status: 'PICKED', updatedAt: { [Op.gte]: todayStart } } });
    }
    if (user.role === 'packer') {
      ordersPackedToday = await PackingTask.count({ where: { assignedTo: user.id, status: 'PACKED', updatedAt: { [Op.gte]: todayStart } } });
    }

    res.json({
      success: true,
      data: {
        warehouses: counts[0],
        users: counts[1],
        products: counts[2],
        customers: counts[3],
        pendingOrders: counts[4],
        completedOrdersToday: counts[5],
        totalStock,
        lowStockCount,
        pickingPendingCount: user.role === 'picker' ? await PickList.count({ where: { assignedTo: user.id, status: { [Op.in]: ['NOT_STARTED', 'ASSIGNED', 'PARTIALLY_PICKED'] } } }) : counts[6],
        packingPendingCount: user.role === 'packer' ? await PackingTask.count({ where: { assignedTo: user.id, status: { [Op.in]: ['NOT_STARTED', 'ASSIGNED', 'PACKING'] } } }) : counts[7],
        movementsToday: counts[8],
        ordersPickedToday,
        ordersPackedToday,
        utilization,
        staffPerformance
      },
    });
  } catch (err) {
    next(err);
  }
}

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

    // Fetch Stock Distribution
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
          association: 'SalesOrder',
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
    // Initialize last N days with 0
    for (let i = 0; i < daysBack; i++) {
      const d = new Date();
      d.setDate(today.getDate() - i);
      const dateStr = d.toISOString().slice(0, 10);
      dateMap[dateStr] = { date: dateStr, count: 0, revenue: 0 };
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

    // Process Stock Distribution
    const stockByWarehouse = await Promise.all(
      warehouses.map(async (wh) => {
        const total = await ProductStock.sum('quantity', {
          where: { warehouseId: wh.id },
        });
        return {
          name: wh.name,
          stock: total || 0,
        };
      })
    );

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
      .slice(0, 5);

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
        stockByWarehouse
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
    let outOfStockCount = 0;
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
        if ((sum || 0) <= 0) outOfStockCount += 1;
      }
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

/**
 * GET /api/dashboard/notifications
 * Returns role-specific dynamic notifications
 */
async function notifications(req, res, next) {
  try {
    const user = req.user;
    const companyId = user.companyId || null;
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    const alerts = [];

    if (user.role === 'super_admin') {
      const planRequests = await Company.count({ where: { status: 'PENDING' } });
      if (planRequests > 0) {
        alerts.push({ id: 'plan-req', title: 'Plan Requests', message: `${planRequests} companies awaiting approval.`, type: 'info', link: '/companies' });
      }
    }

    if (companyId) {
      // Common for admin/manager/inventory
      if (['company_admin', 'inventory_manager', 'warehouse_manager'].includes(user.role)) {
        // Low Stock
        const products = await Product.findAll({ where: { companyId, status: 'ACTIVE' }, attributes: ['id', 'name', 'reorderLevel'] });
        const whs = await Warehouse.findAll({ where: { companyId }, attributes: ['id'] });
        const whIds = whs.map(w => w.id);
        
        let lowStockCount = 0;
        for (const p of products) {
          const sum = await ProductStock.sum('quantity', { where: { productId: p.id, warehouseId: { [Op.in]: whIds } } });
          if ((sum || 0) < (p.reorderLevel || 0)) lowStockCount++;
        }
        if (lowStockCount > 0) {
          alerts.push({ id: 'low-stock', title: 'Low Stock Alert', message: `${lowStockCount} items are below reorder level.`, type: 'warning', link: '/inventory' });
        }
      }

      if (user.role === 'warehouse_manager' || user.role === 'company_admin') {
        const pendingPicks = await PickList.count({ 
          where: { status: 'pending' },
          include: [{ association: 'SalesOrder', where: { companyId }, attributes: [] }]
        });
        if (pendingPicks > 0) {
          alerts.push({ id: 'pending-picks', title: 'Unassigned Picks', message: `${pendingPicks} picking tasks need assignment.`, type: 'error', link: '/picking' });
        }
        
        const pendingPacks = await PackingTask.count({ 
          where: { status: 'pending' },
          include: [{ association: 'SalesOrder', where: { companyId }, attributes: [] }]
        });
        if (pendingPacks > 0) {
          alerts.push({ id: 'pending-packs', title: 'Pending Packing', message: `${pendingPacks} packing tasks are awaiting fulfillment.`, type: 'info', link: '/packing' });
        }
      }

      if (user.role === 'picker') {
        const myPicks = await PickList.count({ where: { assignedTo: user.id, status: { [Op.in]: ['pending', 'in_progress'] } } });
        if (myPicks > 0) {
          alerts.push({ id: 'my-picks', title: 'New Assignments', message: `You have ${myPicks} picking tasks assigned to you.`, type: 'success', link: '/picking' });
        }
      }

      if (user.role === 'packer') {
        const myPacks = await PackingTask.count({ where: { assignedTo: user.id, status: { [Op.in]: ['pending', 'packing'] } } });
        if (myPacks > 0) {
          alerts.push({ id: 'my-packs', title: 'New Assignments', message: `You have ${myPacks} packing tasks assigned to you.`, type: 'success', link: '/packing' });
        }
      }
      
      if (user.role === 'inventory_manager') {
        const pendingPOs = await SalesOrder.count({ where: { companyId, status: 'pending' } });
        if (pendingPOs > 0) {
          alerts.push({ id: 'pending-orders', title: 'New Orders', message: `${pendingPOs} new orders received today.`, type: 'info', link: '/sales-orders' });
        }
      }
    }

    res.json({ success: true, data: alerts });
  } catch (err) {
    next(err);
  }
}

module.exports = { stats, charts, reports, notifications };
