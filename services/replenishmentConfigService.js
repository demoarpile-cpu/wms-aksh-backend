const { ReplenishmentConfig, Product, Company, ProductStock, ReplenishmentTask, Warehouse, Zone, Location } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  else if (query.companyId) where.companyId = query.companyId;
  if (query.status) where.status = query.status;

  const configs = await ReplenishmentConfig.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [{ association: 'Product', attributes: ['id', 'name', 'sku'] }],
  });
  return configs.map((c) => c.get({ plain: true }));
}

async function getById(id, reqUser) {
  const config = await ReplenishmentConfig.findByPk(id, {
    include: [{ association: 'Product' }],
  });
  if (!config) throw new Error('Replenishment config not found');
  if (reqUser.role !== 'super_admin' && config.companyId !== reqUser.companyId) throw new Error('Replenishment config not found');
  return config.get({ plain: true });
}

async function create(data, reqUser) {
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('companyId required');

  const config = await ReplenishmentConfig.create({
    companyId,
    productId: data.productId,
    minStockLevel: Number(data.minStockLevel) || 0,
    maxStockLevel: Number(data.maxStockLevel) || 0,
    reorderPoint: Number(data.reorderPoint) || 0,
    reorderQuantity: Number(data.reorderQuantity) || 0,
    autoCreateTasks: data.autoCreateTasks !== false,
    status: data.status || 'ACTIVE',
  });
  return getById(config.id, reqUser);
}

async function update(id, data, reqUser) {
  const config = await ReplenishmentConfig.findByPk(id);
  if (!config) throw new Error('Replenishment config not found');
  if (reqUser.role !== 'super_admin' && config.companyId !== reqUser.companyId) throw new Error('Replenishment config not found');

  const updates = {};
  if (data.productId != null) updates.productId = data.productId;
  if (data.minStockLevel != null) updates.minStockLevel = Number(data.minStockLevel);
  if (data.maxStockLevel != null) updates.maxStockLevel = Number(data.maxStockLevel);
  if (data.reorderPoint != null) updates.reorderPoint = Number(data.reorderPoint);
  if (data.reorderQuantity != null) updates.reorderQuantity = Number(data.reorderQuantity);
  if (data.autoCreateTasks !== undefined) updates.autoCreateTasks = !!data.autoCreateTasks;
  if (data.status != null) updates.status = data.status;
  await config.update(updates);
  return getById(id, reqUser);
}

async function remove(id, reqUser) {
  const config = await ReplenishmentConfig.findByPk(id);
  if (!config) throw new Error('Replenishment config not found');
  if (reqUser.role !== 'super_admin' && config.companyId !== reqUser.companyId) throw new Error('Replenishment config not found');
  await config.destroy();
  return { message: 'Replenishment config deleted' };
}

/** Run auto-check: products below reorder point (Settings → flow → create Tasks) */
async function runAutoCheck(reqUser) {
  const where = { status: 'ACTIVE', autoCreateTasks: true };
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;

  const configs = await ReplenishmentConfig.findAll({
    where,
    include: [{ association: 'Product', attributes: ['id', 'name', 'sku'] }],
  });

  const productWhere = { productId: { [Op.in]: configs.map((c) => c.productId) } };
  const stocks = await ProductStock.findAll({
    where: productWhere,
    attributes: ['productId', 'quantity', 'reserved'],
  });

  const stockByProduct = {};
  for (const s of stocks) {
    const pid = s.productId;
    if (!stockByProduct[pid]) stockByProduct[pid] = { quantity: 0, reserved: 0 };
    stockByProduct[pid].quantity += Number(s.quantity) || 0;
    stockByProduct[pid].reserved += Number(s.reserved) || 0;
  }

  const suggestions = [];
  for (const c of configs) {
    const total = (stockByProduct[c.productId]?.quantity ?? 0) - (stockByProduct[c.productId]?.reserved ?? 0);
    const reorderPoint = Number(c.reorderPoint) || 0;
    if (reorderPoint > 0 && total < reorderPoint) {
      suggestions.push({
        configId: c.id,
        productId: c.productId,
        productName: c.Product?.name,
        productSku: c.Product?.sku,
        currentStock: total,
        reorderPoint,
        reorderQuantity: Number(c.reorderQuantity) || 0,
      });
    }
  }
  return { suggestions };
}

/** Run auto-check and create replenishment tasks when stock is below reorder point (e.g. pick location max 60, when stock < 20 → auto task) */
async function runAutoCheckAndCreateTasks(reqUser) {
  const { suggestions } = await runAutoCheck(reqUser);
  if (suggestions.length === 0) return { suggestions, tasksCreated: 0, message: 'No products below reorder point' };

  const companyId = reqUser.companyId || (suggestions[0] && suggestions[0].companyId);
  if (!companyId) return { suggestions, tasksCreated: 0, message: 'Company context required to create tasks' };

  const warehouses = await Warehouse.findAll({ where: { companyId }, attributes: ['id'] });
  const whIds = warehouses.map((w) => w.id);
  if (whIds.length === 0) return { suggestions, tasksCreated: 0, message: 'No warehouse found' };

  const zones = await Zone.findAll({ where: { warehouseId: { [Op.in]: whIds } }, attributes: ['id'] });
  const zoneIds = zones.map((z) => z.id);
  if (zoneIds.length === 0) return { suggestions, tasksCreated: 0, message: 'No zones found' };

  const locations = await Location.findAll({
    where: { zoneId: { [Op.in]: zoneIds } },
    attributes: ['id', 'locationType'],
  });
  const bulkLoc = locations.find((l) => (l.locationType || '').toUpperCase() === 'BULK');
  const pickLoc = locations.find((l) => (l.locationType || '').toUpperCase() === 'PICK');
  const fromLocationId = bulkLoc?.id || locations[0]?.id;
  const toLocationId = pickLoc?.id || locations[1]?.id || locations[0]?.id;
  if (!fromLocationId || !toLocationId || fromLocationId === toLocationId) {
    return { suggestions, tasksCreated: 0, message: 'Need at least 2 locations (e.g. BULK and PICK) to create replenishment tasks' };
  }

  const count = await ReplenishmentTask.count({ where: { companyId } });
  let tasksCreated = 0;
  for (let i = 0; i < suggestions.length; i++) {
    const s = suggestions[i];
    const taskNumber = `RPL-${String(count + tasksCreated + 1).padStart(6, '0')}`;
    await ReplenishmentTask.create({
      companyId,
      productId: s.productId,
      fromLocationId,
      toLocationId,
      taskNumber,
      quantityNeeded: s.reorderQuantity || 1,
      quantityCompleted: 0,
      priority: 'MEDIUM',
      status: 'PENDING',
    });
    tasksCreated++;
  }
  return { suggestions, tasksCreated, message: `Created ${tasksCreated} replenishment task(s)` };
}

module.exports = { list, getById, create, update, remove, runAutoCheck, runAutoCheckAndCreateTasks };
