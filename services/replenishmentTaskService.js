const { ReplenishmentTask, Product, Location, Zone, Company, Movement, ProductStock } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  else if (query.companyId) where.companyId = query.companyId;
  if (query.status) where.status = query.status;

  const tasks = await ReplenishmentTask.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      { association: 'Product', attributes: ['id', 'name', 'sku'] },
      { association: 'fromLocation', attributes: ['id', 'name', 'code'] },
      { association: 'toLocation', attributes: ['id', 'name', 'code'] },
    ],
  });
  return tasks.map((t) => t.get({ plain: true }));
}

async function getById(id, reqUser) {
  const task = await ReplenishmentTask.findByPk(id, {
    include: [
      { association: 'Product' },
      { association: 'fromLocation' },
      { association: 'toLocation' },
    ],
  });
  if (!task) throw new Error('Replenishment task not found');
  if (reqUser.role !== 'super_admin' && task.companyId !== reqUser.companyId) throw new Error('Replenishment task not found');
  return task.get({ plain: true });
}

async function create(data, reqUser) {
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('companyId required');

  const count = await ReplenishmentTask.count({ where: { companyId } });
  const taskNumber = `RPL-${String(count + 1).padStart(6, '0')}`;

  const task = await ReplenishmentTask.create({
    companyId,
    productId: data.productId,
    fromLocationId: data.fromLocationId,
    toLocationId: data.toLocationId,
    taskNumber,
    quantityNeeded: Number(data.quantityNeeded) || 0,
    quantityCompleted: 0,
    priority: data.priority || 'MEDIUM',
    notes: data.notes || null,
    status: 'PENDING',
  });
  return getById(task.id, reqUser);
}

async function update(id, data, reqUser) {
  const task = await ReplenishmentTask.findByPk(id);
  if (!task) throw new Error('Replenishment task not found');
  if (reqUser.role !== 'super_admin' && task.companyId !== reqUser.companyId) throw new Error('Replenishment task not found');

  const updates = {};
  if (data.productId != null) updates.productId = data.productId;
  if (data.fromLocationId != null) updates.fromLocationId = data.fromLocationId;
  if (data.toLocationId != null) updates.toLocationId = data.toLocationId;
  if (data.quantityNeeded != null) updates.quantityNeeded = Number(data.quantityNeeded);
  if (data.quantityCompleted != null) updates.quantityCompleted = Number(data.quantityCompleted);
  if (data.priority != null) updates.priority = data.priority;
  if (data.notes !== undefined) updates.notes = data.notes;
  if (data.status != null) updates.status = data.status;
  await task.update(updates);
  return getById(id, reqUser);
}

async function complete(id, reqUser) {
  const task = await ReplenishmentTask.findByPk(id, {
    include: [
      { association: 'fromLocation', include: [{ association: 'Zone', attributes: ['warehouseId'] }] },
      { association: 'toLocation', include: [{ association: 'Zone', attributes: ['warehouseId'] }] },
    ],
  });
  if (!task) throw new Error('Replenishment task not found');
  if (reqUser.role !== 'super_admin' && task.companyId !== reqUser.companyId) throw new Error('Replenishment task not found');
  if ((task.status || '').toUpperCase() === 'COMPLETED') throw new Error('Task already completed');

  const qty = Number(task.quantityNeeded) || 0;
  if (qty <= 0) throw new Error('Invalid quantity');

  const fromLoc = task.fromLocation;
  const toLoc = task.toLocation;
  if (!fromLoc || !toLoc) throw new Error('From/To location not found');
  const warehouseIdFrom = fromLoc.Zone?.warehouseId;
  const warehouseIdTo = toLoc.Zone?.warehouseId;
  if (!warehouseIdFrom || !warehouseIdTo) throw new Error('Location warehouse not found');

  // 1. Decrease stock at FROM location (source)
  const fromStock = await ProductStock.findOne({
    where: { productId: task.productId, locationId: task.fromLocationId, warehouseId: warehouseIdFrom },
  });
  if (!fromStock) throw new Error('No stock at source location. Add inventory first.');
  const available = Number(fromStock.quantity) - Number(fromStock.reserved || 0);
  if (available < qty) throw new Error(`Insufficient stock at source. Available: ${available}, needed: ${qty}`);
  await fromStock.decrement('quantity', { by: qty });

  // 2. Increase stock at TO location (destination) – find or create
  let toStock = await ProductStock.findOne({
    where: { productId: task.productId, locationId: task.toLocationId, warehouseId: warehouseIdTo },
  });
  if (toStock) {
    await toStock.increment('quantity', { by: qty });
  } else {
    await ProductStock.create({
      productId: task.productId,
      warehouseId: warehouseIdTo,
      locationId: task.toLocationId,
      quantity: qty,
      reserved: 0,
      status: 'ACTIVE',
    });
  }

  // 3. Create Movement (flow: Inventory Movements page pe dikhega)
  await Movement.create({
    companyId: task.companyId,
    type: 'TRANSFER',
    productId: task.productId,
    fromLocationId: task.fromLocationId,
    toLocationId: task.toLocationId,
    quantity: qty,
    reason: 'Replenishment',
    notes: `Task ${task.taskNumber} completed`,
    createdBy: reqUser.id,
  });

  // 4. Mark task completed
  await task.update({ status: 'COMPLETED', quantityCompleted: qty });
  return getById(id, reqUser);
}

async function remove(id, reqUser) {
  const task = await ReplenishmentTask.findByPk(id);
  if (!task) throw new Error('Replenishment task not found');
  if (reqUser.role !== 'super_admin' && task.companyId !== reqUser.companyId) throw new Error('Replenishment task not found');
  await task.destroy();
  return { message: 'Replenishment task deleted' };
}

module.exports = { list, getById, create, update, complete, remove };
