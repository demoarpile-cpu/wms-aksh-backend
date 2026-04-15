const { Zone, Warehouse } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  const qWh =
    query.warehouseId != null && query.warehouseId !== '' ? Number(query.warehouseId) : null;

  // Align with location import: scope zones by company warehouses, not only Zone.companyId
  // (legacy rows may have companyId null while warehouse still belongs to the company).
  if (reqUser.role === 'super_admin') {
    if (qWh) where.warehouseId = qWh;
  } else if (reqUser.role === 'company_admin' || reqUser.role === 'inventory_manager') {
    if (reqUser.companyId) {
      const companyWarehouses = await Warehouse.findAll({
        where: { companyId: reqUser.companyId },
        attributes: ['id'],
      });
      const whIds = companyWarehouses.map((w) => Number(w.id));
      if (qWh) {
        where.warehouseId = whIds.includes(qWh) ? qWh : { [Op.in]: [-1] };
      } else {
        where.warehouseId = whIds.length ? { [Op.in]: whIds } : { [Op.in]: [-1] };
      }
    } else if (qWh) {
      where.warehouseId = qWh;
    }
  } else if (reqUser.warehouseId) {
    const wid = Number(reqUser.warehouseId);
    if (qWh && qWh !== wid) {
      where.warehouseId = { [Op.in]: [-1] };
    } else {
      where.warehouseId = qWh || wid;
    }
  } else if (qWh) {
    where.warehouseId = qWh;
  }

  const zones = await Zone.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [{ association: 'Warehouse', attributes: ['id', 'name', 'code'] }],
  });
  return zones;
}

async function getById(id, reqUser) {
  const zone = await Zone.findByPk(id, {
    include: [{ association: 'Warehouse' }, { association: 'Locations' }],
  });
  if (!zone) throw new Error('Zone not found');
  return zone;
}

async function create(data, reqUser) {
  if (!data.warehouseId) throw new Error('warehouseId required');
  let companyId = reqUser.companyId;
  if (!companyId && data.warehouseId) {
    const wh = await Warehouse.findByPk(data.warehouseId);
    if (wh) companyId = wh.companyId;
  }

  return Zone.create({
    companyId,
    warehouseId: data.warehouseId,
    name: data.name,
    code: data.code || null,
    zoneType: data.zoneType || null,
  });
}

async function update(id, data, reqUser) {
  const zone = await Zone.findByPk(id);
  if (!zone) throw new Error('Zone not found');
  await zone.update({
    name: data.name ?? zone.name,
    code: data.code !== undefined ? data.code : zone.code,
    zoneType: data.zoneType !== undefined ? data.zoneType : zone.zoneType,
  });
  return zone;
}

async function remove(id, reqUser) {
  const zone = await Zone.findByPk(id);
  if (!zone) throw new Error('Zone not found');
  await zone.destroy();
  return { message: 'Zone deleted' };
}

module.exports = { list, getById, create, update, remove };
