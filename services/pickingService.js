const { PickList, PickListItem, SalesOrder, Product, Warehouse, User } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role === 'picker') {
    where[Op.or] = [
      { assignedTo: reqUser.id },
      { assignedTo: null }
    ];
    // If user belongs to a warehouse, filter by it too
    if (reqUser.warehouseId) {
      where.warehouseId = reqUser.warehouseId;
    }
  }
  else if (query.warehouseId) where.warehouseId = query.warehouseId;
  if (query.status) where.status = query.status;
  const orderInclude = {
    association: 'SalesOrder',
    attributes: ['id', 'orderNumber', 'status', 'companyId'],
    include: [{ association: 'Customer', attributes: ['name'] }]
  };
  if (reqUser.role === 'warehouse_manager' || reqUser.role === 'company_admin') {
    orderInclude.where = { companyId: reqUser.companyId };
    orderInclude.required = true;
  } else if (reqUser.role === 'super_admin' && query.companyId) {
    orderInclude.where = { companyId: query.companyId };
    orderInclude.required = true;
  }
  const pickLists = await PickList.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      orderInclude,
      { association: 'Warehouse', attributes: ['id', 'name'] },
      { association: 'User', attributes: ['id', 'name', 'email'], required: false },
      { association: 'PickListItems', include: ['Product'] },
    ],
  });
  return pickLists;
}

async function getById(id, reqUser) {
  const pickList = await PickList.findByPk(id, {
    include: [
      { association: 'SalesOrder', include: ['Customer'] },
      { association: 'Warehouse' },
      { association: 'User', attributes: { exclude: ['passwordHash'] }, required: false },
      { association: 'PickListItems', include: ['Product'] },
    ],
  });
  if (!pickList) throw new Error('Pick list not found');
  const order = await SalesOrder.findByPk(pickList.salesOrderId);
  if (reqUser.role === 'picker' && pickList.assignedTo !== reqUser.id) throw new Error('Pick list not found');
  if (reqUser.role === 'company_admin' && order.companyId !== reqUser.companyId) throw new Error('Pick list not found');
  return pickList;
}

async function assignPicker(pickListId, userId, reqUser) {
  if (reqUser.role !== 'warehouse_manager' && reqUser.role !== 'company_admin' && reqUser.role !== 'super_admin') {
    throw new Error('Only Warehouse Manager can assign picker');
  }
  const pickList = await PickList.findByPk(pickListId, { include: ['SalesOrder'] });
  if (!pickList) throw new Error('Pick list not found');
  const order = await SalesOrder.findByPk(pickList.salesOrderId);
  if (reqUser.role === 'company_admin' && order.companyId !== reqUser.companyId) throw new Error('Pick list not found');
  const user = await User.findByPk(userId);
  if (!user || user.role !== 'picker' || user.companyId !== order.companyId) throw new Error('Invalid picker');

  await pickList.update({ assignedTo: userId, status: 'ASSIGNED' });
  await order.update({ status: 'PICKING_IN_PROGRESS' });

  return getById(pickListId, reqUser);
}

async function startPicking(id, reqUser) {
  const pickList = await PickList.findByPk(id, { include: ['SalesOrder'] });
  if (!pickList) throw new Error('Pick list not found');
  if (reqUser.role === 'picker' && pickList.assignedTo !== reqUser.id) throw new Error('Not assigned to you');

  await pickList.update({ status: 'PARTIALLY_PICKED' });
  await pickList.SalesOrder.update({ status: 'PICKING_IN_PROGRESS' });

  return getById(id, reqUser);
}

async function updatePickedQuantity(pickListItemId, quantityPicked, reqUser) {
  const item = await PickListItem.findByPk(pickListItemId, { include: ['PickList'] });
  if (!item) throw new Error('Item not found');
  const pickList = await PickList.findByPk(item.pickListId, { include: ['SalesOrder'] });
  if (reqUser.role === 'picker' && pickList.assignedTo !== reqUser.id) throw new Error('Not assigned to you');

  await item.update({ quantityPicked: quantityPicked ?? item.quantityRequired });

  if (pickList.status === 'NOT_STARTED' || pickList.status === 'ASSIGNED') {
    await pickList.update({ status: 'PARTIALLY_PICKED' });
    await pickList.SalesOrder.update({ status: 'PICKING_IN_PROGRESS' });
  }

  return item;
}

async function completePicking(id, reqUser) {
  const pickList = await PickList.findByPk(id, { include: ['PickListItems', 'SalesOrder'] });
  if (!pickList) throw new Error('Pick list not found');
  if (reqUser.role === 'picker' && pickList.assignedTo !== reqUser.id) throw new Error('Not assigned to you');

  if (pickList.status === 'PICKED') {
    return getById(id, reqUser);
  }

  await pickList.update({ status: 'PICKED' });
  await pickList.SalesOrder.update({ status: 'PICKED' });

  // Create Packing Task automatically
  const { PackingTask } = require('../models');

  const existingTask = await PackingTask.findOne({ where: { pickListId: pickList.id } });
  if (!existingTask) {
    await PackingTask.create({
      salesOrderId: pickList.salesOrderId,
      pickListId: pickList.id,
      status: 'NOT_STARTED',
      warehouseId: pickList.warehouseId // Assuming warehouseId exists on PickList
    });
  }

  return getById(id, reqUser);
}

async function rejectAssignment(id, reqUser) {
  const pickList = await PickList.findByPk(id, { include: ['SalesOrder'] });
  if (!pickList) throw new Error('Pick list not found');
  if (reqUser.role === 'picker' && pickList.assignedTo !== reqUser.id) throw new Error('Not assigned to you');

  await pickList.update({ status: 'NOT_STARTED', assignedTo: null });
  // Revert order status to CONFIRMED if it was PICKING_IN_PROGRESS, assuming no other active tasks prevent it?
  // Simply reverting to CONFIRMED seems correct as it goes back to "Ready to Pick" state.
  await pickList.SalesOrder.update({ status: 'CONFIRMED' });

  return { message: 'Assignment rejected', id };
}

module.exports = { list, getById, assignPicker, startPicking, updatePickedQuantity, completePicking, rejectAssignment };
