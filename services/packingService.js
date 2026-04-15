const { PackingTask, PickList, SalesOrder, User } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role === 'packer') {
    where[Op.or] = [
      { assignedTo: reqUser.id },
      { assignedTo: null }
    ];
  }

  if (query.status) where.status = query.status;

  // Filter by Company for non-super-admins (assuming reqUser.companyId exists)
  if (reqUser.role !== 'super_admin' && reqUser.companyId) {
    // We filter via SalesOrder include
  }

  const tasks = await PackingTask.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      {
        association: 'SalesOrder',
        where: (reqUser.companyId ? { companyId: reqUser.companyId } : {}),
        required: true,
        attributes: ['id', 'orderNumber', 'status'],
        include: ['Customer']
      },
      {
        association: 'PickList',
        where: { status: 'PICKED' },
        required: true,
        attributes: ['id', 'status'],
        include: ['PickListItems']
      },
      { association: 'User', attributes: ['id', 'name', 'email'], required: false },
    ],
  });
  return tasks;
}

async function getById(id, reqUser) {
  const task = await PackingTask.findByPk(id, {
    include: [
      { association: 'SalesOrder', include: ['OrderItems', 'Customer'] },
      { association: 'PickList', include: ['PickListItems'] },
      { association: 'User', attributes: { exclude: ['passwordHash'] }, required: false },
    ],
  });
  if (!task) throw new Error('Packing task not found');
  if (reqUser.role === 'packer' && task.assignedTo !== reqUser.id) throw new Error('Packing task not found');
  if (reqUser.role === 'company_admin' && task.SalesOrder.companyId !== reqUser.companyId) throw new Error('Packing task not found');
  return task;
}

async function assignPacker(taskId, userId, reqUser) {
  if (reqUser.role !== 'warehouse_manager' && reqUser.role !== 'company_admin' && reqUser.role !== 'super_admin') {
    throw new Error('Only Warehouse Manager can assign packer');
  }
  const task = await PackingTask.findByPk(taskId, { include: ['SalesOrder'] });
  if (!task) throw new Error('Packing task not found');
  if (task.SalesOrder.companyId !== reqUser.companyId && reqUser.role !== 'super_admin') throw new Error('Packing task not found');
  const user = await User.findByPk(userId);
  if (!user || user.role !== 'packer' || user.companyId !== task.SalesOrder.companyId) throw new Error('Invalid packer');

  await task.update({ assignedTo: userId, status: 'ASSIGNED' });

  return getById(taskId, reqUser);
}

async function startPacking(id, reqUser) {
  const task = await PackingTask.findByPk(id, { include: ['SalesOrder'] });
  if (!task) throw new Error('Packing task not found');
  if (reqUser.role === 'packer' && task.assignedTo !== reqUser.id) throw new Error('Not assigned to you');
  try {
    console.log('Starting packing task:', id, 'User:', reqUser.id);
    const task = await PackingTask.findByPk(id, { include: ['SalesOrder'] });
    if (!task) throw new Error('Packing task not found');

    console.log('Task found:', task.id, 'AssignedTo:', task.assignedTo);
    if (reqUser.role === 'packer' && task.assignedTo !== reqUser.id) throw new Error('Not assigned to you');

    console.log('Updating task status to PACKING');
    await task.update({ status: 'PACKING' });

    console.log('Updating SalesOrder status to PACKING_IN_PROGRESS');
    if (task.SalesOrder) {
      await task.SalesOrder.update({ status: 'PACKING_IN_PROGRESS' });
    } else {
      console.warn('SalesOrder not found for task', id);
    }

    return getById(id, reqUser);
  } catch (error) {
    console.error('Error in startPacking:', error);
    throw error;
  }
}

async function completePacking(id, reqUser) {
  const task = await PackingTask.findByPk(id, { include: ['SalesOrder'] });
  if (!task) throw new Error('Packing task not found');
  if (reqUser.role === 'packer' && task.assignedTo !== reqUser.id) throw new Error('Not assigned to you');
  if (task.status === 'PACKED') return task; // Idempotency

  // Create Shipment
  const { Shipment } = require('../models');
  await Shipment.create({
    salesOrderId: task.salesOrderId,
    companyId: task.SalesOrder.companyId, // Ensure SalesOrder is fetched
    packedBy: reqUser.id,
    dispatchDate: new Date(),
    deliveryStatus: 'READY_TO_SHIP'
  });

  await task.update({ status: 'PACKED', packedAt: new Date() });
  await task.SalesOrder.update({ status: 'PACKED' });
  return getById(id, reqUser);
}

async function rejectAssignment(id, reqUser) {
  const task = await PackingTask.findByPk(id, { include: ['SalesOrder'] });
  if (!task) throw new Error('Packing task not found');
  if (reqUser.role === 'packer' && task.assignedTo !== reqUser.id) throw new Error('Not assigned to you');

  // Unassign and reset status
  await task.update({ assignedTo: null, status: 'NOT_STARTED' });

  // Return simple success object because getById will fail (permission denied for unassigned task)
  return { id: parseInt(id), status: 'NOT_STARTED', assignedTo: null, success: true };
}

module.exports = { list, getById, assignPacker, startPacking, completePacking, rejectAssignment };
