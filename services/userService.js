const { User, Company, Warehouse } = require('../models');
const authService = require('./authService');
const { Op } = require('sequelize');

const STAFF_ROLES = ['warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer'];

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role === 'super_admin') {
    if (query.companyId) where.companyId = query.companyId;
    if (query.role) where.role = query.role;
  } else if (reqUser.role === 'company_admin') {
    where.companyId = reqUser.companyId;
    if (query.role) {
      if (STAFF_ROLES.includes(query.role)) where.role = query.role;
      else where.role = { [Op.in]: STAFF_ROLES }; // Fallback or empty? Fallback to all staff seems safer or maybe empty
    } else {
      where.role = { [Op.in]: STAFF_ROLES };
    }
  } else {
    return [];
  }
  if (query.warehouseId) where.warehouseId = query.warehouseId;
  if (query.status) where.status = query.status;
  if (query.search) {
    where[Op.or] = [
      { name: { [Op.like]: `%${query.search}%` } },
      { email: { [Op.like]: `%${query.search}%` } },
    ];
  }
  const users = await User.findAll({
    where,
    attributes: { exclude: ['passwordHash'] },
    order: [['createdAt', 'DESC']],
    include: [
      { association: 'Company', attributes: ['id', 'name', 'code'], required: false },
      { association: 'Warehouse', attributes: ['id', 'name'], required: false },
    ],
  });
  return users;
}

async function getById(id, reqUser) {
  const user = await User.findByPk(id, {
    attributes: { exclude: ['passwordHash'] },
    include: [
      { association: 'Company', attributes: ['id', 'name', 'code'] },
      { association: 'Warehouse', attributes: ['id', 'name'] },
    ],
  });
  if (!user) throw new Error('User not found');
  if (reqUser.role === 'company_admin' && user.companyId !== reqUser.companyId) throw new Error('User not found');
  if (reqUser.role === 'company_admin' && !STAFF_ROLES.includes(user.role)) throw new Error('User not found');
  return user;
}

async function create(data, reqUser) {
  if (reqUser.role === 'super_admin') {
    const allowedRoles = ['company_admin', ...STAFF_ROLES];
    if (data.role && !allowedRoles.includes(data.role)) throw new Error('Invalid role');
    if (!data.companyId) throw new Error('companyId required');
  }
  if (reqUser.role === 'company_admin') {
    if (!STAFF_ROLES.includes(data.role)) throw new Error('Invalid role for staff');
    data.companyId = reqUser.companyId;
  }
  if (!data.password || data.password.length < 6) throw new Error('Password min 6 characters');
  return authService.createUser(data);
}

async function update(id, data, reqUser) {
  const user = await User.findByPk(id);
  if (!user) throw new Error('User not found');
  if (reqUser.role === 'company_admin') {
    if (user.companyId !== reqUser.companyId) throw new Error('User not found');
    if (!STAFF_ROLES.includes(user.role)) throw new Error('User not found');
    delete data.companyId;
    if (data.role !== undefined && !STAFF_ROLES.includes(data.role)) delete data.role;
  }
  if (data.password) {
    user.passwordHash = await authService.hashPassword(data.password);
    delete data.password;
  }
  const allowed = ['name', 'email', 'role', 'companyId', 'warehouseId', 'status'];
  allowed.forEach((k) => { if (data[k] !== undefined) user[k] = data[k]; });
  await user.save();
  const u = user.toJSON();
  delete u.passwordHash;
  return u;
}

async function remove(id, reqUser) {
  const user = await User.findByPk(id);
  if (!user) throw new Error('User not found');
  if (reqUser.role === 'company_admin' && (user.companyId !== reqUser.companyId || !STAFF_ROLES.includes(user.role))) {
    throw new Error('User not found');
  }
  await user.destroy();
  return { message: 'User deleted' };
}

module.exports = { list, getById, create, update, remove };
