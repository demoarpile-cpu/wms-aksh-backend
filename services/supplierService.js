const { Supplier } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  else if (query.companyId) where.companyId = query.companyId;
  if (query.search) {
    where[Op.or] = [
      { name: { [Op.like]: `%${query.search}%` } },
      { email: { [Op.like]: `%${query.search}%` } },
      { code: { [Op.like]: `%${query.search}%` } },
    ];
  }
  const suppliers = await Supplier.findAll({ where, order: [['name']] });
  return suppliers;
}

async function getById(id, reqUser) {
  const supplier = await Supplier.findByPk(id);
  if (!supplier) throw new Error('Supplier not found');
  if (reqUser.role !== 'super_admin' && supplier.companyId !== reqUser.companyId) throw new Error('Supplier not found');
  return supplier;
}

async function create(data, reqUser) {
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('companyId required');
  return Supplier.create({
    companyId,
    code: data.code,
    name: data.name,
    email: data.email || null,
    phone: data.phone || null,
    address: data.address || null,
  });
}

async function update(id, data, reqUser) {
  const supplier = await Supplier.findByPk(id);
  if (!supplier) throw new Error('Supplier not found');
  if (reqUser.role !== 'super_admin' && supplier.companyId !== reqUser.companyId) throw new Error('Supplier not found');
  await supplier.update({
    code: data.code ?? supplier.code,
    name: data.name ?? supplier.name,
    email: data.email !== undefined ? data.email : supplier.email,
    phone: data.phone !== undefined ? data.phone : supplier.phone,
    address: data.address !== undefined ? data.address : supplier.address,
  });
  return supplier;
}

async function remove(id, reqUser) {
  const supplier = await Supplier.findByPk(id);
  if (!supplier) throw new Error('Supplier not found');
  if (reqUser.role !== 'super_admin' && supplier.companyId !== reqUser.companyId) throw new Error('Supplier not found');
  await supplier.destroy();
  return { message: 'Supplier deleted' };
}

module.exports = { list, getById, create, update, remove };
