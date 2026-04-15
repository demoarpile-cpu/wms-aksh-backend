const { Company, User } = require('../models');
const { Op } = require('sequelize');

async function list(query = {}) {
  const where = {};
  if (query.status) where.status = query.status;
  if (query.search) {
    where[Op.or] = [
      { name: { [Op.like]: `%${query.search}%` } },
      { code: { [Op.like]: `%${query.search}%` } },
    ];
  }
  const companies = await Company.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      { association: 'Users', attributes: ['id', 'name', 'email', 'role'], required: false },
      { association: 'Warehouses', attributes: ['id', 'name', 'code'], required: false },
    ],
  });
  return companies;
}

async function getById(id) {
  const company = await Company.findByPk(id, {
    include: [
      { association: 'Users', attributes: { exclude: ['passwordHash'] } },
      { association: 'Warehouses' },
    ],
  });
  if (!company) throw new Error('Company not found');
  return company;
}

async function create(data) {
  const existing = await Company.findOne({ where: { code: data.code.trim() } });
  if (existing) throw new Error('Company code already exists');
  return Company.create({
    name: data.name,
    code: data.code.trim(),
    email: data.email || null,
    phone: data.phone || null,
    address: data.address || null,
    status: data.status || 'ACTIVE',
  });
}

async function update(id, data) {
  const company = await Company.findByPk(id);
  if (!company) throw new Error('Company not found');
  if (data.code && data.code !== company.code) {
    const existing = await Company.findOne({ where: { code: data.code.trim() } });
    if (existing) throw new Error('Company code already exists');
  }
  await company.update({
    name: data.name ?? company.name,
    code: (data.code && data.code.trim()) || company.code,
    email: data.email !== undefined ? data.email : company.email,
    phone: data.phone !== undefined ? data.phone : company.phone,
    address: data.address !== undefined ? data.address : company.address,
    status: data.status ?? company.status,
  });
  return company;
}

async function remove(id) {
  const company = await Company.findByPk(id);
  if (!company) throw new Error('Company not found');
  await company.destroy();
  return { message: 'Company deleted' };
}

module.exports = { list, getById, create, update, remove };
