const { Customer } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  else if (query.companyId) where.companyId = query.companyId;
  if (query.search) {
    const term = `%${query.search}%`;
    where[Op.or] = [
      { name: { [Op.like]: term } },
      { email: { [Op.like]: term } },
      { code: { [Op.like]: term } },
      { contactPerson: { [Op.like]: term } },
      { phone: { [Op.like]: term } },
    ];
  }
  if (query.type) where.type = query.type;
  if (query.status) where.status = query.status;
  if (query.tier) where.tier = query.tier;
  const customers = await Customer.findAll({ where, order: [['name']] });
  // Return plain objects so type, segment, tier etc. are always present in JSON
  return customers.map((c) => c.get({ plain: true }));
}

async function getById(id, reqUser) {
  const customer = await Customer.findByPk(id);
  if (!customer) throw new Error('Customer not found');
  if (reqUser.role !== 'super_admin' && customer.companyId !== reqUser.companyId) throw new Error('Customer not found');
  return customer;
}

async function create(data, reqUser) {
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('companyId required');
  return Customer.create({
    companyId,
    code: data.code || null,
    name: data.name,
    type: data.type || null,
    contactPerson: data.contactPerson || null,
    email: data.email || null,
    phone: data.phone || null,
    country: data.country || null,
    state: data.state || null,
    city: data.city || null,
    address: data.address || null,
    postcode: data.postcode || null,
    tier: data.tier || null,
    segment: data.segment || null,
    creditLimit: data.creditLimit != null ? Number(data.creditLimit) : null,
    paymentTerms: data.paymentTerms || null,
    status: data.status || 'ACTIVE',
  });
}

async function update(id, data, reqUser) {
  const customer = await Customer.findByPk(id);
  if (!customer) throw new Error('Customer not found');
  if (reqUser.role !== 'super_admin' && customer.companyId !== reqUser.companyId) throw new Error('Customer not found');
  const updates = {
    name: data.name !== undefined ? data.name : customer.name,
    email: data.email !== undefined ? data.email : customer.email,
    phone: data.phone !== undefined ? data.phone : customer.phone,
    address: data.address !== undefined ? data.address : customer.address,
    postcode: data.postcode !== undefined ? data.postcode : customer.postcode,
    code: data.code !== undefined ? data.code : customer.code,
    type: data.type !== undefined ? data.type : customer.type,
    contactPerson: data.contactPerson !== undefined ? data.contactPerson : customer.contactPerson,
    country: data.country !== undefined ? data.country : customer.country,
    state: data.state !== undefined ? data.state : customer.state,
    city: data.city !== undefined ? data.city : customer.city,
    tier: data.tier !== undefined ? data.tier : customer.tier,
    segment: data.segment !== undefined ? data.segment : customer.segment,
    creditLimit: data.creditLimit !== undefined ? (data.creditLimit == null ? null : Number(data.creditLimit)) : customer.creditLimit,
    paymentTerms: data.paymentTerms !== undefined ? data.paymentTerms : customer.paymentTerms,
    status: data.status !== undefined ? data.status : customer.status,
  };
  await customer.update(updates);
  return customer;
}

async function remove(id, reqUser) {
  const customer = await Customer.findByPk(id);
  if (!customer) throw new Error('Customer not found');
  if (reqUser.role !== 'super_admin' && customer.companyId !== reqUser.companyId) throw new Error('Customer not found');
  await customer.destroy();
  return { message: 'Customer deleted' };
}

module.exports = { list, getById, create, update, remove };
