const { Bundle, BundleItem, Product } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  else if (query.companyId) where.companyId = query.companyId;
  if (query.search) {
    where[Op.or] = [
      { name: { [Op.like]: `%${query.search}%` } },
      { sku: { [Op.like]: `%${query.search}%` } },
    ];
  }
  const bundles = await Bundle.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [{
      association: 'BundleItems',
      include: [{ association: 'Product', attributes: ['id', 'name', 'sku'] }],
    }],
  });
  return bundles.map(b => {
    const j = b.toJSON();
    if (j.BundleItems) {
      j.bundleItems = j.BundleItems.map(it => ({
        id: it.id,
        productId: it.productId,
        quantity: it.quantity,
        child: it.Product,
      }));
      delete j.BundleItems;
    }
    return j;
  });
}

async function getById(id, reqUser) {
  const bundle = await Bundle.findByPk(id, {
    include: [{
      association: 'BundleItems',
      include: [{ association: 'Product', attributes: ['id', 'name', 'sku'] }],
    }],
  });
  if (!bundle) throw new Error('Bundle not found');
  if (reqUser.role !== 'super_admin' && bundle.companyId !== reqUser.companyId) throw new Error('Bundle not found');
  const j = bundle.toJSON();
  if (j.BundleItems) {
    j.bundleItems = j.BundleItems.map(it => ({ id: it.id, productId: it.productId, quantity: it.quantity, child: it.Product }));
    delete j.BundleItems;
  }
  return j;
}

async function create(data, reqUser) {
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('companyId required');
  const existing = await Bundle.findOne({ where: { companyId, sku: (data.sku || '').trim() } });
  if (existing) throw new Error('Bundle SKU already exists for this company');
  const bundle = await Bundle.create({
    companyId,
    sku: (data.sku || '').trim(),
    name: data.name,
    description: data.description || null,
    costPrice: data.costPrice ?? 0,
    sellingPrice: data.sellingPrice ?? 0,
    status: data.status || 'ACTIVE',
  });
  const items = Array.isArray(data.bundleItems) ? data.bundleItems.filter(i => i.productId && i.quantity > 0) : [];
  for (const it of items) {
    await BundleItem.create({ bundleId: bundle.id, productId: it.productId, quantity: it.quantity });
  }
  return getById(bundle.id, reqUser);
}

async function update(id, data, reqUser) {
  const bundle = await Bundle.findByPk(id);
  if (!bundle) throw new Error('Bundle not found');
  if (reqUser.role !== 'super_admin' && bundle.companyId !== reqUser.companyId) throw new Error('Bundle not found');
  await bundle.update({
    name: data.name ?? bundle.name,
    sku: data.sku !== undefined ? data.sku.trim() : bundle.sku,
    description: data.description !== undefined ? data.description : bundle.description,
    costPrice: data.costPrice !== undefined ? data.costPrice : bundle.costPrice,
    sellingPrice: data.sellingPrice !== undefined ? data.sellingPrice : bundle.sellingPrice,
    status: data.status ?? bundle.status,
  });
  if (Array.isArray(data.bundleItems)) {
    await BundleItem.destroy({ where: { bundleId: bundle.id } });
    for (const it of data.bundleItems.filter(i => i.productId && i.quantity > 0)) {
      await BundleItem.create({ bundleId: bundle.id, productId: it.productId, quantity: it.quantity });
    }
  }
  return getById(bundle.id, reqUser);
}

async function remove(id, reqUser) {
  const bundle = await Bundle.findByPk(id);
  if (!bundle) throw new Error('Bundle not found');
  if (reqUser.role !== 'super_admin' && bundle.companyId !== reqUser.companyId) throw new Error('Bundle not found');
  await BundleItem.destroy({ where: { bundleId: bundle.id } });
  await bundle.destroy();
  return { message: 'Bundle deleted' };
}

module.exports = { list, getById, create, update, remove };
