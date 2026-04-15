const warehouseService = require('../services/warehouseService');

async function list(req, res, next) {
  try {
    const data = await warehouseService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await warehouseService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Warehouse not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await warehouseService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message?.includes('companyId') || err.message?.includes('already exists')) {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await warehouseService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Warehouse not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await warehouseService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Warehouse deleted' });
  } catch (err) {
    if (err.message === 'Warehouse not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function getProducts(req, res, next) {
  try {
    const data = await warehouseService.getProducts(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Warehouse not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, remove, getProducts };
