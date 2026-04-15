const purchaseOrderService = require('../services/purchaseOrderService');

async function list(req, res, next) {
  try {
    const data = await purchaseOrderService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await purchaseOrderService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await purchaseOrderService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message === 'Invalid supplier' || err.message === 'Company context required') return res.status(400).json({ success: false, message: err.message });
    if (err.message === 'Not allowed to create purchase orders') return res.status(403).json({ success: false, message: err.message });
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await purchaseOrderService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('Only pending')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function approve(req, res, next) {
  try {
    const data = await purchaseOrderService.approve(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('Only pending')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await purchaseOrderService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Purchase order deleted' });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('Only pending')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function generateAsn(req, res, next) {
  try {
    const data = await purchaseOrderService.generateAsn(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.code === 'BAD_REQUEST' || err.message?.includes('Only approved')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, approve, remove, generateAsn };

