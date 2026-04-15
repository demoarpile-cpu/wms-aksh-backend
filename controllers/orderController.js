const orderService = require('../services/orderService');

async function list(req, res, next) {
  try {
    const data = await orderService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await orderService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Order not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await orderService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message?.includes('Company Admin')) return res.status(403).json({ success: false, message: err.message });
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await orderService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('Only pending')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    const data = await orderService.remove(req.params.id, req.user);
    res.json({ success: true, ...data });
  } catch (err) {
    if (err.message === 'Order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('cannot be deleted') || err.message?.includes('can be deleted')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, remove };
