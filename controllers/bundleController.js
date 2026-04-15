const bundleService = require('../services/bundleService');

async function list(req, res, next) {
  try {
    const data = await bundleService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await bundleService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Bundle not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await bundleService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await bundleService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Bundle not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await bundleService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Bundle deleted' });
  } catch (err) {
    if (err.message === 'Bundle not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, remove };
