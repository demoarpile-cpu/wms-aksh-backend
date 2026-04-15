const replenishmentTaskService = require('../services/replenishmentTaskService');

async function list(req, res, next) {
  try {
    const data = await replenishmentTaskService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await replenishmentTaskService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Replenishment task not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await replenishmentTaskService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await replenishmentTaskService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Replenishment task not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function complete(req, res, next) {
  try {
    const data = await replenishmentTaskService.complete(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Replenishment task not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await replenishmentTaskService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Replenishment task deleted' });
  } catch (err) {
    if (err.message === 'Replenishment task not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, complete, remove };
