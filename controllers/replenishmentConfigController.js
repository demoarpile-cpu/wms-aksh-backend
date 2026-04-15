const replenishmentConfigService = require('../services/replenishmentConfigService');

async function list(req, res, next) {
  try {
    const data = await replenishmentConfigService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await replenishmentConfigService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Replenishment config not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await replenishmentConfigService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await replenishmentConfigService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Replenishment config not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await replenishmentConfigService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Replenishment config deleted' });
  } catch (err) {
    if (err.message === 'Replenishment config not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function runAutoCheck(req, res, next) {
  try {
    const result = await replenishmentConfigService.runAutoCheck(req.user);
    res.json({ success: true, data: result.suggestions });
  } catch (err) {
    next(err);
  }
}

async function runAutoCheckAndCreateTasks(req, res, next) {
  try {
    const result = await replenishmentConfigService.runAutoCheckAndCreateTasks(req.user);
    res.json({ success: true, data: result.suggestions, tasksCreated: result.tasksCreated, message: result.message });
  } catch (err) {
    next(err);
  }
}

module.exports = { list, getById, create, update, remove, runAutoCheck, runAutoCheckAndCreateTasks };
