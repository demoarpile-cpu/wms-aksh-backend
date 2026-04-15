const zoneService = require('../services/zoneService');

async function list(req, res, next) {
  try {
    const zones = await zoneService.list(req.user, req.query);
    res.json({ success: true, data: zones });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const zone = await zoneService.getById(req.params.id, req.user);
    res.json({ success: true, data: zone });
  } catch (err) {
    if (err.message === 'Zone not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const zone = await zoneService.create(req.body, req.user);
    res.status(201).json({ success: true, data: zone });
  } catch (err) {
    if (err.message?.includes('warehouseId')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const zone = await zoneService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data: zone });
  } catch (err) {
    if (err.message === 'Zone not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await zoneService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Zone deleted' });
  } catch (err) {
    if (err.message === 'Zone not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, remove };
