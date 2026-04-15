const packingService = require('../services/packingService');

async function list(req, res, next) {
  try {
    const data = await packingService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await packingService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Packing task not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function assignPacker(req, res, next) {
  try {
    const data = await packingService.assignPacker(req.params.id, req.body.userId, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Invalid packer') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function completePacking(req, res, next) {
  try {
    const data = await packingService.completePacking(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Not assigned to you') return res.status(403).json({ success: false, message: err.message });
    next(err);
  }
}

async function startPacking(req, res, next) {
  try {
    const data = await packingService.startPacking(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function rejectAssignment(req, res, next) {
  try {
    const data = await packingService.rejectAssignment(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

module.exports = { list, getById, assignPacker, completePacking, startPacking, rejectAssignment };
