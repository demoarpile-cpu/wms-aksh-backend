const pickingService = require('../services/pickingService');

async function list(req, res, next) {
  try {
    const data = await pickingService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await pickingService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Pick list not found' || err.message === 'Not assigned to you') {
      return res.status(404).json({ success: false, message: err.message });
    }
    next(err);
  }
}

async function assignPicker(req, res, next) {
  try {
    const data = await pickingService.assignPicker(req.params.id, req.body.userId, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message?.includes('assign') || err.message === 'Invalid picker') {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

async function startPicking(req, res, next) {
  try {
    const data = await pickingService.startPicking(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Not assigned to you') return res.status(403).json({ success: false, message: err.message });
    next(err);
  }
}

async function updatePickedQuantity(req, res, next) {
  try {
    const data = await pickingService.updatePickedQuantity(req.params.itemId, req.body.quantityPicked, req.user);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function completePicking(req, res, next) {
  try {
    const data = await pickingService.completePicking(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Not assigned to you') return res.status(403).json({ success: false, message: err.message });
    next(err);
  }
}

async function rejectAssignment(req, res, next) {
  try {
    const data = await pickingService.rejectAssignment(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Not assigned to you') return res.status(403).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, assignPicker, startPicking, updatePickedQuantity, completePicking, rejectAssignment };
