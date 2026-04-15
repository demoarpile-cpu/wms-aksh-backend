const shipmentService = require('../services/shipmentService');

async function list(req, res, next) {
  try {
    const data = await shipmentService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await shipmentService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Shipment not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await shipmentService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message === 'Order not found' || err.message === 'Order must be packed first') {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await shipmentService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Shipment not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function deductStock(req, res, next) {
  try {
    const data = await shipmentService.deductStockForShipment(req.params.id, req.user);
    res.json({ success: true, ...data });
  } catch (err) {
    if (err.message === 'Shipment not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message === 'Only shipped/delivered shipments can deduct stock') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, deductStock };
