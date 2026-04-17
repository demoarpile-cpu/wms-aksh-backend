const goodsReceiptService = require('../services/goodsReceiptService');

async function list(req, res, next) {
  try {
    const data = await goodsReceiptService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await goodsReceiptService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Goods receipt not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await goodsReceiptService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (
      err.message === 'Purchase order not found' ||
      err.message === 'Company context required' ||
      err.message === 'Warehouse is required for GRN creation' ||
      err.message === 'Invalid warehouse'
    ) return res.status(400).json({ success: false, message: err.message });
    if (err.message?.includes('Only approved')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function updateReceived(req, res, next) {
  try {
    const data = await goodsReceiptService.updateReceived(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Goods receipt not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('already completed')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await goodsReceiptService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Goods receipt deleted' });
  } catch (err) {
    if (err.message === 'Goods receipt not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function updateAsnItems(req, res, next) {
  try {
    const data = await goodsReceiptService.updateAsnItems(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'ASN not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function finalizeReceiving(req, res, next) {
  try {
    const data = await goodsReceiptService.finalizeReceiving(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'ASN not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('Already finalized') || err.message?.includes('Heat-sensitive product')) {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

module.exports = { list, getById, create, updateReceived, updateAsnItems, finalizeReceiving, remove };
