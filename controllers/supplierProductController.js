const supplierProductService = require('../services/supplierProductService');

async function list(req, res, next) {
  try {
    const data = await supplierProductService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function bulkUpload(req, res, next) {
  try {
    // Basic CSV parsing simulation if body is array
    const mappings = Array.isArray(req.body) ? req.body : (req.body.data || []);
    const results = await supplierProductService.bulkUpload(mappings, req.user);
    res.json({ success: true, results });
  } catch (err) {
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await supplierProductService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Mapping deleted' });
  } catch (err) {
    if (err.message === 'Mapping not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, bulkUpload, remove };
