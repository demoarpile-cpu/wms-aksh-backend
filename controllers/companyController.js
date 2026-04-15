const companyService = require('../services/companyService');

async function list(req, res, next) {
  try {
    const companies = await companyService.list(req.query);
    res.json({ success: true, data: companies });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const company = await companyService.getById(req.params.id);
    res.json({ success: true, data: company });
  } catch (err) {
    if (err.message === 'Company not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const company = await companyService.create(req.body);
    res.status(201).json({ success: true, data: company });
  } catch (err) {
    if (err.message === 'Company code already exists') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const company = await companyService.update(req.params.id, req.body);
    res.json({ success: true, data: company });
  } catch (err) {
    if (err.message === 'Company not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message === 'Company code already exists') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await companyService.remove(req.params.id);
    res.json({ success: true, message: 'Company deleted' });
  } catch (err) {
    if (err.message === 'Company not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, remove };
