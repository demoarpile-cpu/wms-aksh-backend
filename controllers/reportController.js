const reportService = require('../services/reportService');

async function list(req, res, next) {
  try {
    const data = await reportService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await reportService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Report not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await reportService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await reportService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Report not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await reportService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Report deleted' });
  } catch (err) {
    if (err.message === 'Report not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function download(req, res, next) {
  try {
    const { filename, content, mimeType } = await reportService.download(req.params.id, req.user);
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', mimeType);
    res.send(content);
  } catch (err) {
    if (err.message === 'Report not found' || err.message === 'Report content not available') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, remove, download };
