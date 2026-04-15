const locationService = require('../services/locationService');
const csv = require('csv-parser');
const fs = require('fs');

function detectCsvSeparator(filePath) {
  const text = fs.readFileSync(filePath, 'utf8');
  const firstLine = text.split(/\r?\n/).find((l) => l.trim().length > 0) || '';
  const commas = (firstLine.match(/,/g) || []).length;
  const semis = (firstLine.match(/;/g) || []).length;
  return semis > commas ? ';' : ',';
}

async function list(req, res, next) {
  try {
    const locations = await locationService.list(req.user, req.query);
    res.json({ success: true, data: locations });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const location = await locationService.getById(req.params.id, req.user);
    res.json({ success: true, data: location });
  } catch (err) {
    if (err.message === 'Location not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const location = await locationService.create(req.body, req.user);
    res.status(201).json({ success: true, data: location });
  } catch (err) {
    if (err.message?.includes('zoneId')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const location = await locationService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data: location });
  } catch (err) {
    if (err.message === 'Location not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await locationService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Location deleted' });
  } catch (err) {
    if (err.message === 'Location not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function bulkUpload(req, res, next) {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, message: 'No file uploaded' });
    }

    const sep = detectCsvSeparator(req.file.path);
    const results = [];
    fs.createReadStream(req.file.path)
      .pipe(
        csv({
          separator: sep,
          mapHeaders: ({ header }) =>
            String(header || '')
              .replace(/^\uFEFF/, '')
              .replace(/^["'\s]+|["'\s]+$/g, '')
              .trim(),
        })
      )
      .on('data', (data) => results.push(data))
      .on('end', async () => {
        try {
          const importResult = await locationService.bulkCreate(results, req.user);
          res.json(importResult);
        } catch (err) {
          res.status(400).json({ success: false, message: err.message });
        } finally {
          if (fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
        }
      });
  } catch (err) {
    next(err);
  }
}

module.exports = { list, getById, create, update, remove, bulkUpload };

