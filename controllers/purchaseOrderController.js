const purchaseOrderService = require('../services/purchaseOrderService');
const fs = require('fs');
const csv = require('csv-parser');
const { Readable } = require('stream');

function readFileAsUtf8String(filePath) {
  const buf = fs.readFileSync(filePath);
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) return buf.slice(2).toString('utf16le');
  if (buf.length >= 2 && buf[0] === 0xfe && buf[1] === 0xff) {
    const body = Buffer.from(buf.slice(2));
    body.swap16();
    return body.toString('utf16le');
  }
  let s = buf.toString('utf8');
  if (s.length > 0 && s.charCodeAt(0) === 0xfeff) s = s.slice(1);
  return s;
}

function detectSeparatorFromFirstLine(firstLine) {
  if (!firstLine) return ',';
  const tabs = (firstLine.match(/\t/g) || []).length;
  const commas = (firstLine.match(/,/g) || []).length;
  const semis = (firstLine.match(/;/g) || []).length;
  if (tabs > 0 && tabs >= commas && tabs >= semis) return '\t';
  if (semis > commas) return ';';
  return ',';
}

function parseCsvText(text) {
  const firstLine = text.split(/\r?\n/).find((l) => l.trim().length > 0) || '';
  const separator = detectSeparatorFromFirstLine(firstLine);
  return new Promise((resolve, reject) => {
    const rows = [];
    Readable.from([text])
      .pipe(
        csv({
          separator,
          mapHeaders: ({ header }) =>
            String(header || '')
              .replace(/^\uFEFF/, '')
              .replace(/^["'\s]+|["'\s]+$/g, '')
              .trim(),
        })
      )
      .on('data', (row) => rows.push(row))
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

async function list(req, res, next) {
  try {
    const data = await purchaseOrderService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const data = await purchaseOrderService.getById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const data = await purchaseOrderService.create(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message === 'Invalid supplier' || err.message === 'Company context required') return res.status(400).json({ success: false, message: err.message });
    if (err.message === 'Not allowed to create purchase orders') return res.status(403).json({ success: false, message: err.message });
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const data = await purchaseOrderService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('Only pending')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function approve(req, res, next) {
  try {
    const data = await purchaseOrderService.approve(req.params.id, req.body || {}, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('Only pending')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await purchaseOrderService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Purchase order deleted' });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.message?.includes('Only pending')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function generateAsn(req, res, next) {
  try {
    const data = await purchaseOrderService.generateAsn(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    if (err.code === 'BAD_REQUEST' || err.message?.includes('Only approved')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function uploadCsv(req, res, next) {
  const file = req.file;
  if (!file) return res.status(400).json({ success: false, message: 'CSV file is required' });
  try {
    const text = readFileAsUtf8String(file.path);
    const rows = await parseCsvText(text);
    const data = await purchaseOrderService.createFromCsv({
      supplierId: req.body.supplierId,
      clientId: req.body.clientId,
      poNumber: req.body.poNumber,
      expectedDelivery: req.body.expectedDelivery,
      notes: req.body.notes,
      rows,
    }, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.code === 'BAD_REQUEST' || err.message?.includes('CSV') || err.message?.includes('supplier')) {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  } finally {
    try {
      if (fs.existsSync(file.path)) fs.unlinkSync(file.path);
    } catch (_) {
      // ignore cleanup errors
    }
  }
}

async function downloadPdf(req, res, next) {
  try {
    const { buffer, filename } = await purchaseOrderService.generatePoPdf(req.params.id, req.user);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(buffer);
  } catch (err) {
    if (err.message === 'Purchase order not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, approve, remove, generateAsn, uploadCsv, downloadPdf };

