const { VatCode } = require('../models');
const { Op } = require('sequelize');

function normalizeRole(role) {
  return (role || '').toString().toLowerCase().replace(/-/g, '_').trim();
}

async function list(req, res) {
  try {
    const role = normalizeRole(req.user.role);
    const where = {};
    if (role !== 'super_admin' && req.user.companyId) {
      where[Op.or] = [{ companyId: req.user.companyId }, { companyId: null }];
    }
    const list = await VatCode.findAll({
      where,
      order: [['code']],
    });
    let data = list.map((r) => r.get({ plain: true }));
    if (data.length === 0) {
      data = [
        { id: 'default-zero', code: 'ZERO', label: 'Zero rated (0%)', ratePercent: 0, countryCode: 'UK' },
        { id: 'default-reduced', code: 'REDUCED', label: 'Reduced (5%)', ratePercent: 5, countryCode: 'UK' },
        { id: 'default-standard', code: 'STANDARD', label: 'Standard (20%)', ratePercent: 20, countryCode: 'UK' },
      ];
    }
    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
}

async function create(req, res) {
  try {
    const { code, label, ratePercent, countryCode } = req.body;
    const companyId = req.user.companyId || null;
    const existing = await VatCode.findOne({
      where: { code: (code || '').trim(), companyId: companyId || null },
    });
    if (existing) {
      return res.status(400).json({ success: false, message: 'VAT code already exists' });
    }
    const row = await VatCode.create({
      companyId,
      code: (code || '').trim(),
      label: label ? String(label).trim() : null,
      ratePercent: ratePercent != null ? Number(ratePercent) : 0,
      countryCode: countryCode || 'UK',
    });
    res.status(201).json({ success: true, data: row.get({ plain: true }) });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
}

async function update(req, res) {
  try {
    const row = await VatCode.findByPk(req.params.id);
    if (!row) return res.status(404).json({ success: false, message: 'VAT code not found' });
    const role = normalizeRole(req.user.role);
    if (role !== 'super_admin' && row.companyId !== req.user.companyId) {
      return res.status(404).json({ success: false, message: 'VAT code not found' });
    }
    const { code, label, ratePercent, countryCode } = req.body;
    if (code !== undefined) row.code = (code || '').trim();
    if (label !== undefined) row.label = label ? String(label).trim() : null;
    if (ratePercent !== undefined) row.ratePercent = Number(ratePercent);
    if (countryCode !== undefined) row.countryCode = countryCode || 'UK';
    await row.save();
    res.json({ success: true, data: row.get({ plain: true }) });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
}

async function remove(req, res) {
  try {
    const row = await VatCode.findByPk(req.params.id);
    if (!row) return res.status(404).json({ success: false, message: 'VAT code not found' });
    const role = normalizeRole(req.user.role);
    if (role !== 'super_admin' && row.companyId !== req.user.companyId) {
      return res.status(404).json({ success: false, message: 'VAT code not found' });
    }
    await row.destroy();
    res.json({ success: true, message: 'VAT code deleted' });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
}

module.exports = { list, create, update, remove };
