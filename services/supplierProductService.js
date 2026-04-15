const { SupplierProduct, Supplier, Product } = require('../models');
const { Op } = require('sequelize');

function normalizeUploadRow(raw) {
  if (!raw || typeof raw !== 'object') return {};
  const row = {};
  for (const [k, v] of Object.entries(raw)) {
    const key = String(k).replace(/^\uFEFF/, '').trim();
    row[key] = typeof v === 'string' ? v.trim() : v;
  }
  return row;
}

async function list(reqUser, query = {}) {
  const where = { companyId: reqUser.companyId };
  if (query.supplierId) where.supplierId = query.supplierId;
  if (query.productId) where.productId = query.productId;
  
  return await SupplierProduct.findAll({
    where,
    include: [
      { model: Supplier, attributes: ['id', 'name'] },
      { model: Product, attributes: ['id', 'name', 'sku'] },
    ],
    order: [['createdAt', 'DESC']],
  });
}

/**
 * Get the effective cost price for a product from all its suppliers.
 * Averages all supplier costs that are effective as of `asOfDate`.
 * If a supplier has multiple price entries, only the most recent effective one is used.
 * @param {number} productId 
 * @param {number} companyId 
 * @param {string|Date} asOfDate - The date to check effective prices against (defaults to today)
 * @returns {number|null} Average cost or null if no supplier prices found
 */
async function getEffectiveCostPrice(productId, companyId, asOfDate = null) {
  const dateToCheck = asOfDate ? new Date(asOfDate).toISOString().slice(0, 10) : new Date().toISOString().slice(0, 10);
  
  // Get all supplier mappings for this product
  const allMappings = await SupplierProduct.findAll({
    where: { productId, companyId },
    order: [['effectiveDate', 'DESC'], ['updatedAt', 'DESC']],
  });

  if (!allMappings.length) return null;

  // Group by supplier and pick the best (most recent effective) price per supplier
  const bySupplier = {};
  for (const sp of allMappings) {
    const sid = sp.supplierId;
    const effDate = sp.effectiveDate ? new Date(sp.effectiveDate).toISOString().slice(0, 10) : null;
    
    // Only include prices that are effective (effectiveDate is null/past/today)
    if (effDate && effDate > dateToCheck) continue;

    // Take the first one per supplier (already sorted by effectiveDate DESC)
    if (!bySupplier[sid]) {
      bySupplier[sid] = Number(sp.costPrice) || 0;
    }
  }

  const costs = Object.values(bySupplier);
  if (costs.length === 0) return null;

  // Average of all supplier costs
  const avg = costs.reduce((sum, c) => sum + c, 0) / costs.length;
  return Math.round(avg * 100) / 100; // Round to 2 decimal places
}

/**
 * Recalculate and update the product's costPrice based on all supplier prices.
 */
async function recalcProductCostPrice(productId, companyId) {
  const avgCost = await getEffectiveCostPrice(productId, companyId);
  if (avgCost !== null) {
    const product = await Product.findByPk(productId);
    if (product) {
      await product.update({ costPrice: avgCost });
    }
  }
}

async function bulkUpload(mappings, reqUser) {
  const results = { created: 0, updated: 0, errors: [] };
  // Track which products were affected so we recalculate once per product
  const affectedProductIds = new Set();
  
  for (const raw of mappings) {
    try {
      const row = normalizeUploadRow(raw);
      const orConds = [];
      const sidRaw = row.supplierId;
      if (sidRaw !== undefined && sidRaw !== null && String(sidRaw).trim() !== '') {
        const sid = Number(sidRaw);
        if (!Number.isNaN(sid) && sid > 0) orConds.push({ id: sid });
      }
      const sname = String(row.supplierName || '').trim();
      if (sname) orConds.push({ name: sname });
      const scode = String(row.supplierCode || '').trim();
      if (scode) orConds.push({ code: scode });

      if (orConds.length === 0) {
        results.errors.push(`Row skip: set supplierId, supplierName, or supplierCode (sku: ${row.sku || '—'})`);
        continue;
      }

      const supplier = await Supplier.findOne({
        where: { companyId: reqUser.companyId, [Op.or]: orConds },
      });

      const sku = String(row.sku || '').trim();
      if (!sku) {
        results.errors.push(`Row skip: missing sku (supplier: ${sname || scode || sidRaw || '—'})`);
        continue;
      }

      const product = await Product.findOne({ where: { companyId: reqUser.companyId, sku } });

      if (!supplier || !product) {
        results.errors.push(`Row skip: Supplier/Product not found (${sname || scode || sidRaw || '?'}/${sku})`);
        continue;
      }

      // Parse effectiveDate from the uploaded row
      const effectiveDate = row.effectiveDate ? new Date(row.effectiveDate).toISOString().slice(0, 10) : null;

      const [entry, created] = await SupplierProduct.findOrCreate({
        where: { 
          companyId: reqUser.companyId, 
          supplierId: supplier.id, 
          productId: product.id 
        },
        defaults: {
          supplierSku: row.supplierSku,
          supplierProductName: row.supplierProductName || product.name,
          packSize: Number(row.packSize) || 1,
          costPrice: Number(row.costPrice) || 0,
          effectiveDate: effectiveDate,
        }
      });
      
      if (!created) {
        await entry.update({
          supplierSku: row.supplierSku || entry.supplierSku,
          supplierProductName: row.supplierProductName || entry.supplierProductName,
          packSize: Number(row.packSize) || entry.packSize,
          costPrice: Number(row.costPrice) || entry.costPrice,
          effectiveDate: effectiveDate !== null ? effectiveDate : entry.effectiveDate,
        });
        results.updated++;
      } else {
        results.created++;
      }

      // Track for batch recalculation at the end
      affectedProductIds.add(product.id);

      // Auto-update packSize and supplierId on the base product
      const productUpdates = {};
      if (row.packSize !== undefined && row.packSize !== '') {
        const parsedPack = Number(row.packSize);
        if (!isNaN(parsedPack)) productUpdates.packSize = parsedPack;
      }
      productUpdates.supplierId = supplier.id;

      if (Object.keys(productUpdates).length > 0) {
        await product.update(productUpdates);
      }
    } catch (err) {
      const sku = raw && raw.sku != null ? String(raw.sku) : '—';
      results.errors.push(`Error processing ${sku}: ${err.message}`);
    }
  }

  // Recalculate costPrice for all affected products using multi-supplier average
  for (const productId of affectedProductIds) {
    try {
      await recalcProductCostPrice(productId, reqUser.companyId);
    } catch (err) {
      results.errors.push(`Error recalculating cost for product ${productId}: ${err.message}`);
    }
  }

  return results;
}

async function remove(id, reqUser) {
  const entry = await SupplierProduct.findByPk(id);
  if (!entry || (reqUser.role !== 'super_admin' && entry.companyId !== reqUser.companyId)) {
    throw new Error('Mapping not found');
  }
  const productId = entry.productId;
  const companyId = entry.companyId;
  await entry.destroy();

  // Recalculate the product's cost after removing a supplier mapping
  await recalcProductCostPrice(productId, companyId);

  return { deleted: true };
}

module.exports = { list, bulkUpload, remove, getEffectiveCostPrice, recalcProductCostPrice };
