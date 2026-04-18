const { SupplierProduct, Supplier, Product } = require('../models');
const { sequelize } = require('../config/db');
const { Op } = require('sequelize');

function isBulkRowEmpty(row) {
  const vSku = String(row.supplierSku || '').trim();
  if (vSku.length > 0) return false;
  const sku = String(row.sku || '').trim();
  const pidRaw = row.productId;
  const pid = pidRaw !== undefined && pidRaw !== null && String(pidRaw).trim() !== '' ? Number(pidRaw) : NaN;
  const hasProduct = sku.length > 0 || (!Number.isNaN(pid) && pid > 0);
  const sidRaw = row.supplierId;
  const sid = sidRaw !== undefined && sidRaw !== null && String(sidRaw).trim() !== '' ? Number(sidRaw) : NaN;
  const sname = String(row.supplierName || '').trim();
  const scode = String(row.supplierCode || '').trim();
  const hasSupplier = (!Number.isNaN(sid) && sid > 0) || sname.length > 0 || scode.length > 0;
  return !hasProduct && !hasSupplier;
}

/** Match row for upsert: one row per (company, supplier, product, supplierSku); blank supplierSku = single "default" line. */
async function findSupplierProductMapping(companyId, supplierId, productId, supplierSkuRaw, transaction) {
  const norm = String(supplierSkuRaw ?? '').trim();
  const base = { companyId, supplierId, productId };
  const q = transaction ? { transaction } : {};
  if (norm === '') {
    return await SupplierProduct.findOne({
      where: {
        ...base,
        [Op.or]: [{ supplierSku: null }, { supplierSku: '' }],
      },
      ...q,
    });
  }
  return await SupplierProduct.findOne({
    where: { ...base, supplierSku: norm },
    ...q,
  });
}

/**
 * When forceCreate inserts another row for the same supplier+product, vendor SKU must be unique.
 * Tries the preferred value first, then "stem (1)", "stem (2)"… Empty preferred uses product-{id} as stem after first collision.
 */
async function allocateUniqueSupplierSku(companyId, supplierId, productId, preferredRaw, transaction) {
  const preferred = String(preferredRaw ?? '').trim();
  const stem = preferred || `product-${productId}`;
  for (let suffix = 0; suffix < 5000; suffix += 1) {
    const candidate = suffix === 0 ? preferred : `${stem} (${suffix})`;
    const keyForLookup = String(candidate).trim();
    const existing = await findSupplierProductMapping(
      companyId,
      supplierId,
      productId,
      keyForLookup,
      transaction
    );
    if (!existing) return keyForLookup;
  }
  throw new Error('Could not allocate a unique supplier SKU for this row');
}

/** Fold header so "Supplier ID", "supplier_id", "SUPPLIERID" all match. */
function foldHeaderKey(k) {
  return String(k || '')
    .replace(/^\uFEFF/, '')
    .replace(/\r/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');
}

/** Excel/Windows CSV often leaves \\r in keys and cells — breaks column matching. */
function stripCarriageReturnsFromRow(raw) {
  if (!raw || typeof raw !== 'object') return raw;
  const out = {};
  for (const [k, v] of Object.entries(raw)) {
    const kn = String(k).replace(/\r/g, '');
    if (typeof v === 'string') out[kn] = v.replace(/\r/g, '');
    else out[kn] = v;
  }
  return out;
}

function getRequestCompanyId(reqUser) {
  if (!reqUser) return null;
  let id = reqUser.companyId;
  if (id == null || id === '') id = typeof reqUser.get === 'function' ? reqUser.get('companyId') : null;
  if (id == null || id === '') {
    const c = reqUser.Company || reqUser.company;
    id = c && c.id != null ? c.id : null;
  }
  if (id === '' || id === undefined) return null;
  return id;
}

/** Map folded header → canonical field names (Excel often renames columns). */
const HEADER_FOLD_TO_FIELD = {
  supplierid: 'supplierId',
  suppliername: 'supplierName',
  suppliercode: 'supplierCode',
  /** Single-column exports often use "Supplier" / "Vendor" as the supplier name. */
  supplier: 'supplierName',
  vendor: 'supplierName',
  partner: 'supplierName',
  vendorid: 'supplierId',
  vendorname: 'supplierName',
  /** "Vendor code" = supplier account code, not vendor SKU (was wrongly mapped to supplierSku). */
  vendorcode: 'supplierCode',
  partnerid: 'supplierId',
  partnername: 'supplierName',
  partnercode: 'supplierCode',
  productid: 'productId',
  /** Explicit CSV headers (snake_case) map to same canonical fields after foldHeaderKey. */
  sku: 'sku',
  internalsku: 'sku',
  internalcode: 'sku',
  internalproductsku: 'sku',
  innersku: 'sku',
  productsku: 'sku',
  productcode: 'sku',
  itemcode: 'sku',
  stockcode: 'sku',
  stylecode: 'sku',
  itemsku: 'sku',
  mastersku: 'sku',
  articlenumber: 'sku',
  itemnumber: 'sku',
  materialnumber: 'sku',
  material: 'sku',
  suppliersku: 'supplierSku',
  vendorsku: 'supplierSku',
  supplierproductname: 'supplierProductName',
  supplierproduct: 'supplierProductName',
  packsize: 'packSize',
  costprice: 'costPrice',
  unitcost: 'costPrice',
  effectivedate: 'effectiveDate',
};

function cellValue(v) {
  if (v === undefined || v === null) return '';
  if (typeof v === 'string') {
    return v
      .replace(/\r/g, '')
      .replace(/[\u201C\u201D\u201E\u00AB\u00BB\u2033\u2036]/g, '"')
      .replace(/[\u2018\u2019\u201A\u2032\u2035]/g, "'")
      .trim();
  }
  if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  return String(v).replace(/\r/g, '').trim();
}

function normalizeUploadRow(raw) {
  if (!raw || typeof raw !== 'object') return {};
  const row = {};
  for (const [k, v] of Object.entries(raw)) {
    if (k === undefined || k === null) continue;
    const folded = foldHeaderKey(k);
    const field = HEADER_FOLD_TO_FIELD[folded];
    const outKey = field || String(k).replace(/^\uFEFF/, '').trim();
    row[outKey] = cellValue(v);
  }
  return row;
}

/** How many data rows have at least supplier + product hints after header mapping (used to pick best CSV delimiter/parser). */
function countMappableImportRows(rawRows) {
  if (!Array.isArray(rawRows)) return 0;
  let n = 0;
  for (const raw of rawRows) {
    const row = normalizeUploadRow(stripCarriageReturnsFromRow(raw));
    if (!isBulkRowEmpty(row)) n++;
  }
  return n;
}

/** Counts for import UI when rows fail or zero saved. */
function summarizeBulkSkips(errors, emptySkipped) {
  let invalidProductOrSku = 0;
  let supplierNotFound = 0;
  let formatOrMissing = 0;
  let other = 0;
  for (const e of errors) {
    const s = String(e);
    if (/product sku .+ not found|product id \d+ not found/i.test(s)) invalidProductOrSku += 1;
    else if (/supplier id \d+ not found|supplier not found/i.test(s)) supplierNotFound += 1;
    else if (/looked empty|No rows in upload|No data rows were parsed|set sku or productId|set supplierId|invalid effectiveDate|every data line|Row skip: missing sku|companyId missing|not linked to a company|No rows were saved even though|Re-export CSV/i.test(s)) {
      formatOrMissing += 1;
    } else other += 1;
  }
  return {
    emptyRows: emptySkipped,
    invalidProductOrSku,
    supplierNotFound,
    formatOrMissing,
    other,
    skippedApprox: emptySkipped + errors.length,
  };
}

/** YYYY-MM-DD for DB, or null. Never throws (bad CSV dates used to break the whole batch). */
function parseEffectiveDate(raw) {
  if (raw === undefined || raw === null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

async function list(reqUser, query = {}) {
  const where = { companyId: reqUser.companyId };
  if (query.supplierId) where.supplierId = query.supplierId;
  if (query.productId) where.productId = query.productId;
  
  return await SupplierProduct.findAll({
    where,
    include: [
      { model: Supplier, attributes: ['id', 'name', 'code'] },
      { model: Product, attributes: ['id', 'name', 'sku'] },
    ],
    order: [['createdAt', 'DESC']],
  });
}

async function listMappedProductsBySupplier(reqUser, supplierId) {
  const companyId = getRequestCompanyId(reqUser);
  if (!companyId) throw new Error('Company context required');
  const mappings = await SupplierProduct.findAll({
    where: { companyId, supplierId },
    include: [
      { model: Supplier, attributes: ['id', 'name', 'code'] },
      { model: Product, attributes: ['id', 'name', 'sku', 'reorderLevel'] },
    ],
    order: [['effectiveDate', 'DESC'], ['updatedAt', 'DESC']],
  });

  // Return every mapping row (multiple supplier SKUs per product are all valid PO lines).
  return mappings.map((m) => ({
    mappingId: m.id,
    productId: m.Product?.id || m.productId,
    productName: m.supplierProductName || m.Product?.name,
    productSku: m.supplierSku || m.Product?.sku,
    internalSku: m.Product?.sku || null,
    suggestedQuantity: Number(m.Product?.reorderLevel || 1),
    costPrice: Number(m.costPrice || 0),
    packSize: Number(m.packSize || 1),
    supplierId: m.supplierId,
  }));
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

const DEFAULT_BULK_CHUNK_SIZE = 250;

/** Load suppliers and products referenced by the CSV in a small number of queries (avoids per-row N+1). */
async function prefetchSuppliersAndProducts(companyId, normalizedItems) {
  const supplierIdSet = new Set();
  const supplierNameSet = new Set();
  const supplierCodeSet = new Set();
  const productIdSet = new Set();
  const skuLowerSet = new Set();

  for (const { row } of normalizedItems) {
    const sidRaw = row.supplierId;
    if (sidRaw !== undefined && sidRaw !== null && String(sidRaw).trim() !== '') {
      const sid = Number(sidRaw);
      if (!Number.isNaN(sid) && sid > 0) supplierIdSet.add(sid);
    }
    const sname = String(row.supplierName || '').trim();
    if (sname) supplierNameSet.add(sname);
    const scode = String(row.supplierCode || '').trim();
    if (scode) supplierCodeSet.add(scode);
    const pidRaw = row.productId;
    if (pidRaw !== undefined && pidRaw !== null && String(pidRaw).trim() !== '') {
      const pid = Number(pidRaw);
      if (!Number.isNaN(pid) && pid > 0) productIdSet.add(pid);
    }
    const sku = String(row.sku || '').trim();
    if (sku) skuLowerSet.add(sku.toLowerCase());
  }

  const supplierWhere = [];
  if (supplierIdSet.size) supplierWhere.push({ id: { [Op.in]: [...supplierIdSet] } });
  if (supplierNameSet.size) supplierWhere.push({ name: { [Op.in]: [...supplierNameSet] } });
  if (supplierCodeSet.size) supplierWhere.push({ code: { [Op.in]: [...supplierCodeSet] } });

  const suppliers =
    supplierWhere.length > 0
      ? await Supplier.findAll({ where: { companyId, [Op.or]: supplierWhere } })
      : [];

  const productWhere = [];
  if (productIdSet.size) productWhere.push({ id: { [Op.in]: [...productIdSet] } });
  if (skuLowerSet.size) {
    productWhere.push(
      sequelize.where(sequelize.fn('LOWER', sequelize.col('sku')), { [Op.in]: [...skuLowerSet] })
    );
  }

  const products =
    productWhere.length > 0
      ? await Product.findAll({ where: { companyId, [Op.or]: productWhere } })
      : [];

  const productById = new Map(products.map((p) => [p.id, p]));
  const productBySkuLower = new Map();
  for (const p of products) {
    productBySkuLower.set(String(p.sku).toLowerCase(), p);
  }

  return { suppliers, productById, productBySkuLower };
}

/** supplierSku → existing SupplierProduct rows (for “vendor columns only” CSV updates). */
async function prefetchMappingsBySupplierSku(companyId, normalizedItems) {
  const skuSet = new Set();
  for (const { row } of normalizedItems) {
    const s = String(row.supplierSku || '').trim();
    if (s) skuSet.add(s);
  }
  if (!skuSet.size) return new Map();
  const rows = await SupplierProduct.findAll({
    where: { companyId, supplierSku: { [Op.in]: [...skuSet] } },
    include: [
      { model: Supplier, attributes: ['id', 'name', 'code'] },
      { model: Product, attributes: ['id', 'name', 'sku'] },
    ],
  });
  const map = new Map();
  for (const sp of rows) {
    const k = String(sp.supplierSku || '').trim();
    if (!k) continue;
    if (!map.has(k)) map.set(k, []);
    map.get(k).push(sp);
  }
  return map;
}

function resolveSupplierFromPrefetch(row, suppliers) {
  const sidRaw = row.supplierId;
  if (sidRaw !== undefined && sidRaw !== null && String(sidRaw).trim() !== '') {
    const sid = Number(sidRaw);
    if (!Number.isNaN(sid) && sid > 0) {
      return suppliers.find((s) => s.id === sid) || null;
    }
  }
  const sname = String(row.supplierName || '').trim();
  const scode = String(row.supplierCode || '').trim();
  if (!sname && !scode) return null;
  return (
    suppliers.find((s) => (sname && s.name === sname) || (scode && s.code === scode)) || null
  );
}

/**
 * Resolve product by product_id and/or internal_sku (sku).
 * When both are set: product_id wins if it resolves. Internal sku may be a variant label (not in catalog);
 * we only error if sku maps to a *different* product than product_id.
 */
function resolveProductWithCrossCheck(row, productById, productBySkuLower) {
  const pidRaw = row.productId;
  const hasPid = pidRaw !== undefined && pidRaw !== null && String(pidRaw).trim() !== '';
  const pid = hasPid ? Number(pidRaw) : NaN;
  const sku = String(row.sku || '').trim();

  const byId = !Number.isNaN(pid) && pid > 0 ? productById.get(pid) || null : null;
  const bySku = sku ? productBySkuLower.get(sku.toLowerCase()) || null : null;

  if (hasPid && sku) {
    if (!byId && !bySku) {
      return { product: null, error: `product id ${pid} and internal_sku "${sku}" not found for this company` };
    }
    if (byId) {
      if (bySku && bySku.id !== byId.id) {
        return {
          product: null,
          error: `internal_sku "${sku}" maps to product ${bySku.id}, which conflicts with product_id ${pid}`,
        };
      }
      return { product: byId, error: null };
    }
    if (bySku) {
      if (Number(bySku.id) !== pid) {
        return {
          product: null,
          error: `product id ${pid} not found; internal_sku "${sku}" maps to product ${bySku.id} instead`,
        };
      }
      return { product: bySku, error: null };
    }
    return { product: null, error: `product id ${pid} not found for this company` };
  }

  if (hasPid && !Number.isNaN(pid) && pid > 0) {
    return { product: byId, error: byId ? null : `product id ${pid} not found for this company` };
  }
  if (sku) {
    return {
      product: bySku,
      error: bySku ? null : `product sku "${sku}" not found — add the product in catalog first`,
    };
  }
  return { product: null, error: 'set product_id or internal_sku (sku)' };
}

async function applyValidatedMappingRow(validated, companyId, transaction, options = {}) {
  const forceCreate = Boolean(options.forceCreate);
  const { row, supplier, product } = validated;
  const effectiveDate = parseEffectiveDate(row.effectiveDate);
  if (row.effectiveDate != null && String(row.effectiveDate).trim() !== '' && effectiveDate === null) {
    throw new Error('invalid effectiveDate (use YYYY-MM-DD or leave blank)');
  }
  const supplierSkuNorm = String(row.supplierSku ?? '').trim();
  let wasCreate = false;

  if (forceCreate) {
    const uniqueSku = await allocateUniqueSupplierSku(
      companyId,
      supplier.id,
      product.id,
      supplierSkuNorm,
      transaction
    );
    await SupplierProduct.create(
      {
        companyId,
        supplierId: supplier.id,
        productId: product.id,
        supplierSku: uniqueSku ? uniqueSku : null,
        supplierProductName: row.supplierProductName || product.name,
        packSize: Number(row.packSize) || 1,
        costPrice: Number(row.costPrice) || 0,
        effectiveDate: effectiveDate,
      },
      { transaction }
    );
    wasCreate = true;
  } else {
    let entry = await findSupplierProductMapping(
      companyId,
      supplier.id,
      product.id,
      supplierSkuNorm,
      transaction
    );
    if (!entry) {
      await SupplierProduct.create(
        {
          companyId,
          supplierId: supplier.id,
          productId: product.id,
          supplierSku: supplierSkuNorm || null,
          supplierProductName: row.supplierProductName || product.name,
          packSize: Number(row.packSize) || 1,
          costPrice: Number(row.costPrice) || 0,
          effectiveDate: effectiveDate,
        },
        { transaction }
      );
      wasCreate = true;
    } else {
      await entry.update(
        {
          supplierSku: supplierSkuNorm || entry.supplierSku,
          supplierProductName: row.supplierProductName || entry.supplierProductName,
          packSize: Number(row.packSize) || entry.packSize,
          costPrice: Number(row.costPrice) || entry.costPrice,
          effectiveDate: effectiveDate !== null ? effectiveDate : entry.effectiveDate,
        },
        { transaction }
      );
    }
  }

  const productUpdates = {};
  if (row.packSize !== undefined && row.packSize !== '') {
    const parsedPack = Number(row.packSize);
    if (!isNaN(parsedPack)) productUpdates.packSize = parsedPack;
  }
  productUpdates.supplierId = supplier.id;
  await product.update(productUpdates, { transaction });
  return wasCreate;
}

/**
 * Bulk upsert supplier↔product mappings from normalized CSV/JSON rows.
 * @param {object[]} mappings Raw rows (CSV objects or JSON array)
 * @param {object} reqUser Authenticated user (company scope)
 * @param {object} [options]
 * @param {number} [options.chunkSize=250] Rows per DB transaction (MySQL batch)
 * @param {boolean} [options.forceCreate] If true, never update existing mappings — insert every row (unique supplierSku allocated if needed).
 */
async function bulkUpload(mappings, reqUser, options = {}) {
  const chunkSize = Math.max(1, Math.min(Number(options.chunkSize) || DEFAULT_BULK_CHUNK_SIZE, 2000));
  const forceCreate = Boolean(options.forceCreate);
  const results = {
    created: 0,
    updated: 0,
    errors: [],
    failedRows: [],
    emptySkipped: 0,
    emptySkippedDetails: [],
    totalRowsReceived: Array.isArray(mappings) ? mappings.length : 0,
    totalRowsProcessed: 0,
  };
  const affectedProductIds = new Set();

  const companyId = getRequestCompanyId(reqUser);
  if (companyId == null) {
    results.errors.push(
      'Your user is not linked to a company (companyId missing). Assign this user to a company in admin, then try again.'
    );
    results.failedRowCount = 0;
    results.totalRowsProcessed = 0;
    results.skipSummary = summarizeBulkSkips(results.errors, 0);
    return results;
  }

  if (!Array.isArray(mappings) || mappings.length === 0) {
    results.errors.push('No rows in upload');
    results.failedRowCount = 0;
    results.totalRowsProcessed = 0;
    results.skipSummary = summarizeBulkSkips(results.errors, 0);
    return results;
  }

  const normalizedItems = [];
  let line = 0;
  for (const raw of mappings) {
    line += 1;
    const row = normalizeUploadRow(stripCarriageReturnsFromRow(raw));
    if (isBulkRowEmpty(row)) {
      results.emptySkipped += 1;
      if (results.emptySkippedDetails.length < 12) {
        const stripped = stripCarriageReturnsFromRow(raw);
        results.emptySkippedDetails.push({
          line,
          fileColumns: stripped && typeof stripped === 'object' ? Object.keys(stripped) : [],
        });
      }
      continue;
    }
    normalizedItems.push({ line, row, raw });
  }

  results.totalRowsProcessed = normalizedItems.length;

  if (normalizedItems.length === 0) {
    results.attemptedRows = results.totalRowsReceived - results.emptySkipped;
    results.succeededRows = 0;
    results.failedRowCount = 0;
    const sample = mappings[0] ? normalizeUploadRow(stripCarriageReturnsFromRow(mappings[0])) : {};
    const colHint = Object.keys(sample).length ? ` Parsed columns (first row): ${Object.keys(sample).join(', ')}.` : '';
    results.errors.push(
      `No rows imported: every data line looked empty after parsing.${colHint} Use CSV UTF-8 with supplier + product columns.`
    );
    results.skipSummary = summarizeBulkSkips(results.errors, results.emptySkipped);
    return results;
  }

  const { suppliers, productById, productBySkuLower } = await prefetchSuppliersAndProducts(
    companyId,
    normalizedItems
  );
  const byVendorSku = await prefetchMappingsBySupplierSku(companyId, normalizedItems);

  const validRows = [];
  for (const item of normalizedItems) {
    const { line, row } = item;
    const sku = String(row.sku || '').trim();
    const sidRaw = row.supplierId;
    const sname = String(row.supplierName || '').trim();
    const scode = String(row.supplierCode || '').trim();
    const vSku = String(row.supplierSku || '').trim();

    const hasSupplierHint =
      (sidRaw !== undefined && sidRaw !== null && String(sidRaw).trim() !== '') || sname || scode;

    let supplier = null;
    let product = null;

    if (hasSupplierHint) {
      supplier = resolveSupplierFromPrefetch(row, suppliers);
      if (!supplier) {
        const sidNum = sidRaw !== undefined && sidRaw !== null && String(sidRaw).trim() !== '' ? Number(sidRaw) : NaN;
        const msg = !Number.isNaN(sidNum) && sidNum > 0
          ? `Row ${line}: supplier id ${sidNum} not found for this company (sku: ${sku || '—'})`
          : `Row ${line}: supplier not found (${sname || scode || '—'}) for sku ${sku || '—'}`;
        results.errors.push(msg);
        results.failedRows.push({ rowNumber: line, reason: msg });
        continue;
      }

      const resolved = resolveProductWithCrossCheck(row, productById, productBySkuLower);
      if (!resolved.product || resolved.error) {
        const msg = `Row ${line}: ${resolved.error || 'product not resolved'}`;
        results.errors.push(msg);
        results.failedRows.push({ rowNumber: line, reason: msg });
        continue;
      }
      product = resolved.product;
    } else if (vSku) {
      const matches = byVendorSku.get(vSku) || [];
      if (matches.length === 0) {
        const msg = `Row ${line}: supplierSku "${vSku}" — koi pehle se mapping nahi mili. Pehle full row (supplierId + internal sku) se mapping banao, ya Export CSV template use karo.`;
        results.errors.push(msg);
        results.failedRows.push({ rowNumber: line, reason: msg });
        continue;
      }
      if (matches.length > 1) {
        const msg = `Row ${line}: supplierSku "${vSku}" ${matches.length} products se match — CSV mein supplierId aur sku / productId columns add karo.`;
        results.errors.push(msg);
        results.failedRows.push({ rowNumber: line, reason: msg });
        continue;
      }
      const sp = matches[0];
      supplier = sp.Supplier;
      product = sp.Product;
      if (!supplier || !product) {
        const msg = `Row ${line}: supplierSku "${vSku}" mapping corrupt (missing Supplier/Product).`;
        results.errors.push(msg);
        results.failedRows.push({ rowNumber: line, reason: msg });
        continue;
      }
    } else {
      const msg = `Row ${line}: supplierId / supplierName / supplierCode ya supplierSku set karo (internal_sku: ${sku || '—'})`;
      results.errors.push(msg);
      results.failedRows.push({ rowNumber: line, reason: msg });
      continue;
    }

    validRows.push({ line, row, supplier, product });
  }

  const applyChunk = async (chunk) => {
    let c = 0;
    let u = 0;
    const pids = new Set();
    const rowOpts = { forceCreate };
    await sequelize.transaction(async (transaction) => {
      for (const vr of chunk) {
        const wasCreate = await applyValidatedMappingRow(vr, companyId, transaction, rowOpts);
        if (wasCreate) c += 1;
        else u += 1;
        pids.add(vr.product.id);
      }
    });
    results.created += c;
    results.updated += u;
    for (const pid of pids) affectedProductIds.add(pid);
  };

  for (let i = 0; i < validRows.length; i += chunkSize) {
    const chunk = validRows.slice(i, i + chunkSize);
    try {
      await applyChunk(chunk);
    } catch (_err) {
      for (const vr of chunk) {
        try {
          let c = 0;
          let u = 0;
          await sequelize.transaction(async (transaction) => {
            const wasCreate = await applyValidatedMappingRow(vr, companyId, transaction, { forceCreate });
            if (wasCreate) c = 1;
            else u = 1;
          });
          results.created += c;
          results.updated += u;
          affectedProductIds.add(vr.product.id);
        } catch (e2) {
          const msg = `Row ${vr.line}: ${e2.message}`;
          results.errors.push(msg);
          results.failedRows.push({ rowNumber: vr.line, reason: e2.message });
        }
      }
    }
  }

  for (const productId of affectedProductIds) {
    try {
      await recalcProductCostPrice(productId, companyId);
    } catch (err) {
      results.errors.push(`Error recalculating cost for product ${productId}: ${err.message}`);
    }
  }

  results.attemptedRows = results.totalRowsReceived - results.emptySkipped;
  results.succeededRows = results.created + results.updated;
  results.failedRowCount = results.failedRows.length;

  if (results.created === 0 && results.updated === 0 && results.errors.length === 0) {
    const sample = mappings[0] ? normalizeUploadRow(stripCarriageReturnsFromRow(mappings[0])) : {};
    const colHint = Object.keys(sample).length ? ` Parsed columns (first row): ${Object.keys(sample).join(', ')}.` : '';
    if (results.emptySkipped > 0) {
      results.errors.push(
        `No rows imported: every data line looked empty after parsing.${colHint} Use "Export CSV" from this page, then upload as CSV UTF-8 (comma or tab).`
      );
    } else if (results.totalRowsReceived > 0) {
      results.errors.push(
        `No rows were saved even though the file contained lines.${colHint} Confirm supplier IDs and internal SKUs exist for your company.`
      );
    }
  }

  results.skipSummary = summarizeBulkSkips(results.errors, results.emptySkipped);
  results.importMode = forceCreate ? 'forceCreate' : 'upsert';
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

module.exports = {
  list,
  listMappedProductsBySupplier,
  bulkUpload,
  remove,
  getEffectiveCostPrice,
  recalcProductCostPrice,
  summarizeBulkSkips,
  countMappableImportRows,
};
