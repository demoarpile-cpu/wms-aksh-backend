const { PurchaseOrder, PurchaseOrderItem, Supplier, Product, SupplierProduct } = require('../models');
const { Op } = require('sequelize');
const PDFDocument = require('pdfkit');
const auditLogService = require('./auditLogService');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role === 'super_admin') {
    if (query.companyId) where.companyId = query.companyId;
  } else {
    where.companyId = reqUser.companyId;
  }
  if (query.status) where.status = query.status;
  if (reqUser.clientId) {
    where.clientId = reqUser.clientId;
  } else if (query.clientId) {
    where.clientId = query.clientId;
  }

  const pos = await PurchaseOrder.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      { association: 'Supplier', attributes: ['id', 'name', 'code'] },
      { association: 'Warehouse', attributes: ['id', 'name', 'code'], required: false },
      { association: 'Client', attributes: ['id', 'name'], required: false },
      { association: 'PurchaseOrderItems', include: [{ association: 'Product', attributes: ['id', 'name', 'sku'] }] },
    ],
  });
  return pos;
}

async function getById(id, reqUser) {
  const po = await PurchaseOrder.findByPk(id, {
    include: [
      { association: 'Supplier' },
      { association: 'Client', attributes: ['id', 'name'] },
      { association: 'Warehouse', attributes: ['id', 'name', 'code'] },
      { association: 'PurchaseOrderItems', include: ['Product'] },
    ],
  });
  if (!po) throw new Error('Purchase order not found');
  if (reqUser.role !== 'super_admin' && po.companyId !== reqUser.companyId) throw new Error('Purchase order not found');
  if (reqUser.clientId && po.clientId !== reqUser.clientId) throw new Error('Not authorized to access this client data');
  return po;
}

async function create(body, reqUser) {
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'company_admin' && reqUser.role !== 'warehouse_manager' && reqUser.role !== 'inventory_manager') {
    throw new Error('Not allowed to create purchase orders');
  }
  // super_admin can pass companyId in body; others use their company
  const companyId = reqUser.role === 'super_admin' ? (body.companyId || reqUser.companyId) : reqUser.companyId;
  if (!companyId) throw new Error('Company context required');

  const dateStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const count = await PurchaseOrder.count({ where: { companyId } });
  const poNumber = body.poNumber || `PO-${dateStr}-${String(count + 1).padStart(3, '0')}`;

  const supplier = await Supplier.findByPk(body.supplierId);
  if (!supplier || supplier.companyId !== companyId) throw new Error('Invalid supplier');

  // Auto-fill unitPrice from effective supplier price when not provided
  const priceDate = body.expectedDelivery || new Date().toISOString().slice(0, 10);
  const rawItems = body.items || [];
  const resolvedItems = [];
  for (const i of rawItems) {
    let unitPrice = Number(i.unitPrice) || 0;
    // If unitPrice is 0/empty, look up effective supplier price
    if (!unitPrice && i.productId && body.supplierId) {
      const sp = await SupplierProduct.findAll({
        where: { companyId, supplierId: body.supplierId, productId: i.productId },
        order: [['effectiveDate', 'DESC'], ['updatedAt', 'DESC']],
      });
      for (const entry of sp) {
        const effDate = entry.effectiveDate ? new Date(entry.effectiveDate).toISOString().slice(0, 10) : null;
        // Only use prices effective on or before the PO delivery/creation date
        if (!effDate || effDate <= priceDate) {
          unitPrice = Number(entry.costPrice) || 0;
          break;
        }
      }
    }
    resolvedItems.push({
      purchaseOrderId: null, // set below after PO creation
      productId: i.productId,
      productName: i.productName || null,
      productSku: i.productSku || null,
      quantity: Number(i.quantity) || 0,
      supplierQuantity: Number(i.supplierQuantity) || 0,
      packSize: Number(i.packSize) || 1,
      unitPrice,
      totalPrice: (Number(i.quantity) || 0) * unitPrice,
    });
  }

  const totalAmount = resolvedItems.reduce((sum, i) => sum + (i.totalPrice || 0), 0);

  const po = await PurchaseOrder.create({
    companyId,
    supplierId: body.supplierId,
    clientId: body.clientId || null,
    poNumber,
    status: (body.status || 'pending').toLowerCase(),
    totalAmount,
    expectedDelivery: body.expectedDelivery || null,
    // Warehouse is set at goods receiving (GRN), not at PO creation.
    warehouseId: null,
    notes: body.notes || null,
  });

  const items = resolvedItems.map((i) => ({ ...i, purchaseOrderId: po.id }));
  if (items.length) await PurchaseOrderItem.bulkCreate(items);

  await auditLogService.logAction(reqUser, {
    action: 'PO_CREATED',
    module: 'INBOUND',
    referenceId: po.id,
    referenceNumber: po.poNumber,
    details: { totalAmount: po.totalAmount, itemCount: items.length }
  });

  return getById(po.id, reqUser);
}

async function update(id, body, reqUser) {
  const po = await PurchaseOrder.findByPk(id);
  if (!po) throw new Error('Purchase order not found');
  if (reqUser.role !== 'super_admin' && po.companyId !== reqUser.companyId) throw new Error('Purchase order not found');
  if (reqUser.clientId && po.clientId !== reqUser.clientId) throw new Error('Not authorized to access this client data');
  if (po.status !== 'pending' && po.status !== 'draft') throw new Error('Only pending/draft PO can be updated');

  if (body.supplierId != null) po.supplierId = body.supplierId;
  if (body.clientId !== undefined) po.clientId = body.clientId;
  if (body.expectedDelivery != null) po.expectedDelivery = body.expectedDelivery;
  // Warehouse is intentionally assigned at GRN stage, not PO stage.
  if (body.notes != null) po.notes = body.notes;
  if (body.status != null) po.status = (body.status).toLowerCase();

  if (Array.isArray(body.items) && body.items.length > 0) {
    await PurchaseOrderItem.destroy({ where: { purchaseOrderId: id } });
    const totalAmount = body.items.reduce((sum, i) => sum + (Number(i.unitPrice) || 0) * (Number(i.quantity) || 0), 0);
    po.totalAmount = totalAmount;
    await po.save();
    await PurchaseOrderItem.bulkCreate(body.items.map((i) => ({
      purchaseOrderId: id,
      productId: i.productId,
      productName: i.productName || null,
      productSku: i.productSku || null,
      quantity: Number(i.quantity) || 0,
      supplierQuantity: Number(i.supplierQuantity) || 0,
      packSize: Number(i.packSize) || 1,
      unitPrice: Number(i.unitPrice) || 0,
      totalPrice: (Number(i.quantity) || 0) * (Number(i.unitPrice) || 0),
    })));
  } else {
    await po.save();
  }

  await auditLogService.logAction(reqUser, {
    action: 'PO_UPDATED',
    module: 'INBOUND',
    referenceId: po.id,
    referenceNumber: po.poNumber
  });

  return getById(id, reqUser);
}

async function approve(id, body, reqUser) {
  const po = await PurchaseOrder.findByPk(id, { include: ['PurchaseOrderItems'] });
  if (!po) throw new Error('Purchase order not found');
  if (reqUser.role !== 'super_admin' && po.companyId !== reqUser.companyId) throw new Error('Purchase order not found');
  if (reqUser.clientId && po.clientId !== reqUser.clientId) throw new Error('Not authorized to access this client data');
  if (po.status !== 'pending' && po.status !== 'draft') throw new Error('Only pending/draft PO can be approved/rejected');

  const action = String(body.action || 'approve').toLowerCase();
  if (action === 'reject') {
    await po.update({ status: 'rejected' });
    return getById(id, reqUser);
  }

  if (Array.isArray(body.items) && body.items.length > 0) {
    for (const item of body.items) {
      const idNum = Number(item.id);
      if (!idNum) continue;
      const confirmedQty = Number(item.confirmedQuantity);
      if (!Number.isFinite(confirmedQty) || confirmedQty < 0) continue;
      await PurchaseOrderItem.update(
        { supplierQuantity: confirmedQty },
        { where: { id: idNum, purchaseOrderId: po.id } }
      );
    }
  }
  await po.update({
    status: 'approved',
    expectedDelivery: body.expectedDeliveryDate || body.expectedDelivery || po.expectedDelivery,
  });
  await generateAsn(id, {
    eta: body.expectedDeliveryDate || body.expectedDelivery || po.expectedDelivery,
    notes: body.notes || `Auto ASN generated on approval for ${po.poNumber}`,
  }, reqUser);

  await auditLogService.logAction(reqUser, {
    action: action === 'reject' ? 'PO_REJECTED' : 'PO_APPROVED',
    module: 'INBOUND',
    referenceId: po.id,
    referenceNumber: po.poNumber
  });

  return getById(id, reqUser);
}

async function remove(id, reqUser) {
  const po = await PurchaseOrder.findByPk(id);
  if (!po) throw new Error('Purchase order not found');
  if (reqUser.role !== 'super_admin' && po.companyId !== reqUser.companyId) throw new Error('Purchase order not found');
  if (po.status !== 'pending' && po.status !== 'draft') throw new Error('Only pending/draft PO can be deleted');
  await PurchaseOrderItem.destroy({ where: { purchaseOrderId: id } });
  await po.destroy();
  return { deleted: true };
}

async function generateAsn(id, body, reqUser) {
  const po = await PurchaseOrder.findByPk(id, { include: ['PurchaseOrderItems'] });
  if (!po) throw new Error('Purchase order not found');
  if (reqUser.role !== 'super_admin' && po.companyId !== reqUser.companyId) throw new Error('Purchase order not found');
  if (po.status !== 'approved' && po.status !== 'asn_sent') throw new Error('Only approved PO can generate ASN');

  // Logic: Create a pending GoodsReceipt from the PO items
  const { GoodsReceipt, GoodsReceiptItem } = require('../models');
  
  const existing = await GoodsReceipt.findOne({ where: { purchaseOrderId: po.id } });
  if (existing) return { success: true, goodsReceiptId: existing.id, reused: true };

  const count = await GoodsReceipt.count({ where: { companyId: po.companyId } });
  const grNumber = `GRN${String(count + 1).padStart(3, '0')}`;
  
  const gr = await GoodsReceipt.create({
    companyId: po.companyId,
    purchaseOrderId: po.id,
    clientId: po.clientId || null,
    // Warehouse is selected during GRN/asn receiving flow.
    warehouseId: body.warehouseId || null,
    deliveryType: body.deliveryType || 'carton',
    eta: body.eta || null,
    grNumber,
    status: 'pending',
    totalExpected: (po.PurchaseOrderItems || []).reduce((s, i) => s + (i.quantity || 0), 0),
    totalReceived: 0,
    notes: body.notes || `ASN generated from ${po.poNumber}`,
  });

  const grItems = (po.PurchaseOrderItems || []).map(i => ({
    goodsReceiptId: gr.id,
    productId: i.productId,
    productName: i.productName,
    productSku: i.productSku,
    expectedQty: i.quantity,
    receivedQty: 0,
    qtyToBook: i.quantity, // Default qty to book is the expected qty
  }));
  if (grItems.length) await GoodsReceiptItem.bulkCreate(grItems);

  await po.update({ status: 'asn_sent' });
  return { success: true, goodsReceiptId: gr.id };
}

function mapCsvRow(row) {
  const folded = {};
  for (const [k, v] of Object.entries(row || {})) {
    const key = String(k || '').trim().toLowerCase().replace(/[\s_-]+/g, '');
    folded[key] = typeof v === 'string' ? v.trim() : v;
  }
  const finalQtyRaw =
    folded.finalquantity ??
    folded.emptyquantity ??
    folded.quantity ??
    folded.confirmedquantity ??
    folded.editablequantity ??
    folded.editableqty;
  const suggestedQtyRaw = folded.suggestedquantity ?? folded.suggestedqty;
  return {
    productId: Number(folded.productid || folded.id) || 0,
    sku: String(folded.sku || '').trim(),
    productName: String(folded.productname || folded.product || '').trim(),
    finalQuantity: Number(finalQtyRaw) || 0,
    suggestedQuantity: Number(suggestedQtyRaw) || 0,
  };
}

function validateCsvHeaders(rows) {
  const first = rows[0] || {};
  const keys = Object.keys(first).map((k) => String(k || '').trim().toLowerCase().replace(/[\s_-]+/g, ''));
  const hasIdentity = ['productid', 'sku', 'productname', 'product'].some((k) => keys.includes(k));
  const hasQty = [
    'finalquantity',
    'emptyquantity',
    'suggestedquantity',
    'suggestedqty',
    'quantity',
    'confirmedquantity',
    'editablequantity',
    'editableqty',
  ].some((k) => keys.includes(k));
  if (!hasIdentity || !hasQty) {
    throw new Error('Invalid CSV headers. Required: Product ID or SKU or Product Name, and Final Quantity or Suggested Quantity.');
  }
}

function normalizeLookup(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
    .replace(/^0+/, '');
}

async function createFromCsv(body, reqUser) {
  if (!body?.supplierId) throw new Error('supplierId is required');
  if (!Array.isArray(body.rows) || body.rows.length === 0) throw new Error('CSV has no data rows');
  const companyId = reqUser.role === 'super_admin' ? (body.companyId || reqUser.companyId) : reqUser.companyId;
  if (!companyId) throw new Error('Company context required');

  const supplier = await Supplier.findByPk(body.supplierId);
  if (!supplier || supplier.companyId !== companyId) throw new Error('Invalid supplier');
  validateCsvHeaders(body.rows);

  const csvItems = body.rows.map(mapCsvRow).filter((r) => (r.productId || r.sku || r.productName) && (r.finalQuantity > 0 || r.suggestedQuantity > 0));
  if (!csvItems.length) throw new Error('Final Quantity column me data required hai');

  const supplierMappings = await SupplierProduct.findAll({
    where: { companyId, supplierId: body.supplierId },
    include: [{ model: Product, attributes: ['id', 'name', 'sku'] }],
    order: [['effectiveDate', 'DESC'], ['updatedAt', 'DESC']],
  });

  const bySku = new Map();
  const byName = new Map();
  for (const m of supplierMappings) {
    const product = m.Product;
    if (!product) continue;
    const lookup = { map: m, product };
    const skuKeys = [
      String(product.sku || '').trim().toLowerCase(),
      String(m.supplierSku || '').trim().toLowerCase(),
      normalizeLookup(product.sku),
      normalizeLookup(m.supplierSku),
    ].filter(Boolean);
    const nameKeys = [
      String(product.name || '').trim().toLowerCase(),
      String(m.supplierProductName || '').trim().toLowerCase(),
      normalizeLookup(product.name),
      normalizeLookup(m.supplierProductName),
    ].filter(Boolean);
    skuKeys.forEach((k) => {
      if (!bySku.has(k)) bySku.set(k, lookup);
    });
    nameKeys.forEach((k) => {
      if (!byName.has(k)) byName.set(k, lookup);
    });
  }

  const items = [];
  for (const row of csvItems) {
    if (Number(row.productId) > 0) {
      const mappedById = supplierMappings.find((m) => Number(m.productId) === Number(row.productId) && m.Product);
      if (mappedById) {
        const quantity = row.finalQuantity > 0 ? row.finalQuantity : row.suggestedQuantity;
        const unitPrice = Number(mappedById.costPrice) || 0;
        items.push({
          productId: mappedById.Product.id,
          productName: mappedById.supplierProductName || mappedById.Product.name,
          productSku: mappedById.supplierSku || mappedById.Product.sku,
          quantity,
          supplierQuantity: quantity,
          packSize: Number(mappedById.packSize) || 1,
          unitPrice,
        });
        continue;
      }
    }
    const skuRaw = String(row.sku || '').trim().toLowerCase();
    const nameRaw = String(row.productName || '').trim().toLowerCase();
    const skuNorm = normalizeLookup(row.sku);
    const nameNorm = normalizeLookup(row.productName);
    const picked =
      bySku.get(skuRaw) ||
      bySku.get(skuNorm) ||
      byName.get(nameRaw) ||
      byName.get(nameNorm);
    if (!picked) continue;
    const quantity = row.finalQuantity > 0 ? row.finalQuantity : row.suggestedQuantity;
    const unitPrice = Number(picked.map.costPrice) || 0;
    items.push({
      productId: picked.product.id,
      productName: picked.map.supplierProductName || picked.product.name,
      productSku: picked.map.supplierSku || picked.product.sku,
      quantity,
      supplierQuantity: quantity,
      packSize: Number(picked.map.packSize) || 1,
      unitPrice,
    });
  }
  if (!items.length) throw new Error('Final Quantity column me data required hai');

  let po = null;
  if (body.poNumber) {
    const existing = await PurchaseOrder.findOne({
      where: {
        companyId,
        poNumber: body.poNumber,
        status: { [Op.in]: ['pending', 'draft'] },
      },
    });
    if (existing) {
      po = await update(existing.id, {
        supplierId: body.supplierId,
        clientId: body.clientId || null,
        expectedDelivery: body.expectedDelivery || null,
        notes: body.notes || null,
        items,
      }, reqUser);
    }
  }
  if (!po) {
    po = await create({
      supplierId: body.supplierId,
      clientId: body.clientId || null,
      expectedDelivery: body.expectedDelivery || null,
      notes: body.notes || null,
      poNumber: body.poNumber || null,
      items,
    }, reqUser);
  }
  return {
    purchaseOrder: po,
    pdfDownloadUrl: `/api/purchase-orders/${po.id}/pdf`,
  };
}

async function generatePoPdf(id, reqUser) {
  const po = await getById(id, reqUser);
  const doc = new PDFDocument({ size: 'A4', margin: 40 });
  const buffers = [];
  doc.on('data', (d) => buffers.push(d));

  doc.fontSize(20).text('Purchase Order', { align: 'left' });
  doc.moveDown(0.3);
  doc.fontSize(11).text(`PO Number: ${po.poNumber}`);
  doc.text(`Supplier: ${po.Supplier?.name || '-'}`);
  doc.text(`Status: ${(po.status || '').toUpperCase()}`);
  doc.text(`Expected Delivery: ${po.expectedDelivery ? new Date(po.expectedDelivery).toISOString().slice(0, 10) : '-'}`);
  doc.moveDown();
  doc.fontSize(12).text('Items', { underline: true });
  doc.moveDown(0.5);
  doc.fontSize(10).text('SKU', 40, doc.y, { width: 100 });
  doc.text('Product', 140, doc.y - 12, { width: 200 });
  doc.text('Qty', 350, doc.y - 12, { width: 50, align: 'right' });
  doc.text('Unit Price', 410, doc.y - 12, { width: 70, align: 'right' });
  doc.text('Total', 490, doc.y - 12, { width: 70, align: 'right' });
  doc.moveDown(0.4);

  for (const item of (po.PurchaseOrderItems || [])) {
    const lineY = doc.y;
    doc.fontSize(10).text(item.productSku || '-', 40, lineY, { width: 100 });
    doc.text(item.productName || '-', 140, lineY, { width: 200 });
    doc.text(String(item.quantity || 0), 350, lineY, { width: 50, align: 'right' });
    doc.text(Number(item.unitPrice || 0).toFixed(2), 410, lineY, { width: 70, align: 'right' });
    doc.text(Number(item.totalPrice || 0).toFixed(2), 490, lineY, { width: 70, align: 'right' });
    doc.moveDown(0.6);
  }

  doc.moveDown();
  doc.fontSize(12).text(`Total Amount: ${Number(po.totalAmount || 0).toFixed(2)}`, { align: 'right' });
  if (po.notes) {
    doc.moveDown();
    doc.fontSize(10).text(`Notes: ${po.notes}`);
  }
  doc.end();
  const buffer = await new Promise((resolve) => doc.on('end', () => resolve(Buffer.concat(buffers))));
  return { buffer, filename: `${po.poNumber || `PO-${po.id}`}.pdf` };
}

module.exports = { list, getById, create, update, approve, remove, generateAsn, createFromCsv, generatePoPdf };
