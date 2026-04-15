const { PurchaseOrder, PurchaseOrderItem, Supplier, Product, SupplierProduct } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role === 'super_admin') {
    if (query.companyId) where.companyId = query.companyId;
  } else {
    where.companyId = reqUser.companyId;
  }
  if (query.status) where.status = query.status;

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
  const poNumber = `PO-${dateStr}-${String(count + 1).padStart(3, '0')}`;

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
    warehouseId: body.warehouseId || null,
    notes: body.notes || null,
  });

  const items = resolvedItems.map((i) => ({ ...i, purchaseOrderId: po.id }));
  if (items.length) await PurchaseOrderItem.bulkCreate(items);

  return getById(po.id, reqUser);
}

async function update(id, body, reqUser) {
  const po = await PurchaseOrder.findByPk(id);
  if (!po) throw new Error('Purchase order not found');
  if (reqUser.role !== 'super_admin' && po.companyId !== reqUser.companyId) throw new Error('Purchase order not found');
  if (po.status !== 'pending' && po.status !== 'draft') throw new Error('Only pending/draft PO can be updated');

  if (body.supplierId != null) po.supplierId = body.supplierId;
  if (body.clientId != null) po.clientId = body.clientId;
  if (body.expectedDelivery != null) po.expectedDelivery = body.expectedDelivery;
  if (body.warehouseId != null) po.warehouseId = body.warehouseId;
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
  return getById(id, reqUser);
}

async function approve(id, reqUser) {
  const po = await PurchaseOrder.findByPk(id);
  if (!po) throw new Error('Purchase order not found');
  if (reqUser.role !== 'super_admin' && po.companyId !== reqUser.companyId) throw new Error('Purchase order not found');
  if (po.status !== 'pending' && po.status !== 'draft') throw new Error('Only pending/draft PO can be approved');
  await po.update({ status: 'approved' });
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
  
  const count = await GoodsReceipt.count({ where: { companyId: po.companyId } });
  const grNumber = `GRN${String(count + 1).padStart(3, '0')}`;
  
  const gr = await GoodsReceipt.create({
    companyId: po.companyId,
    purchaseOrderId: po.id,
    warehouseId: body.warehouseId || po.warehouseId,
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

  await po.update({ status: 'asn_sent' }); // Update status to reflect ASN generated
  return { success: true, goodsReceiptId: gr.id };
}

module.exports = { list, getById, create, update, approve, remove, generateAsn };
