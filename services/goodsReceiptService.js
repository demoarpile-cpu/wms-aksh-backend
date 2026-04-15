const { GoodsReceipt, GoodsReceiptItem, PurchaseOrder, PurchaseOrderItem, Supplier, Product, ProductStock, Warehouse } = require('../models');
const { validateHeatSensitivePlacement } = require('./inventoryService');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role === 'super_admin') {
    if (query.companyId) where.companyId = query.companyId;
  } else {
    where.companyId = reqUser.companyId;
  }
  if (query.status) where.status = query.status;

  const receipts = await GoodsReceipt.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      { association: 'PurchaseOrder', include: [{ association: 'Supplier', attributes: ['id', 'name'] }] },
      { association: 'Warehouse', attributes: ['id', 'name', 'code'] },
      { association: 'GoodsReceiptItems', include: [{ association: 'Product', attributes: ['id', 'name', 'sku'] }] },
    ],
  });
  applyGrnDisplayNormalization(receipts);
  return receipts;
}

function applyGrnDisplayNormalization(receipts) {
  const byCompany = {};
  receipts.forEach((r) => {
    const cid = r.companyId;
    if (!byCompany[cid]) byCompany[cid] = [];
    byCompany[cid].push(r);
  });
  Object.values(byCompany).forEach((arr) => {
    const newFormatNums = arr.map((r) => (r.grNumber || '').match(/^GRN(\d+)$/i)).filter(Boolean).map((m) => parseInt(m[1], 10));
    const nextNum = newFormatNums.length > 0 ? Math.max(...newFormatNums) + 1 : 1;
    const oldFormat = arr.filter((r) => /^GRN-\d+-\d+$/i.test((r.grNumber || '').trim()));
    const oldSorted = [...oldFormat].sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
    let n = nextNum;
    oldSorted.forEach((r) => {
      r.setDataValue('grNumber', `GRN${String(n).padStart(3, '0')}`);
      n += 1;
    });
  });
}

async function getById(id, reqUser) {
  const gr = await GoodsReceipt.findByPk(id, {
    include: [
      { association: 'PurchaseOrder', include: ['Supplier'] },
      { association: 'Warehouse', attributes: ['id', 'name', 'code'] },
      { association: 'GoodsReceiptItems', include: ['Product'] },
    ],
  });
  if (!gr) throw new Error('Goods receipt not found');
  if (reqUser.role !== 'super_admin' && gr.companyId !== reqUser.companyId) throw new Error('Goods receipt not found');
  if (/^GRN-\d+-\d+$/i.test((gr.grNumber || '').trim())) {
    const all = await GoodsReceipt.findAll({ where: { companyId: gr.companyId }, order: [['createdAt', 'ASC']] });
    applyGrnDisplayNormalization(all);
    const found = all.find((r) => r.id === gr.id);
    if (found) gr.setDataValue('grNumber', found.grNumber);
  }
  return gr;
}

async function create(body, reqUser) {
  const companyId = reqUser.role === 'super_admin' ? (body.companyId || reqUser.companyId) : reqUser.companyId;
  if (!companyId) throw new Error('Company context required');

  const po = await PurchaseOrder.findByPk(body.purchaseOrderId, {
    include: [{ association: 'PurchaseOrderItems', include: [{ association: 'Product', attributes: ['id', 'name', 'sku'] }] }],
  });
  if (!po || po.companyId !== companyId) throw new Error('Purchase order not found');
  if ((po.status || '').toLowerCase() !== 'approved') throw new Error('Only approved purchase orders can be received');

  // GRN number format: GRN001, GRN002, GRN003, ... (sequential per company)
  const all = await GoodsReceipt.findAll({ where: { companyId }, attributes: ['grNumber'], raw: true });
  const existingNums = all.map((r) => (r.grNumber || '').match(/^GRN(\d+)$/i)).filter(Boolean).map((m) => parseInt(m[1], 10));
  const nextNum = existingNums.length > 0 ? Math.max(...existingNums) + 1 : 1;
  const grNumber = `GRN${String(nextNum).padStart(3, '0')}`;

  const totalExpected = (po.PurchaseOrderItems || []).reduce((s, i) => s + (Number(i.quantity) || 0), 0);

  const gr = await GoodsReceipt.create({
    companyId,
    purchaseOrderId: po.id,
    warehouseId: po.warehouseId || body.warehouseId || null,
    grNumber,
    status: 'pending',
    notes: body.notes || null,
    totalExpected,
    totalReceived: 0,
  });

  const items = (po.PurchaseOrderItems || []).map((i) => ({
    goodsReceiptId: gr.id,
    productId: i.productId,
    productName: (i.productName && i.productName.trim()) ? i.productName.trim() : (i.Product?.name || null),
    productSku: (i.productSku && i.productSku.trim()) ? i.productSku.trim() : (i.Product?.sku || null),
    expectedQty: Number(i.quantity) || 0,
    receivedQty: 0,
    qualityStatus: null,
    unitCost: Number(i.unitPrice) || null,
  }));
  if (items.length) await GoodsReceiptItem.bulkCreate(items);

  return getById(gr.id, reqUser);
}

async function updateReceived(id, body, reqUser) {
  const gr = await GoodsReceipt.findByPk(id, { include: ['GoodsReceiptItems'] });
  if (!gr) throw new Error('Goods receipt not found');
  if (reqUser.role !== 'super_admin' && gr.companyId !== reqUser.companyId) throw new Error('Goods receipt not found');
  if (gr.status === 'completed') throw new Error('Receipt already completed');

  // Keep old values to compute delta for stock update (partial receive should also update stock)
  const oldByProduct = {};
  (gr.GoodsReceiptItems || []).forEach((l) => {
    oldByProduct[l.productId] = { receivedQty: Number(l.receivedQty) || 0, qualityStatus: (l.qualityStatus || '').toUpperCase() };
  });

  const items = body.items || [];
  for (const row of items) {
    const line = gr.GoodsReceiptItems?.find((i) => i.productId === row.productId || i.id === row.id);
    if (line) {
      const receivedQty = Number(row.receivedQty) ?? line.receivedQty;
      await line.update({ receivedQty, qualityStatus: row.qualityStatus || line.qualityStatus });
    }
  }
  const updated = await GoodsReceipt.findByPk(gr.id, { include: ['GoodsReceiptItems'] });
  const newTotal = (updated.GoodsReceiptItems || []).reduce((s, i) => s + (Number(i.receivedQty) || 0), 0);
  const allReceived = (updated.GoodsReceiptItems || []).every((i) => (Number(i.receivedQty) || 0) >= (Number(i.expectedQty) || 0));
  await updated.update({
    totalReceived: newTotal,
    status: allReceived ? 'completed' : 'in_progress',
  });

  // Add received quantity (delta) to stock — existing inventory record update karo, naya sirf jab koi record na ho.
  const companyWarehouses = await Warehouse.findAll({ where: { companyId: updated.companyId }, attributes: ['id'], order: [['id', 'ASC']] });
  const warehouseIds = (companyWarehouses || []).map((w) => w.id);
  const defaultWarehouseId = warehouseIds.length > 0 ? warehouseIds[0] : null;
  let stockUpdated = false;
  let stockWarning = null;
  if (!defaultWarehouseId && (updated.GoodsReceiptItems || []).some((i) => (Number(i.receivedQty) || 0) > 0)) {
    stockWarning = 'No warehouse found. Create a warehouse (Warehouses → Add Warehouse) to update inventory stock.';
  }
  if (warehouseIds.length > 0 && (updated.GoodsReceiptItems || []).length > 0) {
    const warehouseService = require('./warehouseService');
    for (const line of updated.GoodsReceiptItems) {
      const pid = Number(line.productId) || line.productId;
      const newReceived = Number(line.receivedQty) || 0;
      const old = oldByProduct[pid] || oldByProduct[line.productId] || { receivedQty: 0, qualityStatus: '' };
      const delta = newReceived - (Number(old.receivedQty) || 0);
      if (delta <= 0) continue;
      const quality = (line.qualityStatus || '').toUpperCase();
      const qtyToAdd = quality === 'DAMAGED' ? 0 : delta;
      if (qtyToAdd <= 0) continue;

      const whId = gr.warehouseId || defaultWarehouseId;
      if (whId) {
        await warehouseService.validateCapacity(whId, qtyToAdd);
      }

      try {
        // Pehle is product ka koi existing stock record dhoondo (company ke kisi bhi warehouse me) — naya record mat banao jab tak existing na mile
        let stock = await ProductStock.findOne({
          where: { productId: pid, warehouseId: whId },
        });
        if (stock) {
          await stock.update({ quantity: (Number(stock.quantity) || 0) + qtyToAdd });
        } else {
          await ProductStock.create({
            productId: pid,
            warehouseId: whId,
            quantity: qtyToAdd,
            reserved: 0,
            status: 'ACTIVE',
          });
        }
        stockUpdated = true;
      } catch (err) {
        stockWarning = (stockWarning ? stockWarning + ' ' : '') + (err.message || 'Stock update failed.');
      }
    }
  }

  const result = await getById(gr.id, reqUser);
  const plain = result.get ? result.get({ plain: true }) : (result.toJSON ? result.toJSON() : result);
  return { ...plain, stockUpdated: !!stockUpdated, stockWarning: stockWarning || undefined };
}

async function updateAsnItems(id, body, reqUser) {
  const gr = await GoodsReceipt.findByPk(id, { include: ['GoodsReceiptItems'] });
  if (!gr) throw new Error('ASN not found');
  if (reqUser.role !== 'super_admin' && gr.companyId !== reqUser.companyId) throw new Error('Not authorized');

  if (body.deliveryType) gr.deliveryType = body.deliveryType;
  if (body.eta) gr.eta = body.eta;
  if (body.warehouseId) gr.warehouseId = body.warehouseId;
  await gr.save();

  if (Array.isArray(body.items)) {
    for (const item of body.items) {
      const dbItem = gr.GoodsReceiptItems.find(i => i.id === item.id);
      if (dbItem) {
        await dbItem.update({
          batchId: item.batchId || dbItem.batchId,
          bestBeforeDate: item.bestBeforeDate || dbItem.bestBeforeDate,
          qtyToBook: item.qtyToBook ?? dbItem.qtyToBook,
          locationId: item.locationId || dbItem.locationId,
          qualityStatus: item.qualityStatus || dbItem.qualityStatus
        });
      }
    }
  }

  return getById(id, reqUser);
}

async function finalizeReceiving(id, reqUser) {
  const gr = await GoodsReceipt.findByPk(id, { include: ['GoodsReceiptItems'] });
  if (!gr) throw new Error('ASN not found');
  if (reqUser.role !== 'super_admin' && gr.companyId !== reqUser.companyId) throw new Error('Not authorized');
  if (gr.status === 'completed') throw new Error('Already finalized');

  // Transaction for stock booking
  const t = await GoodsReceipt.sequelize.transaction();
  try {
    // Look up PO items to get unitPrice for unitCost recording
    const poItems = gr.purchaseOrderId ? await PurchaseOrderItem.findAll({
      where: { purchaseOrderId: gr.purchaseOrderId },
      transaction: t,
    }) : [];
    const poItemsByProduct = {};
    for (const pi of poItems) {
      poItemsByProduct[pi.productId] = pi;
    }

    for (const item of gr.GoodsReceiptItems) {
      const qty = Number(item.qtyToBook) || 0;
      if (qty <= 0) continue;

      if (!gr.warehouseId) throw new Error('Warehouse not specified for this ASN');
      const product = await Product.findByPk(item.productId, { transaction: t });
      if (!product) throw new Error('Product not found');
      await validateHeatSensitivePlacement({
        product,
        locationId: item.locationId,
        actionLabel: 'received',
        transaction: t,
      });

      // Record unitCost from PO line item (for FIFO layer tracking)
      const poItem = poItemsByProduct[item.productId];
      const unitCost = item.unitCost || (poItem ? (Number(poItem.unitPrice) || null) : null);

      // Add to ProductStock
      let stock = await ProductStock.findOne({
        where: { 
          productId: item.productId, 
          warehouseId: gr.warehouseId,
          locationId: item.locationId || null
        },
        transaction: t
      });

      if (stock) {
        await stock.update({ quantity: (Number(stock.quantity) || 0) + qty }, { transaction: t });
      } else {
        await ProductStock.create({
          productId: item.productId,
          warehouseId: gr.warehouseId,
          locationId: item.locationId || null,
          quantity: qty,
          reserved: 0,
          status: 'ACTIVE'
        }, { transaction: t });
      }
      
      // Update item received qty and unitCost
      await item.update({ receivedQty: qty, unitCost: unitCost }, { transaction: t });
    }

    await gr.update({ 
      status: 'completed',
      totalReceived: (gr.GoodsReceiptItems || []).reduce((s, i) => s + (Number(i.qtyToBook) || 0), 0)
    }, { transaction: t });

    await t.commit();
    return getById(id, reqUser);
  } catch (err) {
    await t.rollback();
    throw err;
  }
}

async function remove(id, reqUser) {
  const gr = await GoodsReceipt.findByPk(id);
  if (!gr) throw new Error('Goods receipt not found');
  if (reqUser.role !== 'super_admin' && gr.companyId !== reqUser.companyId) throw new Error('Goods receipt not found');
  await GoodsReceiptItem.destroy({ where: { goodsReceiptId: id } });
  await gr.destroy();
  return { deleted: true };
}

module.exports = { list, getById, create, updateReceived, updateAsnItems, finalizeReceiving, remove };
