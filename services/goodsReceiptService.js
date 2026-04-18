const { Op } = require('sequelize');
const { GoodsReceipt, GoodsReceiptItem, PurchaseOrder, PurchaseOrderItem, Supplier, Product, ProductStock, Warehouse, Location, Batch } = require('../models');
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
  if (reqUser.clientId && gr.clientId !== reqUser.clientId) throw new Error('Not authorized to access this client data');
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
    companyId: po.companyId,
    purchaseOrderId: po.id,
    clientId: po.clientId || null,
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
  }));
  if (items.length) await GoodsReceiptItem.bulkCreate(items);

  return getById(gr.id, reqUser);
}

async function updateReceived(id, body, reqUser) {
  const t = await GoodsReceipt.sequelize.transaction();
  try {
    const gr = await GoodsReceipt.findByPk(id, { 
      include: ['GoodsReceiptItems'],
      transaction: t,
      lock: t.LOCK.UPDATE 
    });
    if (!gr) throw new Error('Goods receipt not found');
    if (reqUser.role !== 'super_admin' && gr.companyId !== reqUser.companyId) throw new Error('Goods receipt not found');
    if (gr.status === 'completed') throw new Error('Receipt already completed');

    const po = await PurchaseOrder.findByPk(gr.purchaseOrderId, {
      include: ['PurchaseOrderItems'],
      transaction: t,
      lock: t.LOCK.UPDATE
    });

    const items = body.items || [];
    for (const row of items) {
      const line = gr.GoodsReceiptItems?.find((i) => i.productId === row.productId || i.id === row.id);
      if (line) {
        const newReceivedQty = Number(row.receivedQty) || 0;
        
        // Over-receive validation
        const poItem = po.PurchaseOrderItems.find(p => p.productId === line.productId);
        if (poItem) {
          // Check other finalized GRNs
          const otherGrItems = await GoodsReceiptItem.findAll({
            include: [{ association: 'GoodsReceipt', where: { purchaseOrderId: po.id, status: 'completed', id: { [Op.ne]: id } } }],
            where: { productId: line.productId },
            transaction: t
          });
          const alreadyReceived = otherGrItems.reduce((sum, gi) => sum + (Number(gi.receivedQty) || 0), 0);
          if ((alreadyReceived + newReceivedQty) > Number(poItem.quantity)) {
            throw new Error(`Over-receiving detected for SKU ${line.productSku}. Ordered: ${poItem.quantity}, Already finalized: ${alreadyReceived}, Attempting to set this ASN to: ${newReceivedQty}.`);
          }
        }

        await line.update({ 
          receivedQty: newReceivedQty, 
          qualityStatus: row.qualityStatus || line.qualityStatus 
        }, { transaction: t });
      }
    }

    const newTotal = (gr.GoodsReceiptItems || []).reduce((s, i) => s + (Number(i.receivedQty) || 0), 0);
    const allReceived = (gr.GoodsReceiptItems || []).every((i) => (Number(i.receivedQty) || 0) >= (Number(i.expectedQty) || 0));
    
    await gr.update({
      totalReceived: newTotal,
      status: allReceived ? 'completed' : 'in_progress',
    }, { transaction: t });

    await auditLogService.logAction(reqUser, {
      action: 'GRN_RECEIVED_PARTIAL',
      module: 'INBOUND',
      referenceId: gr.id,
      referenceNumber: gr.grNumber,
      details: { totalReceived: newTotal }
    });

    await t.commit();
    return getById(gr.id, reqUser);
  } catch (err) {
    if (t) await t.rollback();
    throw err;
  }
}

async function updateAsnItems(id, body, reqUser) {
  const gr = await GoodsReceipt.findByPk(id, { include: ['GoodsReceiptItems'] });
  if (!gr) throw new Error('ASN not found');
  if (reqUser.role !== 'super_admin' && gr.companyId !== reqUser.companyId) throw new Error('Not authorized');

  if (body.deliveryType) gr.deliveryType = body.deliveryType;
  if (body.eta) gr.eta = body.eta;
  if (body.warehouseId) {
    gr.warehouseId = Number(body.warehouseId);
  }
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
  // 1. Start Transaction
  const t = await GoodsReceipt.sequelize.transaction();
  
  try {
    // 2. Fetch and Lock GRN
    const gr = await GoodsReceipt.findByPk(id, { 
      include: ['GoodsReceiptItems'],
      transaction: t,
      lock: t.LOCK.UPDATE
    });

    if (!gr) throw new Error('ASN not found');
    if (reqUser.role !== 'super_admin' && gr.companyId !== reqUser.companyId) throw new Error('Not authorized');
    if (gr.status === 'completed') throw new Error('Already finalized');
    if (!gr.warehouseId) throw new Error('Warehouse not specified. Please set a destination warehouse.');

    // 3. Fetch and Lock Purchase Order to prevent concurrent receiving edits
    const po = await PurchaseOrder.findByPk(gr.purchaseOrderId, {
      include: [{ association: 'PurchaseOrderItems' }],
      transaction: t,
      lock: t.LOCK.UPDATE
    });
    if (!po) throw new Error('Linked Purchase Order not found');

    // 4. Validate and Process each item
    for (const item of gr.GoodsReceiptItems) {
      const qtyToBook = Number(item.qtyToBook) || 0;
      if (qtyToBook <= 0) continue;

      // Check for over-receiving against PO line
      const poItem = po.PurchaseOrderItems.find(p => p.productId === item.productId);
      if (!poItem) throw new Error(`Product ${item.productSku} not found in original Purchase Order`);

      // Calculate what has been received so far in other finalized GRNs
      const otherGrItems = await GoodsReceiptItem.findAll({
        include: [{ 
          association: 'GoodsReceipt', 
          where: { 
            purchaseOrderId: po.id, 
            status: 'completed',
            id: { [Op.ne]: id }
          } 
        }],
        where: { productId: item.productId },
        transaction: t
      });
      const alreadyReceived = otherGrItems.reduce((sum, gi) => sum + (Number(gi.receivedQty) || 0), 0);
      const remainingAllowed = Number(poItem.quantity) - alreadyReceived;

      if (qtyToBook > remainingAllowed) {
        throw new Error(`Over-receiving detected for ${item.productSku}. Ordered: ${poItem.quantity}, Already Received: ${alreadyReceived}, Attempting: ${qtyToBook}. Maximum allowed now: ${remainingAllowed}`);
      }

      // 5. Heat Sensitivity & Location Validation
      const product = await Product.findByPk(item.productId, { transaction: t });
      const isHeatSensitive = ['yes', 'true', '1'].includes(String(product?.heatSensitive || '').toLowerCase());
      if (isHeatSensitive) {
        if (!item.locationId) throw new Error(`Heat-sensitive product "${product.name}" requires a specific location.`);
        const location = await Location.findByPk(item.locationId, { transaction: t });
        const locHeatSafe = ['yes', 'true', '1'].includes(String(location?.heatSensitive || '').toLowerCase());
        if (!locHeatSafe) throw new Error(`Location ${location?.name} is not suitable for heat-sensitive product "${product.name}"`);
      }

      // 6. Manage Batch (if applicable)
      if (item.batchId) {
        // Prevent duplicate batch for same product in same GRN
        const existingBatch = await Batch.findOne({
          where: { productId: item.productId, batchNumber: item.batchId, grnId: id },
          transaction: t
        });
        if (existingBatch) throw new Error(`Duplicate batch ${item.batchId} for product ${item.productSku} in this GRN`);

        await Batch.create({
          companyId: gr.companyId,
          clientId: gr.clientId || null,
          productId: item.productId,
          warehouseId: gr.warehouseId,
          locationId: item.locationId || null,
          batchNumber: item.batchId,
          quantity: qtyToBook,
          expiryDate: item.bestBeforeDate || null,
          grnId: id,
          status: 'ACTIVE'
        }, { transaction: t });
      }

      // 7. Update Inventory (with row locking check)
      let stock = await ProductStock.findOne({
        where: { 
          productId: item.productId, 
          warehouseId: gr.warehouseId,
          companyId: gr.companyId,
          locationId: item.locationId || null
        },
        transaction: t,
        lock: t.LOCK.UPDATE
      });

      if (stock) {
        await stock.update({ quantity: (Number(stock.quantity) || 0) + qtyToBook }, { transaction: t });
      } else {
        await ProductStock.create({
          companyId: gr.companyId,
          clientId: gr.clientId || null,
          productId: item.productId,
          warehouseId: gr.warehouseId,
          locationId: item.locationId || null,
          quantity: qtyToBook,
          reserved: 0,
          status: 'ACTIVE'
        }, { transaction: t });
      }

      // Update item record
      await item.update({ receivedQty: qtyToBook }, { transaction: t });
    }

    // 8. Finalize GRN status
    const totalReceivedNow = (gr.GoodsReceiptItems || []).reduce((s, i) => s + (Number(i.qtyToBook) || 0), 0);
    await gr.update({ 
      status: 'completed',
      totalReceived: totalReceivedNow
    }, { transaction: t });

    // 9. Auto-Check PO completion
    const allGrItemsForPo = await GoodsReceiptItem.findAll({
      include: [{ association: 'GoodsReceipt', where: { purchaseOrderId: po.id, status: 'completed' } }],
      transaction: t
    });
    const poTotals = {};
    allGrItemsForPo.forEach(gi => {
      poTotals[gi.productId] = (poTotals[gi.productId] || 0) + (Number(gi.receivedQty) || 0);
    });
    const isPoFullyReceived = po.PurchaseOrderItems.every(poi => (poTotals[poi.productId] || 0) >= Number(poi.quantity));
    if (isPoFullyReceived) {
      await po.update({ status: 'received' }, { transaction: t });
    }

    // 10. Audit Log
    await auditLogService.logAction(reqUser, {
      action: 'GRN_FINALIZED',
      module: 'INBOUND',
      referenceId: gr.id,
      referenceNumber: gr.grNumber,
      details: { totalItems: gr.GoodsReceiptItems.length, totalReceived: totalReceivedNow }
    });

    await t.commit();
    return getById(id, reqUser);

  } catch (err) {
    if (t) await t.rollback();
    console.error('Finalize Failed:', err);
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
