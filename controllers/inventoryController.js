const inventoryService = require('../services/inventoryService');

async function listProducts(req, res, next) {
  try {
    const data = await inventoryService.listProducts(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function scanBarcode(req, res, next) {
  try {
    const data = await inventoryService.scanBarcode(req.user, req.params.barcode);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Barcode not found' || err.message === 'Invalid barcode') {
      return res.status(404).json({ success: false, message: 'Invalid barcode' });
    }
    next(err);
  }
}

async function getProduct(req, res, next) {
  try {
    const data = await inventoryService.getProductById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Product not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function createProduct(req, res, next) {
  try {
    const data = await inventoryService.createProduct(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message === 'SKU already exists for this company') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function bulkCreateProducts(req, res, next) {
  try {
    const products = Array.isArray(req.body.products) ? req.body.products : req.body;
    const data = await inventoryService.bulkCreateProducts(products, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message === 'No products to import') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function updateProduct(req, res, next) {
  try {
    console.log(`[DEBUG] Update Product Payload ID=${req.params.id}:`, JSON.stringify(req.body, null, 2));
    if (req.body.color) console.log(`[DEBUG] Color field present: "${req.body.color}"`);
    else console.log('[DEBUG] Color field MISSING or EMPTY in payload');
    const data = await inventoryService.updateProduct(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Product not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function addAlternativeSku(req, res, next) {
  try {
    const data = await inventoryService.addAlternativeSku(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Product not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function removeProduct(req, res, next) {
  try {
    await inventoryService.removeProduct(req.params.id, req.user);
    res.json({ success: true, message: 'Product deleted' });
  } catch (err) {
    if (err.message === 'Product not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function listCategories(req, res, next) {
  try {
    const data = await inventoryService.listCategories(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function createCategory(req, res, next) {
  try {
    const data = await inventoryService.createCategory(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message?.includes('companyId') || err.message?.includes('Category code')) return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function updateCategory(req, res, next) {
  try {
    const data = await inventoryService.updateCategory(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Category not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function removeCategory(req, res, next) {
  try {
    await inventoryService.removeCategory(req.params.id, req.user);
    res.json({ success: true, message: 'Category deleted' });
  } catch (err) {
    if (err.message === 'Category not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function listStock(req, res, next) {
  try {
    const data = await inventoryService.listStock(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function listStockByClient(req, res, next) {
  try {
    const data = await inventoryService.listStockByClient(req.user, req.params.clientId, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function createStock(req, res, next) {
  try {
    const data = await inventoryService.createStock(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message === 'Product not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function updateStock(req, res, next) {
  try {
    const data = await inventoryService.updateStock(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Stock not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function removeStock(req, res, next) {
  try {
    await inventoryService.removeStock(req.params.id, req.user);
    res.json({ success: true, message: 'Stock record deleted' });
  } catch (err) {
    if (err.message === 'Stock not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function listStockByBestBeforeDate(req, res, next) {
  try {
    const data = await inventoryService.listStockByBestBeforeDate(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function listStockByLocation(req, res, next) {
  try {
    const data = await inventoryService.listStockByLocation(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function listAdjustments(req, res, next) {
  try {
    const data = await inventoryService.listAdjustments(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function createAdjustment(req, res, next) {
  try {
    const data = await inventoryService.createAdjustment(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (
      err.message === 'Product not found' ||
      err.message === 'Insufficient available stock for decrease' ||
      err.message === 'No warehouse found for company' ||
      err.message?.includes('Heat-sensitive product')
    ) {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

async function listCycleCounts(req, res, next) {
  try {
    const data = await inventoryService.listCycleCounts(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function createCycleCount(req, res, next) {
  try {
    const data = await inventoryService.createCycleCount(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    next(err);
  }
}


async function completeCycleCount(req, res, next) {
  try {
    const data = await inventoryService.completeCycleCount(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function listBatches(req, res, next) {
  try {
    const data = await inventoryService.listBatches(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getBatch(req, res, next) {
  try {
    const data = await inventoryService.getBatchById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Batch not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function createBatch(req, res, next) {
  try {
    const data = await inventoryService.createBatch(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message === 'Product not found') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function updateBatch(req, res, next) {
  try {
    const data = await inventoryService.updateBatch(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Batch not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function removeBatch(req, res, next) {
  try {
    await inventoryService.removeBatch(req.params.id, req.user);
    res.json({ success: true, message: 'Batch deleted' });
  } catch (err) {
    if (err.message === 'Batch not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function listMovements(req, res, next) {
  try {
    const data = await inventoryService.listMovements(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function getMovement(req, res, next) {
  try {
    const data = await inventoryService.getMovementById(req.params.id, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Movement not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function createMovement(req, res, next) {
  try {
    const data = await inventoryService.createMovement(req.body, req.user);
    res.status(201).json({ success: true, data });
  } catch (err) {
    if (err.message === 'Product not found') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function updateMovement(req, res, next) {
  try {
    const data = await inventoryService.updateMovement(req.params.id, req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Movement not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function removeMovement(req, res, next) {
  try {
    await inventoryService.removeMovement(req.params.id, req.user);
    res.json({ success: true, message: 'Movement deleted' });
  } catch (err) {
    if (err.message === 'Movement not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function listInventory(req, res, next) {
  try {
    const data = await inventoryService.listInventory(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function listInventoryLogs(req, res, next) {
  try {
    const data = await inventoryService.listInventoryLogs(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function stockIn(req, res, next) {
  try {
    const data = await inventoryService.stockIn(req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function stockOut(req, res, next) {
  try {
    const data = await inventoryService.stockOut(req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message === 'Insufficient stock') return res.status(400).json({ success: false, message: err.message });
    next(err);
  }
}

async function transfer(req, res, next) {
  try {
    const data = await inventoryService.transfer(req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (err.message.includes('Insufficient stock') || err.message?.includes('Heat-sensitive product')) {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

async function transferStock(req, res, next) {
  try {
    const data = await inventoryService.transferStock(req.body, req.user);
    res.json({ success: true, data });
  } catch (err) {
    if (
      err.message.includes('Insufficient stock') ||
      err.message.includes('required') ||
      err.message.includes('not found') ||
      err.message.includes('Invalid source warehouse') ||
      err.message.includes('Invalid destination warehouse') ||
      err.message.includes('Source and destination must be different')
    ) {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

module.exports = {
  listProducts,
  scanBarcode,
  getProduct,
  createProduct,
  bulkCreateProducts,
  updateProduct,
  addAlternativeSku,
  removeProduct,
  listCategories,
  createCategory,
  updateCategory,
  removeCategory,
  listStock,
  listStockByClient,
  createStock,
  updateStock,
  removeStock,
  listStockByBestBeforeDate,
  listStockByLocation,
  listAdjustments,
  createAdjustment,
  listCycleCounts,
  createCycleCount,
  completeCycleCount,
  listBatches,
  getBatch,
  createBatch,
  updateBatch,
  removeBatch,
  listMovements,
  getMovement,
  createMovement,
  updateMovement,
  removeMovement,
  listInventory,
  listInventoryLogs,
  stockIn,
  stockOut,
  transfer,
  transferStock,
};
