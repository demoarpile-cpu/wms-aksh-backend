const { Product, Category, ProductStock, Warehouse, Company, Supplier, InventoryAdjustment, CycleCount, Batch, Movement, Customer, InventoryLog, Inventory, Location } = require('../models');
const { Op } = require('sequelize');

/** Ensure product JSON fields from API are proper objects/arrays (e.g. SQLite may return strings) */
function normalizeProductJson(p) {
  if (!p) return p;
  const out = typeof p.get === 'function' ? p.get({ plain: true }) : { ...p };
  if (typeof out.cartons === 'string') {
    try { out.cartons = JSON.parse(out.cartons); } catch (_) { out.cartons = null; }
  }
  if (out.cartons != null && !Array.isArray(out.cartons)) out.cartons = null;
  if (typeof out.supplierProducts === 'string') {
    try { out.supplierProducts = JSON.parse(out.supplierProducts); } catch (_) { out.supplierProducts = null; }
  }
  if (out.supplierProducts != null && !Array.isArray(out.supplierProducts)) out.supplierProducts = null;
  if (typeof out.marketplaceSkus === 'string') {
    try { out.marketplaceSkus = JSON.parse(out.marketplaceSkus); } catch (_) { out.marketplaceSkus = {}; }
  }
  if (out.marketplaceSkus == null || typeof out.marketplaceSkus !== 'object') out.marketplaceSkus = {};
  if (Array.isArray(out.marketplaceSkus)) out.marketplaceSkus = {};
  // Coerce dimension/weight to number so frontend always gets consistent types
  if (out.length != null && out.length !== '') out.length = Number(out.length);
  if (out.width != null && out.width !== '') out.width = Number(out.width);
  if (out.height != null && out.height !== '') out.height = Number(out.height);
  if (out.weight != null && out.weight !== '') out.weight = Number(out.weight);
  if (typeof out.images === 'string') {
    try { out.images = JSON.parse(out.images); } catch (_) { out.images = null; }
  }
  if (typeof out.priceLists === 'string') {
    try { out.priceLists = JSON.parse(out.priceLists); } catch (_) { out.priceLists = null; }
  }
  if (typeof out.alternativeSkus === 'string') {
    try { out.alternativeSkus = JSON.parse(out.alternativeSkus); } catch (_) { out.alternativeSkus = null; }
  }
  if (out.alternativeSkus != null && !Array.isArray(out.alternativeSkus)) out.alternativeSkus = null;
  return out;
}

function isTruthyYes(value) {
  const normalized = String(value == null ? '' : value).trim().toLowerCase();
  return normalized === 'yes' || normalized === 'true' || normalized === '1';
}

async function validateHeatSensitivePlacement({ product, locationId, actionLabel }) {
  if (!locationId || !isTruthyYes(product?.heatSensitive)) return;
  const location = await Location.findByPk(locationId);
  if (!location) throw new Error('Location not found');
  if (!isTruthyYes(location.heatSensitive)) {
    throw new Error(`Heat-sensitive product can only be ${actionLabel} to heat-sensitive locations`);
  }
}

async function listProducts(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  else if (query.companyId) where.companyId = query.companyId;
  if (query.categoryId) where.categoryId = query.categoryId;
  if (query.status) where.status = query.status;
  if (query.search) {
    where[Op.or] = [
      { name: { [Op.like]: `%${query.search}%` } },
      { sku: { [Op.like]: `%${query.search}%` } },
      { barcode: { [Op.like]: `%${query.search}%` } },
      { color: { [Op.like]: `%${query.search}%` } },
    ];
  }
  const products = await Product.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      { association: 'Category', attributes: ['id', 'name', 'code'], required: false },
      { association: 'Company', attributes: ['id', 'name', 'code'], required: false },
      { association: 'ProductStocks', attributes: ['quantity'], required: false },
    ],
  });
  return products;
}

async function listCategories(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  else if (query.companyId) where.companyId = query.companyId;
  const categories = await Category.findAll({
    where,
    order: [['name']],
    include: [{ association: 'Products', attributes: ['id'], required: false }],
  });
  return categories.map(c => {
    const j = c.toJSON();
    j.productCount = (j.Products && j.Products.length) || 0;
    delete j.Products;
    return j;
  });
}

async function getProductById(id, reqUser) {
  const product = await Product.findByPk(id, {
    include: [
      { association: 'Category' },
      { association: 'Company', attributes: ['id', 'name', 'code'] },
      { association: 'Supplier', attributes: ['id', 'name', 'code'] },
      { association: 'ProductStocks', include: [{ association: 'Warehouse' }, { association: 'Location' }] },
    ],
  });
  if (!product) throw new Error('Product not found');
  if (reqUser.role !== 'super_admin' && product.companyId !== reqUser.companyId) throw new Error('Product not found');
  return normalizeProductJson(product);
}

async function createProduct(data, reqUser) {
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'company_admin' && reqUser.role !== 'inventory_manager') {
    throw new Error('Not allowed to create product');
  }
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('companyId required');
  const existing = await Product.findOne({ where: { companyId, sku: data.sku.trim() } });
  if (existing) throw new Error('SKU already exists for this company');
  const payload = {
    companyId,
    categoryId: data.categoryId || null,
    supplierId: data.supplierId || null,
    name: data.name,
    sku: data.sku.trim(),
    barcode: data.barcode || null,
    description: data.description || null,
    color: data.color || null,
    productType: data.productType || null,
    unitOfMeasure: data.unitOfMeasure || null,
    price: data.price ?? 0,
    costPrice: data.costPrice != null ? data.costPrice : null,
    vatRate: data.vatRate != null ? data.vatRate : null,
    vatCode: data.vatCode || null,
    customsTariff: data.customsTariff != null ? String(data.customsTariff) : null,
    marketplaceSkus: data.marketplaceSkus && typeof data.marketplaceSkus === 'object' ? data.marketplaceSkus : null,
    heatSensitive: data.heatSensitive || null,
    perishable: data.perishable || null,
    requireBatchTracking: data.requireBatchTracking || null,
    shelfLifeDays: data.shelfLifeDays != null ? data.shelfLifeDays : null,
    length: data.length != null ? data.length : null,
    width: data.width != null ? data.width : null,
    height: data.height != null ? data.height : null,
    dimensionUnit: data.dimensionUnit || null,
    weight: data.weight != null ? data.weight : null,
    weightUnit: data.weightUnit || null,
    reorderLevel: data.reorderLevel ?? 0,
    reorderQty: data.reorderQty != null ? data.reorderQty : null,
    maxStock: data.maxStock != null ? data.maxStock : null,
    status: data.status || 'ACTIVE',
    images: Array.isArray(data.images) ? data.images : null,
    cartons: Array.isArray(data.cartons) ? data.cartons : (data.cartons && typeof data.cartons === 'object' ? data.cartons : null),
    priceLists: data.priceLists && typeof data.priceLists === 'object' ? data.priceLists : null,
    supplierProducts: Array.isArray(data.supplierProducts) ? data.supplierProducts : null,
    alternativeSkus: Array.isArray(data.alternativeSkus) ? data.alternativeSkus : null,
    packSize: data.packSize != null ? Number(data.packSize) : 1,
    bestBeforeDateWarningPeriodDays: data.bestBeforeDateWarningPeriodDays != null ? Number(data.bestBeforeDateWarningPeriodDays) : 0,
  };
  console.log('[DEBUG_SERVICE] Creating Product Payload:', JSON.stringify(payload, null, 2));
  const created = await Product.create(payload);
  return normalizeProductJson(created);
}

async function bulkCreateProducts(productsArray, reqUser) {
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'company_admin' && reqUser.role !== 'inventory_manager') {
    throw new Error('Not allowed to import products');
  }
  const companyId = reqUser.companyId;
  if (!companyId) throw new Error('Company required');
  if (!Array.isArray(productsArray) || productsArray.length === 0) {
    throw new Error('No products to import');
  }
  const results = { created: 0, skipped: 0, errors: [] };
  
  // Cache categories for this company to reduce DB calls and handle case-insensitive matching
  const existingCategories = await Category.findAll({ where: { companyId } });
  const categoryMap = new Map(); // lowercase name -> ID
  const categoryIdSet = new Set();
  existingCategories.forEach(c => {
    categoryMap.set(c.name.toLowerCase().trim(), c.id);
    categoryIdSet.add(c.id);
  });
  console.log(`[BULK_IMPORT] CompanyId=${companyId} Found ${existingCategories.length} existing categories.`);

  // Cache suppliers for this company
  const existingSuppliers = await Supplier.findAll({ where: { companyId } });
  const supplierMap = new Map(); // lowercase name -> ID
  const supplierIdSet = new Set();
  existingSuppliers.forEach(s => {
    supplierMap.set(s.name.toLowerCase().trim(), s.id);
    supplierIdSet.add(s.id);
  });
  console.log(`[BULK_IMPORT] CompanyId=${companyId} Found ${existingSuppliers.length} existing suppliers.`);



  for (let i = 0; i < productsArray.length; i++) {
    const data = productsArray[i];
    try {
      if (!data || !data.sku || !data.name) {
        results.skipped++;
        results.errors.push({ row: i + 1, message: 'SKU and Product Name required' });
        continue;
      }
      const existing = await Product.findOne({ where: { companyId, sku: String(data.sku).trim() } });
      if (existing) {
        results.skipped++;
        results.errors.push({ row: i + 1, sku: data.sku, message: 'SKU already exists' });
        continue;
      }

      // Resolve categoryId (ID or Name)
      let resolvedCategoryId = null;
      const catInput = data.categoryId != null ? String(data.categoryId).trim() : null;
      if (catInput !== null && catInput !== '') {
        // If it's a numeric ID that already exists
        if (!isNaN(catInput) && categoryIdSet.has(Number(catInput))) {
          resolvedCategoryId = Number(catInput);
          console.log(`[BULK_IMPORT] Row ${i+1}: Matched numeric ID ${resolvedCategoryId}`);
        } else {
          // Treat as Name
          const lowerName = catInput.toLowerCase();
          if (categoryMap.has(lowerName)) {
            resolvedCategoryId = categoryMap.get(lowerName);
            console.log(`[BULK_IMPORT] Row ${i+1}: Matched category name "${catInput}" to ID ${resolvedCategoryId}`);
          } else {
            console.log(`[BULK_IMPORT] Row ${i+1}: Category "${catInput}" NOT found. Creating...`);
            // Auto-create category
            const newCat = await Category.create({
              companyId,
              name: catInput,
              code: catInput.replace(/\s/g, '_').toUpperCase().slice(0, 50)
            });
            resolvedCategoryId = newCat.id;
            categoryMap.set(lowerName, resolvedCategoryId);
            categoryIdSet.add(resolvedCategoryId);
            console.log(`[BULK_IMPORT] Row ${i+1}: Created new category "${catInput}" with ID ${resolvedCategoryId}`);
          }
        }
      }


      // Resolve supplierId (ID or Name)
      let resolvedSupplierId = null;
      const supInput = data.supplierId != null ? String(data.supplierId).trim() : null;
      if (supInput !== null && supInput !== '') {
        if (!isNaN(supInput) && supplierIdSet.has(Number(supInput))) {
          resolvedSupplierId = Number(supInput);
        } else {
          const lowerSup = supInput.toLowerCase();
          if (supplierMap.has(lowerSup)) {
            resolvedSupplierId = supplierMap.get(lowerSup);
          } else {
            console.log(`[BULK_IMPORT] Row ${i+1}: Supplier "${supInput}" NOT found. Creating...`);
            const newSup = await Supplier.create({
              companyId,
              name: supInput,
              code: supInput.replace(/\s/g, '_').toUpperCase().slice(0, 50)
            });
            resolvedSupplierId = newSup.id;
            supplierMap.set(lowerSup, resolvedSupplierId);
            supplierIdSet.add(resolvedSupplierId);
          }
        }
      }

      await Product.create({
        companyId,
        categoryId: resolvedCategoryId,
        supplierId: resolvedSupplierId,
        name: String(data.name).trim(),

        sku: String(data.sku).trim(),
        barcode: data.barcode ? String(data.barcode).trim() : null,
        description: data.description ? String(data.description).trim() : null,
        color: data.color ? String(data.color).trim() : null,
        productType: data.productType || null,
        unitOfMeasure: data.unitOfMeasure || null,
        price: data.price != null ? Number(data.price) : 0,
        costPrice: data.costPrice != null ? Number(data.costPrice) : null,
        vatRate: data.vatRate != null ? Number(data.vatRate) : null,
        vatCode: data.vatCode || null,
        customsTariff: data.customsTariff != null ? String(data.customsTariff) : null,
        marketplaceSkus: data.marketplaceSkus && typeof data.marketplaceSkus === 'object' ? data.marketplaceSkus : null,
        heatSensitive: data.heatSensitive || null,
        perishable: data.perishable || null,
        requireBatchTracking: data.requireBatchTracking || null,
        shelfLifeDays: data.shelfLifeDays != null ? Number(data.shelfLifeDays) : null,
        length: data.length != null ? Number(data.length) : null,
        width: data.width != null ? Number(data.width) : null,
        height: data.height != null ? Number(data.height) : null,
        dimensionUnit: data.dimensionUnit || null,
        weight: data.weight != null ? Number(data.weight) : null,
        weightUnit: data.weightUnit || null,
        reorderLevel: data.reorderLevel != null ? Number(data.reorderLevel) : 0,
        reorderQty: data.reorderQty != null ? Number(data.reorderQty) : null,
        maxStock: data.maxStock != null ? Number(data.maxStock) : null,
        status: data.status && String(data.status).toUpperCase() === 'INACTIVE' ? 'INACTIVE' : 'ACTIVE',
        images: Array.isArray(data.images) ? data.images : null,
        cartons: Array.isArray(data.cartons) && data.cartons.length > 0 ? data.cartons : null,
        priceLists: data.priceLists && typeof data.priceLists === 'object' ? data.priceLists : null,
        supplierProducts: Array.isArray(data.supplierProducts) ? data.supplierProducts : null,
        alternativeSkus: Array.isArray(data.alternativeSkus) ? data.alternativeSkus : null,
        packSize: data.packSize != null ? Number(data.packSize) : 1,
        bestBeforeDateWarningPeriodDays: data.bestBeforeDateWarningPeriodDays != null ? Number(data.bestBeforeDateWarningPeriodDays) : 0,
      });
      results.created++;
    } catch (err) {
      results.skipped++;
      results.errors.push({ row: i + 1, sku: data?.sku, message: err.message || 'Failed' });
    }
  }
  return results;

}

async function updateProduct(id, data, reqUser) {
  const product = await Product.findByPk(id);
  if (!product) throw new Error('Product not found');
  if (reqUser.role !== 'super_admin' && product.companyId !== reqUser.companyId) throw new Error('Product not found');
  // Only update fields that are present in data (partial update) – baki data null nahi hoga
  const upd = {};
  if (data.name !== undefined) upd.name = data.name ?? product.name;
  if (data.categoryId !== undefined) upd.categoryId = data.categoryId;
  if (data.supplierId !== undefined) upd.supplierId = data.supplierId;
  if (data.sku !== undefined) upd.sku = data.sku?.trim() ?? product.sku;
  if (data.barcode !== undefined) upd.barcode = data.barcode;
  if (data.description !== undefined) upd.description = data.description;
  if (data.color !== undefined) {
    console.log(`[DEBUG_SERVICE] Updating color to: "${data.color}"`);
    upd.color = data.color;
  }
  if (data.productType !== undefined) upd.productType = data.productType;
  if (data.unitOfMeasure !== undefined) upd.unitOfMeasure = data.unitOfMeasure;
  if (data.price !== undefined) upd.price = data.price;
  if (data.costPrice !== undefined) upd.costPrice = data.costPrice;
  if (data.vatRate !== undefined) upd.vatRate = data.vatRate;
  if (data.vatCode !== undefined) upd.vatCode = data.vatCode;
  if (data.customsTariff !== undefined) upd.customsTariff = data.customsTariff != null ? String(data.customsTariff) : null;
  if (data.marketplaceSkus !== undefined) upd.marketplaceSkus = data.marketplaceSkus && typeof data.marketplaceSkus === 'object' ? data.marketplaceSkus : product.marketplaceSkus;
  if (data.heatSensitive !== undefined) upd.heatSensitive = data.heatSensitive;
  if (data.perishable !== undefined) upd.perishable = data.perishable;
  if (data.requireBatchTracking !== undefined) upd.requireBatchTracking = data.requireBatchTracking;
  if (data.shelfLifeDays !== undefined) upd.shelfLifeDays = data.shelfLifeDays;
  if (data.length !== undefined) upd.length = data.length;
  if (data.width !== undefined) upd.width = data.width;
  if (data.height !== undefined) upd.height = data.height;
  if (data.dimensionUnit !== undefined) upd.dimensionUnit = data.dimensionUnit;
  if (data.weight !== undefined) upd.weight = data.weight;
  if (data.weightUnit !== undefined) upd.weightUnit = data.weightUnit;
  if (data.reorderLevel !== undefined) upd.reorderLevel = data.reorderLevel;
  if (data.reorderQty !== undefined) upd.reorderQty = data.reorderQty;
  if (data.maxStock !== undefined) upd.maxStock = data.maxStock;
  if (data.status !== undefined) upd.status = data.status ?? product.status;
  if (data.images !== undefined) upd.images = Array.isArray(data.images) ? data.images : product.images;
  if (data.cartons !== undefined) upd.cartons = Array.isArray(data.cartons) ? data.cartons : (data.cartons && typeof data.cartons === 'object' ? data.cartons : product.cartons);
  if (data.priceLists !== undefined) upd.priceLists = data.priceLists && typeof data.priceLists === 'object' ? data.priceLists : product.priceLists;
  if (data.supplierProducts !== undefined) upd.supplierProducts = Array.isArray(data.supplierProducts) ? data.supplierProducts : product.supplierProducts;
  if (data.alternativeSkus !== undefined) upd.alternativeSkus = Array.isArray(data.alternativeSkus) ? data.alternativeSkus : product.alternativeSkus;
  if (data.packSize !== undefined) upd.packSize = data.packSize != null ? Number(data.packSize) : product.packSize;
  if (data.bestBeforeDateWarningPeriodDays !== undefined) upd.bestBeforeDateWarningPeriodDays = data.bestBeforeDateWarningPeriodDays != null ? Number(data.bestBeforeDateWarningPeriodDays) : product.bestBeforeDateWarningPeriodDays;
  if (Object.keys(upd).length === 0) return normalizeProductJson(product);
  console.log('[DEBUG_SERVICE] Final Update Object:', JSON.stringify(upd, null, 2));
  await product.update(upd);
  const updated = await Product.findByPk(id, {
    include: [
      { association: 'Category' },
      { association: 'Company', attributes: ['id', 'name', 'code'] },
      { association: 'Supplier', attributes: ['id', 'name', 'code'] },
      { association: 'ProductStocks', include: [{ association: 'Warehouse' }, { association: 'Location' }] },
    ],
  });
  return normalizeProductJson(updated || product);
}

async function addAlternativeSku(productId, payload, reqUser) {
  const product = await Product.findByPk(productId, {
    include: [
      { association: 'Category' },
      { association: 'Company', attributes: ['id', 'name', 'code'] },
      { association: 'Supplier', attributes: ['id', 'name', 'code'] },
      { association: 'ProductStocks', include: [{ association: 'Warehouse' }, { association: 'Location' }] },
    ],
  });
  if (!product) throw new Error('Product not found');
  if (reqUser.role !== 'super_admin' && product.companyId !== reqUser.companyId) throw new Error('Product not found');
  const list = Array.isArray(product.alternativeSkus) ? [...product.alternativeSkus] : [];
  const newItem = {
    id: payload.id || `alt-${Date.now()}`,
    channelType: payload.channelType || null,
    sku: payload.sku?.trim() || null,
    skuType: payload.skuType || null,
    isPrimary: !!payload.isPrimary,
    active: payload.active !== false,
    notes: payload.notes?.trim() || null,
    leadTimeDays: payload.leadTimeDays != null ? payload.leadTimeDays : null,
    moq: payload.moq != null ? payload.moq : null,
  };
  list.push(newItem);
  await product.update({ alternativeSkus: list });
  const updated = await Product.findByPk(productId, {
    include: [
      { association: 'Category' },
      { association: 'Company', attributes: ['id', 'name', 'code'] },
      { association: 'Supplier', attributes: ['id', 'name', 'code'] },
      { association: 'ProductStocks', include: [{ association: 'Warehouse' }, { association: 'Location' }] },
    ],
  });
  return normalizeProductJson(updated || product);
}

async function removeProduct(id, reqUser) {
  const product = await Product.findByPk(id);
  if (!product) throw new Error('Product not found');
  if (reqUser.role !== 'super_admin' && product.companyId !== reqUser.companyId) throw new Error('Product not found');
  await ProductStock.destroy({ where: { productId: id } });
  await product.destroy();
  return { message: 'Product deleted' };
}

async function createCategory(data, reqUser) {
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('companyId required');
  const code = data.code?.trim() || data.name.replace(/\s/g, '_').toUpperCase().slice(0, 50);
  const existing = await Category.findOne({ where: { companyId, code } });
  if (existing) throw new Error('Category code already exists for this company');
  return Category.create({
    companyId,
    name: data.name,
    code,
  });
}

async function updateCategory(id, data, reqUser) {
  const cat = await Category.findByPk(id);
  if (!cat) throw new Error('Category not found');
  if (reqUser.role !== 'super_admin' && cat.companyId !== reqUser.companyId) throw new Error('Category not found');
  await cat.update({
    name: data.name ?? cat.name,
    code: data.code?.trim() ?? cat.code,
  });
  return cat;
}

async function removeCategory(id, reqUser) {
  const cat = await Category.findByPk(id);
  if (!cat) throw new Error('Category not found');
  if (reqUser.role !== 'super_admin' && cat.companyId !== reqUser.companyId) throw new Error('Category not found');
  await cat.destroy();
  return { message: 'Category deleted' };
}

async function listStock(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin' && reqUser.companyId) {
    where.companyId = reqUser.companyId;
  }
  // Enforce client-level visibility
  if (reqUser.clientId) {
    where.clientId = reqUser.clientId;
  } else if (query.clientId) {
    where.clientId = query.clientId;
  }
  
  if (query.warehouseId) where.warehouseId = query.warehouseId;
  if (query.productId) where.productId = query.productId;
  if (query.locationId) where.locationId = query.locationId;
  if (query.batchNumber) where.batchNumber = query.batchNumber;
  const stocks = await ProductStock.findAll({
    where,
    include: [
      { association: 'Product', where: reqUser.role !== 'super_admin' ? { companyId: reqUser.companyId } : undefined, required: reqUser.role !== 'super_admin' },
      { association: 'Warehouse', include: ['Company'] },
      { association: 'Location', required: false },
    ],
  });
  return stocks;
}

async function createStock(data, reqUser) {
  const { Product } = require('../models');
  const product = await Product.findByPk(data.productId);
  if (!product) throw new Error('Product not found');
  if (reqUser.role !== 'super_admin' && product.companyId !== reqUser.companyId) throw new Error('Product not found');

  if (data.warehouseId && data.quantity > 0) {
    const warehouseService = require('./warehouseService');
    await warehouseService.validateCapacity(data.warehouseId, data.quantity);
  }

  const stock = await ProductStock.create({
    companyId: reqUser.companyId,
    clientId: reqUser.clientId || data.clientId || null,
    productId: data.productId,
    warehouseId: data.warehouseId,
    locationId: data.locationId || null,
    quantity: data.quantity ?? 0,
    reserved: data.reserved ?? 0,
    status: data.status || 'ACTIVE',
    lotNumber: data.lotNumber || null,
    batchNumber: data.batchNumber || null,
    serialNumber: data.serialNumber || null,
    bestBeforeDate: data.bestBeforeDate || null,
  });
  return ProductStock.findByPk(stock.id, {
    include: [
      { association: 'Product' },
      { association: 'Warehouse' },
      { association: 'Location', required: false },
    ],
  });
}

async function updateStock(stockId, data, reqUser) {
  const stock = await ProductStock.findByPk(stockId, { include: ['Product'] });
  if (!stock) throw new Error('Stock not found');
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'inventory_manager' && reqUser.role !== 'company_admin') {
    throw new Error('Not allowed');
  }
  if (stock.Product.companyId !== reqUser.companyId && reqUser.role !== 'super_admin') throw new Error('Stock not found');

  if (data.quantity !== undefined && data.quantity > stock.quantity) {
    const warehouseService = require('./warehouseService');
    await warehouseService.validateCapacity(stock.warehouseId, data.quantity - stock.quantity);
  }

  await stock.update({
    quantity: data.quantity !== undefined ? data.quantity : stock.quantity,
    reserved: data.reserved !== undefined ? data.reserved : stock.reserved,
    locationId: data.locationId !== undefined ? data.locationId : stock.locationId,
    status: data.status !== undefined ? data.status : stock.status,
    lotNumber: data.lotNumber !== undefined ? data.lotNumber : stock.lotNumber,
    batchNumber: data.batchNumber !== undefined ? data.batchNumber : stock.batchNumber,
    serialNumber: data.serialNumber !== undefined ? data.serialNumber : stock.serialNumber,
    bestBeforeDate: data.bestBeforeDate !== undefined ? data.bestBeforeDate : stock.bestBeforeDate,
  });
  return stock;
}

async function removeStock(stockId, reqUser) {
  const stock = await ProductStock.findByPk(stockId, { include: ['Product'] });
  if (!stock) throw new Error('Stock not found');
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'inventory_manager' && reqUser.role !== 'company_admin') throw new Error('Not allowed');
  if (stock.Product && stock.Product.companyId !== reqUser.companyId && reqUser.role !== 'super_admin') throw new Error('Stock not found');
  await stock.destroy();
  return { message: 'Stock record deleted' };
}

async function listStockByBestBeforeDate(reqUser, query = {}) {
  const where = {};
  if (query.productId) where.productId = query.productId;
  if (query.warehouseId) where.warehouseId = query.warehouseId;
  const hasDateFilter = query.minBbd || query.maxBbd;
  if (hasDateFilter) {
    const bbdCond = [{ [Op.ne]: null }];
    if (query.minBbd) bbdCond.push({ [Op.gte]: query.minBbd });
    if (query.maxBbd) bbdCond.push({ [Op.lte]: query.maxBbd });
    where.bestBeforeDate = { [Op.and]: bbdCond };
  }
  // when no date filter: return all stock (including bestBeforeDate = null) so report shows data
  const productWhere = (reqUser.role !== 'super_admin' && reqUser.companyId) ? { companyId: reqUser.companyId } : undefined;
  const stocks = await ProductStock.findAll({
    where,
    include: [
      { association: 'Product', where: productWhere, required: !!productWhere, attributes: ['id', 'name', 'sku'] },
      { association: 'Warehouse', attributes: ['id', 'name'] },
      { association: 'Location', attributes: ['id', 'name', 'code'], required: false },
    ],
  });
  const byKey = {};
  for (const s of stocks) {
    const locName = s.Location?.code || s.Location?.name || 'Unassigned';
    const key = `${s.productId}-${s.bestBeforeDate || 'no-date'}-${s.batchNumber || 'no-batch'}-${s.warehouseId || '0'}-${s.locationId || '0'}`;
    if (!byKey[key]) {
      byKey[key] = {
        productId: s.productId,
        productName: s.Product?.name,
        productSku: s.Product?.sku,
        bestBeforeDate: s.bestBeforeDate,
        batchNumber: s.batchNumber,
        warehouseName: s.Warehouse?.name || '—',
        locationName: locName,
        totalAvailable: 0,
        bbdCount: 0,
      };
    }
    byKey[key].totalAvailable += Math.max(0, (s.quantity || 0) - (s.reserved || 0));
    byKey[key].bbdCount += 1;
  }
  return Object.values(byKey).sort((a, b) => {
    if (!a.bestBeforeDate) return 1;
    if (!b.bestBeforeDate) return -1;
    return a.bestBeforeDate.localeCompare(b.bestBeforeDate);
  });
}

async function listStockByLocation(reqUser, query = {}) {
  const { Location, Zone } = require('../models');
  const where = {};
  if (query.warehouseId) where.warehouseId = query.warehouseId;
  const productWhere = (reqUser.role !== 'super_admin' && reqUser.companyId) ? { companyId: reqUser.companyId } : undefined;
  const include = [
    { association: 'Product', where: productWhere, required: !!productWhere, attributes: ['id', 'name', 'sku'] },
    { association: 'Warehouse', attributes: ['id', 'name'] },
    { association: 'Location', required: false, include: [{ association: 'Zone', attributes: ['id', 'name', 'code'] }] },
  ];
  const stocks = await ProductStock.findAll({ where, include });
  const byLoc = {};
  for (const s of stocks) {
    const locId = s.locationId || 0;
    const loc = s.Location;
    if (query.locationType && loc && loc.locationType !== query.locationType) continue;
    if (!byLoc[locId]) {
      byLoc[locId] = {
        locationId: locId || null,
        locationName: loc?.name || 'Unassigned',
        locationCode: loc?.code || loc?.name || '',
        locationType: loc?.locationType || '—',
        zoneName: loc?.Zone?.name || loc?.Zone?.code || '—',
        properties: loc?.heatSensitive === 'yes' ? 'Hot Location' : (loc?.heatSensitive ? String(loc.heatSensitive) : '—'),
        pickSequence: loc?.pickSequence ?? null,
        totalItems: 0,
        productIds: new Set(),
        warnings: [],
      };
    }
    byLoc[locId].totalItems += (s.quantity || 0);
    byLoc[locId].productIds.add(s.productId);
    if (s.bestBeforeDate && new Date(s.bestBeforeDate) <= new Date(Date.now() + 30 * 24 * 60 * 60 * 1000)) {
      if (!byLoc[locId].warnings.includes('Expiring soon')) byLoc[locId].warnings.push('Expiring soon');
    }
  }
  return Object.values(byLoc)
    .map((r) => ({ ...r, productCount: r.productIds.size, productIds: undefined, warnings: r.warnings.length ? r.warnings.join('; ') : '—' }))
    .sort((a, b) => (a.pickSequence != null && b.pickSequence != null ? a.pickSequence - b.pickSequence : (a.locationCode || '').localeCompare(b.locationCode || '')));
}

function generateAdjustmentReference() {
  return 'ADJ-' + Buffer.from(Date.now().toString(36) + Math.random().toString(36).slice(2)).toString('base64').replace(/[/+=]/g, '').slice(0, 8).toUpperCase();
}

async function listAdjustments(reqUser, query = {}) {
  const where = {};
  const role = (reqUser.role || '').toString().toLowerCase().replace(/-/g, '_');
  if (role !== 'super_admin' && reqUser.companyId) where.companyId = reqUser.companyId;
  if (query.type) where.type = query.type;
  if (query.status) where.status = query.status;
  if (query.search) {
    where[Op.or] = [
      { referenceNumber: { [Op.like]: `%${query.search}%` } },
      { reason: { [Op.like]: `%${query.search}%` } },
    ];
  }
  const list = await InventoryAdjustment.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      { association: 'Product', attributes: ['id', 'name', 'sku'] },
      { association: 'Warehouse', required: false, attributes: ['id', 'name'] },
      { association: 'createdByUser', required: false, attributes: ['id', 'name', 'email'] },
      { association: 'Location', required: false, attributes: ['id', 'name', 'code'] },
      { association: 'Client', required: false, attributes: ['id', 'name'] },
    ],
  });
  return list.map((a) => {
    const j = a.toJSON();
    j.items = [{ product: j.Product, quantity: j.quantity }];
    j.createdBy = j.createdByUser;
    // Keep Product and User for frontend history rendering
    return j;
  });
}


async function createAdjustment(data, reqUser) {
  const role = (reqUser.role || '').toString().toLowerCase().replace(/-/g, '_');
  const allowedRoles = ['super_admin', 'company_admin', 'inventory_manager', 'warehouse_manager', 'picker', 'packer'];
  if (!allowedRoles.includes(role)) {
    throw new Error('Not allowed to create adjustment');
  }
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId && role !== 'super_admin') throw new Error('Company context required');
  const effectiveCompanyId = companyId || (await Product.findByPk(data.productId).then(p => p?.companyId));
  if (!effectiveCompanyId) throw new Error('Company context required');
  const product = await Product.findByPk(data.productId);
  if (!product) throw new Error('Product not found');
  if (effectiveCompanyId && product.companyId !== effectiveCompanyId && role !== 'super_admin') throw new Error('Product not found');
  if (!String(data.batchNumber || '').trim()) {
    throw new Error(`${product.name || 'This product'} requires a Batch Number for accurate tracking`);
  }
  if (!data.bestBeforeDate) {
    throw new Error(`${product.name || 'This product'} requires a Best Before (Expiry) Date`);
  }

  // Auto-detect type from quantity sign if not explicitly provided
  const rawQty = parseInt(data.quantity, 10) || 0;
  const type = data.type?.toUpperCase() === 'INCREASE' || (rawQty >= 0 && data.type?.toUpperCase() !== 'DECREASE') ? 'INCREASE' : 'DECREASE';
  const qty = Math.abs(rawQty);
  if (qty < 1) throw new Error('Quantity must be at least 1');

  let warehouseId = data.warehouseId || null;
  let locationId = data.locationId || null;
  let batchId = data.batchId || null;
  let batchNumber = data.batchNumber || null;
  let bestBeforeDate = data.bestBeforeDate || null;
  let clientId = data.clientId || null;

  // New requirement: Stock updates per SKU + Warehouse + Location + Batch
  // If batchId is provided, resolve batchNumber
  if (batchId && !batchNumber) {
    const b = await Batch.findByPk(batchId);
    if (b) batchNumber = b.batchNumber;
  }

  if (!warehouseId) throw new Error('Warehouse is mandatory for inventory booking');
  if (!locationId) throw new Error('Location is mandatory to ensure stock tracking per bin');
  if (!clientId) throw new Error('Client is mandatory for stock movement mapping');
  if (type === 'INCREASE') {
    await validateHeatSensitivePlacement({ product, locationId, actionLabel: 'booked' });
  }

  if (type === 'INCREASE') {
    const warehouseService = require('./warehouseService');
    await warehouseService.validateCapacity(warehouseId, qty);
  }

  const referenceNumber = generateAdjustmentReference();

  // Find exact stock record for this combination
  const stockWhere = { 
    productId: data.productId,
    warehouseId: warehouseId,
    locationId: locationId || null,
    batchNumber: batchNumber || null
  };

  let stock = await ProductStock.findOne({ where: stockWhere });

  if (type === 'DECREASE') {
    if (!stock || (stock.quantity || 0) - (stock.reserved || 0) < qty) {
      throw new Error('Insufficient available stock for this combination (Warehouse/Location/Batch)');
    }
  }

  const adjustment = await InventoryAdjustment.create({
    referenceNumber,
    companyId: effectiveCompanyId,
    productId: data.productId,
    warehouseId,
    locationId,
    batchId,
    batchNumber,
    bestBeforeDate,
    clientId,
    type,
    quantity: qty,
    reason: data.reason || null,
    notes: data.notes || null,
    status: 'PENDING',
    createdBy: reqUser.id,
  });

  if (stock) {
    const newQty = type === 'INCREASE' ? (stock.quantity || 0) + qty : Math.max(0, (stock.quantity || 0) - qty);
    await stock.update({ 
      quantity: newQty,
      bestBeforeDate: bestBeforeDate || stock.bestBeforeDate,
      clientId: clientId || stock.clientId,
      userId: reqUser.id,
      reason: data.reason || stock.reason
    });
  } else if (type === 'INCREASE') {
    await ProductStock.create({
      productId: data.productId,
      warehouseId,
      locationId,
      batchNumber,
      batchId,
      quantity: qty,
      reserved: 0,
      bestBeforeDate,
      clientId,
      userId: reqUser.id,
      reason: data.reason,
      status: 'ACTIVE',
    });
  }

  // SYNC Inventory Table (Warehouse Level)
  const { Inventory } = require('../models');
  const [inv] = await Inventory.findOrCreate({
    where: { productId: data.productId, warehouseId },
    defaults: { quantity: 0, reservedQuantity: 0 }
  });
  if (type === 'INCREASE') {
    await inv.increment('quantity', { by: qty });
  } else {
    await inv.decrement('quantity', { by: qty });
  }


  // Also create a entry in InventoryLog for history
  const { InventoryLog } = require('../models');
  await InventoryLog.create({
    productId: data.productId,
    warehouseId,
    locationId,
    batchId,
    batchNumber,
    bestBeforeDate,
    clientId,
    userId: reqUser.id,
    type: type === 'INCREASE' ? 'IN' : 'OUT',
    quantity: qty,
    reason: data.reason || (type === 'INCREASE' ? 'Stock In' : 'Stock Out'),
    referenceId: referenceNumber
  });

  await adjustment.update({ status: 'COMPLETED' });
  return InventoryAdjustment.findByPk(adjustment.id, {
    include: [
      { association: 'Product', attributes: ['id', 'name', 'sku'] },
      { association: 'Warehouse', required: false, attributes: ['id', 'name'] },
      { association: 'createdByUser', required: false, attributes: ['id', 'name', 'email'] },
      { association: 'Location', required: false, attributes: ['id', 'name', 'code'] },
      { association: 'Client', required: false, attributes: ['id', 'name'] },
    ],
  }).then((a) => {
    const j = a.toJSON();
    j.items = [{ product: j.Product, quantity: j.quantity }];
    j.createdBy = j.createdByUser;
    delete j.createdByUser;
    delete j.Product;
    return j;
  });
}

async function listCycleCounts(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  if (query.status) where.status = query.status;
  if (query.search) {
    where[Op.or] = [
      { referenceNumber: { [Op.like]: `%${query.search}%` } },
      { countName: { [Op.like]: `%${query.search}%` } },
    ];
  }
  const list = await CycleCount.findAll({
    where,
    order: [['scheduledDate', 'DESC'], ['createdAt', 'DESC']],
    include: [
      { association: 'Location', required: false, attributes: ['id', 'name', 'code', 'aisle', 'rack', 'shelf', 'bin'] },
      { association: 'countedByUser', required: false, attributes: ['id', 'name', 'email'] },
    ],
  });
  return list.map((c) => {
    const j = c.toJSON();
    j.countedBy = j.countedByUser;
    delete j.countedByUser;
    return j;
  });
}

async function createCycleCount(data, reqUser) {
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'company_admin' && reqUser.role !== 'inventory_manager' && reqUser.role !== 'warehouse_manager') {
    throw new Error('Not allowed to create cycle count');
  }
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('Company context required');
  const count = await CycleCount.create({
    companyId,
    countName: data.countName || 'Cycle Count',
    countType: data.countType || null,
    locationId: data.locationId || null,
    scheduledDate: data.scheduledDate || null,
    notes: data.notes || null,
    status: 'PENDING',
    itemsCount: 0,
    discrepancies: 0,
    countedBy: null,
  });
  const refNum = 'CC-' + String(count.id).padStart(5, '0');
  await count.update({ referenceNumber: refNum });
  return CycleCount.findByPk(count.id, {
    include: [
      { association: 'Location', required: false, attributes: ['id', 'name', 'code', 'aisle', 'rack', 'shelf', 'bin'] },
      { association: 'countedByUser', required: false, attributes: ['id', 'name', 'email'] },
    ],
  }).then((c) => {
    const j = c.toJSON();
    j.countedBy = j.countedByUser;
    delete j.countedByUser;
    return j;
  });
}


async function completeCycleCount(id, data, reqUser) {
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'company_admin' && reqUser.role !== 'inventory_manager') {
    throw new Error('Not allowed to complete cycle count');
  }

  const count = await CycleCount.findByPk(id);
  if (!count) throw new Error('Cycle count not found');
  if (count.status === 'COMPLETED') throw new Error('Cycle count already completed');

  if (reqUser.role !== 'super_admin' && count.companyId !== reqUser.companyId) {
    throw new Error('Cycle count not found');
  }

  // data.products = [{ productId, countedQty }]
  const products = Array.isArray(data.products) ? data.products : [];
  let discrepancies = 0;
  let itemsCount = 0;

  const transaction = await sequelize.transaction();
  try {
    for (const p of products) {
      const pid = p.productId;
      const counted = parseInt(p.countedQty, 10) || 0;
      itemsCount++;

      // Find current system stock
      // We assume CycleCount is for a specific location (count.locationId)
      // If count.locationId is null, it might be a "Whole Warehouse" or "Spot" count?
      // For simplicity, if locationId is present, we adjust stock AT THAT LOCATION.
      // If locationId is NULL, we might skip or fail? 
      // The UI requires locationId for creating cycle count usually, or it's optional?
      // In createCycleCount it says locationId || null.
      // If null, we can't easily auto-adjust stock because we don't know WHERE.
      // Let's assume locationId is REQUIRED for auto-adjustment for now, or throw error if missing.

      if (!count.locationId) {
        // If no location, we can't auto-adjust specific records. Just complete the count.
        continue;
      }

      // Find stock at this location
      // Note: A product might have multiple stocks at same location if Batches exist.
      // This logic is tricky if batches exist.
      // Simplified: We sum up all batches at this location for this product to compare?
      // usage: "Blind Count". User counts 10 units of Product A. System says 8.
      // We need to adjust. Which batch? 
      // If we don't support batch scanning in cycle count, we might default to no-batch or oldest batch?
      // Or we just update the "No Batch" record?
      // Let's try to find a stock record without batch first (or any).
      // Ideally, the "Input" should specify Batch if relevant.
      // For now, let's assume standard stock (null batch) or generic adjustment.

      const where = {
        productId: pid,
        locationId: count.locationId,
        warehouseId: (await (require('../models').Location).findByPk(count.locationId)).warehouseId
      };

      // Aggregated check if multiple batches?
      // For this implementation, let's match exact batch if provided in input, else match 'null' batch?
      // Or simpler: The user input should ideally match existing structure.
      // Let's stick to: Update Total Quantity at Location. 
      // If multiple batches exist, this gets complex. 
      // Plan: If Multiple Batches, we can't easily guess. 
      // For this fix: Assume non-batched or user selects 'No Batch'.
      // If user sends batchId/Number, use it.

      if (p.batchNumber) where.batchNumber = p.batchNumber;

      let stock = await ProductStock.findOne({ where, transaction });
      const systemQty = stock ? stock.quantity : 0;
      const diff = counted - systemQty;

      if (diff !== 0) {
        discrepancies++;
        // Create Adjustment
        const type = diff > 0 ? 'INCREASE' : 'DECREASE';
        const qty = Math.abs(diff);

        await InventoryAdjustment.create({
          referenceNumber: generateAdjustmentReference(),
          companyId: count.companyId,
          productId: pid,
          warehouseId: where.warehouseId,
          type,
          quantity: qty,
          reason: `Cycle Count #${count.referenceNumber}`,
          notes: 'Auto-adjustment from cycle count',
          status: 'COMPLETED',
          createdBy: reqUser.id
        }, { transaction });

        // Update Stock
        if (stock) {
          await stock.increment('quantity', { by: diff, transaction });
        } else if (diff > 0) {
          await ProductStock.create({
            ...where,
            quantity: diff,
            status: 'ACTIVE'
          }, { transaction });
        }
      }
    }

    await count.update({
      status: 'COMPLETED',
      itemsCount,
      discrepancies,
      countedBy: reqUser.id
    }, { transaction });

    await transaction.commit();
    return CycleCount.findByPk(id, {
      include: [
        { association: 'Location' },
        { association: 'countedByUser' }
      ]
    });

  } catch (err) {
    await transaction.rollback();
    throw err;
  }
}

async function listBatches(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  if (query.status) where.status = query.status;
  if (query.productId) where.productId = query.productId;
  if (query.warehouseId) where.warehouseId = query.warehouseId;
  if (query.search) {
    where[Op.or] = [
      { batchNumber: { [Op.like]: `%${query.search}%` } },
    ];
  }
  const list = await Batch.findAll({
    where,
    order: [['receivedDate', 'DESC'], ['createdAt', 'DESC']],
    include: [
      { association: 'Product', attributes: ['id', 'name', 'sku'] },
      { association: 'Warehouse', attributes: ['id', 'name', 'code'] },
      { association: 'Location', required: false, attributes: ['id', 'name', 'code', 'aisle', 'rack', 'shelf', 'bin'] },
      { association: 'Supplier', required: false, attributes: ['id', 'name', 'code'] },
    ],
  });
  return list.map((b) => {
    const j = b.toJSON();
    j.availableQuantity = Math.max(0, (b.quantity || 0) - (b.reserved || 0));
    return j;
  });
}

async function createBatch(data, reqUser) {
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'company_admin' && reqUser.role !== 'inventory_manager' && reqUser.role !== 'warehouse_manager') {
    throw new Error('Not allowed to create batch');
  }
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('Company context required');
  const product = await Product.findByPk(data.productId);
  if (!product) throw new Error('Product not found');
  if (product.companyId !== companyId && reqUser.role !== 'super_admin') throw new Error('Product not found');
  /* 
   * [MODIFIED] Now also creates/updates ProductStock so batch inventory is live.
   */
  const batch = await Batch.create({
    companyId,
    batchNumber: data.batchNumber || String(Date.now()),
    productId: data.productId,
    warehouseId: data.warehouseId,
    locationId: data.locationId || null,
    quantity: parseInt(data.quantity, 10) || 0,
    reserved: 0,
    unitCost: data.unitCost != null ? parseFloat(data.unitCost) : null,
    receivedDate: data.receivedDate || null,
    expiryDate: data.expiryDate || null,
    manufacturingDate: data.manufacturingDate || null,
    supplierId: data.supplierId || null,
    status: 'ACTIVE',
  });

  if (batch.quantity > 0 && batch.warehouseId) {
    const warehouseService = require('./warehouseService');
    await warehouseService.validateCapacity(batch.warehouseId, batch.quantity);
  }

  // Sync with ProductStock if quantity > 0
  if (batch.quantity > 0) {
    const stockQty = batch.quantity;
    const stockWhere = {
      productId: batch.productId,
      warehouseId: batch.warehouseId,
      locationId: batch.locationId || null,
      batchNumber: batch.batchNumber,
    };

    // Check if stock exists matching this batch
    let stock = await ProductStock.findOne({ where: stockWhere });
    if (stock) {
      await stock.increment('quantity', { by: stockQty });
    } else {
      await ProductStock.create({
        ...stockWhere,
        quantity: stockQty,
        reserved: 0,
        status: 'ACTIVE',
        expiryDate: batch.expiryDate, // Sync expiry if possible, though model might need update
      });
    }
  }

  return getBatchById(batch.id, reqUser);
}

async function getBatchById(id, reqUser) {
  const batch = await Batch.findByPk(id, {
    include: [
      { association: 'Product', attributes: ['id', 'name', 'sku'] },
      { association: 'Warehouse', attributes: ['id', 'name', 'code'] },
      { association: 'Location', required: false, attributes: ['id', 'name', 'code', 'aisle', 'rack', 'shelf', 'bin'] },
      { association: 'Supplier', required: false, attributes: ['id', 'name', 'code'] },
    ],
  });
  if (!batch) throw new Error('Batch not found');
  if (reqUser.role !== 'super_admin' && batch.companyId !== reqUser.companyId) throw new Error('Batch not found');
  const j = batch.toJSON();
  j.availableQuantity = Math.max(0, (batch.quantity || 0) - (batch.reserved || 0));
  return j;
}

async function updateBatch(id, data, reqUser) {
  const batch = await Batch.findByPk(id, { include: ['Product'] });
  if (!batch) throw new Error('Batch not found');
  if (reqUser.role !== 'super_admin' && batch.companyId !== reqUser.companyId) throw new Error('Batch not found');
  await batch.update({
    batchNumber: data.batchNumber !== undefined ? data.batchNumber : batch.batchNumber,
    locationId: data.locationId !== undefined ? data.locationId : batch.locationId,
    quantity: data.quantity !== undefined ? parseInt(data.quantity, 10) : batch.quantity,
    unitCost: data.unitCost !== undefined ? (data.unitCost == null ? null : parseFloat(data.unitCost)) : batch.unitCost,
    receivedDate: data.receivedDate !== undefined ? data.receivedDate : batch.receivedDate,
    expiryDate: data.expiryDate !== undefined ? data.expiryDate : batch.expiryDate,
    manufacturingDate: data.manufacturingDate !== undefined ? data.manufacturingDate : batch.manufacturingDate,
    supplierId: data.supplierId !== undefined ? data.supplierId : batch.supplierId,
    status: data.status !== undefined ? data.status : batch.status,
  });
  return getBatchById(batch.id, reqUser);
}

async function removeBatch(id, reqUser) {
  const batch = await Batch.findByPk(id);
  if (!batch) throw new Error('Batch not found');
  if (reqUser.role !== 'super_admin' && batch.companyId !== reqUser.companyId) throw new Error('Batch not found');
  await batch.destroy();
  return { message: 'Batch deleted' };
}

async function listMovements(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  if (query.type) where.type = query.type;
  if (query.startDate || query.endDate) {
    const dateCond = {};
    if (query.startDate) dateCond[Op.gte] = new Date(query.startDate + 'T00:00:00');
    if (query.endDate) dateCond[Op.lte] = new Date(query.endDate + 'T23:59:59');
    where.createdAt = dateCond;
  }
  const list = await Movement.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [
      { association: 'Product', attributes: ['id', 'name', 'sku'] },
      { association: 'Batch', required: false, attributes: ['id', 'batchNumber'] },
      { association: 'fromLocation', required: false, attributes: ['id', 'name', 'code', 'aisle', 'rack', 'shelf', 'bin'] },
      { association: 'toLocation', required: false, attributes: ['id', 'name', 'code', 'aisle', 'rack', 'shelf', 'bin'] },
      { association: 'fromWarehouse', required: false, attributes: ['id', 'name', 'code'] },
      { association: 'toWarehouse', required: false, attributes: ['id', 'name', 'code'] },
      { association: 'createdByUser', required: false, attributes: ['id', 'name', 'email'] },
    ],
  });
  return list.map((m) => {
    const j = m.toJSON();
    j.user = j.createdByUser;
    delete j.createdByUser;
    return j;
  });
}

async function createMovement(data, reqUser) {
  if (reqUser.role !== 'super_admin' && reqUser.role !== 'company_admin' && reqUser.role !== 'inventory_manager' && reqUser.role !== 'warehouse_manager') {
    throw new Error('Not allowed to create movement');
  }
  const companyId = reqUser.companyId || data.companyId;
  if (!companyId) throw new Error('Company context required');
  const product = await Product.findByPk(data.productId);
  if (!product) throw new Error('Product not found');
  if (product.companyId !== companyId && reqUser.role !== 'super_admin') throw new Error('Product not found');
  const qty = parseInt(data.quantity, 10) || 0;
  if (qty <= 0) throw new Error('Quantity must be greater than 0');

  const type = data.type || 'TRANSFER';
  const batchId = data.batchId || null;
  let batchNumber = null;

  if (batchId) {
    const b = await Batch.findByPk(batchId);
    if (b) batchNumber = b.batchNumber;
  }

  const transaction = await sequelize.transaction();

  try {
    // Resolve warehouses from locations if provided
    let fromWarehouseId = data.fromWarehouseId || null;
    let toWarehouseId = data.toWarehouseId || null;

    if (!fromWarehouseId && data.fromLocationId) {
      const fl = await (require('../models').Location).findByPk(data.fromLocationId);
      if (fl) fromWarehouseId = fl.warehouseId;
    }
    if (!toWarehouseId && data.toLocationId) {
      const tl = await (require('../models').Location).findByPk(data.toLocationId);
      if (tl) toWarehouseId = tl.warehouseId;
    }

    // 1. Log the Movement
    const movement = await Movement.create({
      companyId,
      type,
      productId: data.productId,
      batchId,
      fromWarehouseId,
      toWarehouseId,
      fromLocationId: data.fromLocationId || null,
      toLocationId: data.toLocationId || null,
      quantity: qty,
      reason: data.reason || null,
      notes: data.notes || null,
      createdBy: reqUser.id,
    }, { transaction });

    // 2. Adjust Stock based on Type
    /*
      RECEIVE/RETURN: Add to ToLocation
      PICK: Subtract from FromLocation
      TRANSFER: Subtract from FromLocation, Add to ToLocation
      ADJUST: (Handled via Adjustments usually, but if used here, implies manual +/- ?)
              Let's assume ADJUST here is just logging or behaves like Transfer if both locs exist? 
              For safety, we will restrict ADJUST to use createAdjustment API. 
              But if user uses this UI, we support:
              - If only ToLocation -> Add
              - If only FromLocation -> Subtract
    */

    // Helper to Add Stock
    const addStock = async (locId, q, batchNum) => {
      if (!locId) throw new Error('Destination location required');
      const loc = await (require('../models').Location).findByPk(locId);
      if (!loc) throw new Error(`Location ${locId} not found`);

      const warehouseService = require('./warehouseService');
      await warehouseService.validateCapacity(loc.warehouseId, q, { transaction });

      const where = {
        productId: data.productId,
        warehouseId: loc.warehouseId, // Use resolved warehouseId
        locationId: locId,
        batchNumber: batchNum || null
      };

      // We need to resolve warehouseId efficiently. 
      // Assuming Location belongs to a Warehouse.
      // Optimisation: movement creates usually pass warehouse context? No, just loc IDs.
      // Let's look up location.

      // Check if stock exists
      // Note: ProductStock unique key is usually product+warehouse+location+batch
      // We need to be careful with "null" batchNumber in where clause if DB treats it uniquely.
      // Sequelize "where: { batchNumber: null }" works for finding NULLs.

      let stock = await ProductStock.findOne({ where, transaction });
      if (stock) {
        await stock.increment('quantity', { by: q, transaction });
      } else {
        await ProductStock.create({
          ...where,
          quantity: q,
          status: 'ACTIVE'
        }, { transaction });
      }
    };

    // Helper to Remove Stock
    const removeStock = async (locId, q, batchNum) => {
      if (!locId) throw new Error('Source location required');
      // Resolve warehouse from location
      const loc = await (require('../models').Location).findByPk(locId);
      if (!loc) throw new Error(`Location ${locId} not found`);

      const where = {
        productId: data.productId,
        warehouseId: loc.warehouseId,
        locationId: locId,
        batchNumber: batchNum || null
      };

      const stock = await ProductStock.findOne({ where, transaction });
      if (!stock || (stock.quantity < q)) {
        throw new Error(`Insufficient stock at location ${loc.name || loc.code}`);
      }
      await stock.decrement('quantity', { by: q, transaction });
    };

    if (type === 'RECEIVE' || type === 'RETURN') {
      await addStock(data.toLocationId, qty, batchNumber);
    }
    else if (type === 'PICK') {
      await removeStock(data.fromLocationId, qty, batchNumber);
    }
    else if (type === 'TRANSFER') {
      await removeStock(data.fromLocationId, qty, batchNumber);
      await addStock(data.toLocationId, qty, batchNumber);
    }

    await transaction.commit();
    return getMovementById(movement.id, reqUser);

  } catch (err) {
    await transaction.rollback();
    throw err;
  }
}

async function getMovementById(id, reqUser) {
  const movement = await Movement.findByPk(id, {
    include: [
      { association: 'Product', attributes: ['id', 'name', 'sku'] },
      { association: 'Batch', required: false, attributes: ['id', 'batchNumber'] },
      { association: 'fromLocation', required: false, attributes: ['id', 'name', 'code', 'aisle', 'rack', 'shelf', 'bin'] },
      { association: 'toLocation', required: false, attributes: ['id', 'name', 'code', 'aisle', 'rack', 'shelf', 'bin'] },
      { association: 'fromWarehouse', required: false, attributes: ['id', 'name', 'code'] },
      { association: 'toWarehouse', required: false, attributes: ['id', 'name', 'code'] },
      { association: 'createdByUser', required: false, attributes: ['id', 'name', 'email'] },
    ],
  });
  if (!movement) throw new Error('Movement not found');
  if (reqUser.role !== 'super_admin' && movement.companyId !== reqUser.companyId) throw new Error('Movement not found');
  const j = movement.toJSON();
  j.user = j.createdByUser;
  delete j.createdByUser;
  return j;
}

async function updateMovement(id, data, reqUser) {
  const movement = await Movement.findByPk(id);
  if (!movement) throw new Error('Movement not found');
  if (reqUser.role !== 'super_admin' && movement.companyId !== reqUser.companyId) throw new Error('Movement not found');
  await movement.update({
    type: data.type !== undefined ? data.type : movement.type,
    batchId: data.batchId !== undefined ? data.batchId : movement.batchId,
    fromLocationId: data.fromLocationId !== undefined ? data.fromLocationId : movement.fromLocationId,
    toLocationId: data.toLocationId !== undefined ? data.toLocationId : movement.toLocationId,
    quantity: data.quantity !== undefined ? parseInt(data.quantity, 10) : movement.quantity,
    reason: data.reason !== undefined ? data.reason : movement.reason,
    notes: data.notes !== undefined ? data.notes : movement.notes,
  });
  return getMovementById(movement.id, reqUser);
}

async function removeMovement(id, reqUser) {
  const movement = await Movement.findByPk(id);
  if (!movement) throw new Error('Movement not found');
  if (reqUser.role !== 'super_admin' && movement.companyId !== reqUser.companyId) throw new Error('Movement not found');
  await movement.destroy();
  return { message: 'Movement deleted' };
}

async function listInventory(reqUser, query = {}) {
  const { Inventory, Product, Warehouse } = require('../models');
  const where = {};
  if (query.warehouseId) where.warehouseId = query.warehouseId;
  const productWhere = {};
  if (reqUser.role !== 'super_admin') productWhere.companyId = reqUser.companyId;
  if (query.search) {
    productWhere.name = { [Op.like]: `%${query.search}%` };
  }

  const inventory = await Inventory.findAll({
    where,
    include: [
      { model: Product, where: productWhere, attributes: ['id', 'name', 'sku', 'reorderLevel'] },
      { model: Warehouse, attributes: ['id', 'name'] }
    ]
  });

  return inventory.map(item => {
    const status = item.quantity <= 0 ? 'Out of Stock' : (item.quantity < (item.Product?.reorderLevel || 10) ? 'Low Stock' : 'In Stock');
    return {
      ...item.toJSON(),
      status,
      availableQuantity: item.availableQuantity // VIRTUAL field
    };
  });
}

async function listInventoryLogs(reqUser, query = {}) {
  const { InventoryLog, Product, Location, Customer, User, Warehouse } = require('../models');
  const where = {};
  if (query.warehouseId) where.warehouseId = query.warehouseId;
  if (query.type) where.type = query.type;
  if (query.productId) where.productId = query.productId;
  if (query.locationId) where.locationId = query.locationId;
  if (query.clientId) where.clientId = query.clientId;

  const productWhere = {};
  if (reqUser.role !== 'super_admin' && reqUser.companyId) {
    productWhere.companyId = reqUser.companyId;
  }

  const logs = await InventoryLog.findAll({
    where,
    include: [
      { model: Product, where: productWhere, required: true, attributes: ['id', 'name', 'sku'] },
      { association: 'Location', required: false, attributes: ['id', 'name', 'code'] },
      { association: 'Client', required: false, attributes: ['id', 'name'] },
      { association: 'Warehouse', required: false, attributes: ['id', 'name'] },
      { association: 'User', required: false, attributes: ['id', 'name', 'email'] },
    ],
    order: [['createdAt', 'DESC']],
    limit: query.limit ? parseInt(query.limit) : 100
  });

  return logs.map(l => {
    const j = l.get({ plain: true });
    j.createdBy = j.User;
    // For legacy logs or transfers, ensure product info is available
    if (j.Product) {
      j.product = j.Product; // Backend compatibility
    }
    if (j.type === 'TRANSFER' && j.referenceId) {
      const m = String(j.referenceId).match(/TRANSFER:\s*(\d+):(\d+)\s*->\s*(\d+):(\d+)/i);
      if (m) {
        j.fromWarehouseId = Number(m[1]);
        j.fromLocationId = Number(m[2]);
        j.toWarehouseId = Number(m[3]);
        j.toLocationId = Number(m[4]);
      }
    }
    return j;
  });
}



async function stockIn(data, reqUser) {
  const { Inventory, InventoryLog, Product, ProductStock } = require('../models');
  const { 
    productId, warehouseId, locationId, clientId, 
    quantity, referenceId, batchNumber, bestBeforeDate, reason 
  } = data;

  const product = await Product.findByPk(productId);
  if (!product) throw new Error('Product not found');
  if (reqUser.role !== 'super_admin' && product.companyId !== reqUser.companyId) throw new Error('Product not found');

  if (!String(batchNumber || '').trim()) {
    throw new Error(`${product.name || 'This product'} requires a Batch Number for accurate tracking`);
  }
  if (!bestBeforeDate) {
    throw new Error(`${product.name || 'This product'} requires a Best Before (Expiry) Date`);
  }
  await validateHeatSensitivePlacement({ product, locationId, actionLabel: 'booked' });

  // 1. Sync Warehouse Level Total
  const [inventory] = await Inventory.findOrCreate({
    where: { productId, warehouseId },
    defaults: { quantity: 0, reservedQuantity: 0 }
  });
  await inventory.increment('quantity', { by: quantity });

  // 2. Sync Granular Stock (Batch + Location)
  const [stock] = await ProductStock.findOrCreate({
    where: { 
      productId, 
      warehouseId, 
      locationId: locationId || null, 
      batchNumber: batchNumber.trim(),
      clientId: clientId || null
    },
    defaults: {
      quantity: 0,
      reserved: 0,
      bestBeforeDate,
      status: 'ACTIVE'
    }
  });
  await stock.increment('quantity', { by: quantity });
  if (stock.bestBeforeDate !== bestBeforeDate) {
     await stock.update({ bestBeforeDate });
  }
  
  // 3. Create Audit Log
  await InventoryLog.create({
    productId,
    warehouseId,
    locationId: locationId || null,
    clientId: clientId || null,
    type: 'IN',
    quantity,
    referenceId: referenceId || 'SCAN_IN',
    batchNumber: batchNumber.trim(),
    bestBeforeDate,
    reason: reason || 'Scan In',
    userId: reqUser.id
  });

  return inventory.reload();
}

async function stockOut(data, reqUser) {
  const { Inventory, InventoryLog } = require('../models');
  const { productId, warehouseId, quantity, referenceId } = data;

  const inventory = await Inventory.findOne({
    where: { productId, warehouseId }
  });

  if (!inventory || inventory.quantity < quantity) {
    throw new Error('Insufficient stock');
  }

  await inventory.decrement('quantity', { by: quantity });

  await InventoryLog.create({
    productId,
    warehouseId,
    type: 'OUT',
    quantity,
    referenceId
  });

  return inventory.reload();
}

async function transferStock(data, reqUser) {
  const { ProductStock, InventoryLog, sequelize } = require('../models');
  const { productId, fromLocationId, toLocationId, clientId, quantity, batchNumber, bestBeforeDate, reason } = data;
  
  const fromWarehouseId = data.fromWarehouseId || data.warehouseId;
  const toWarehouseId = data.toWarehouseId || fromWarehouseId;

  if (fromLocationId === toLocationId && fromWarehouseId === toWarehouseId) {
    throw new Error('Source and destination must be different');
  }

  const qty = parseInt(quantity);
  if (!qty || qty <= 0) throw new Error('Quantity must be greater than 0');
  const product = await Product.findByPk(productId);
  if (!product) throw new Error('Product not found');
  if (reqUser.role !== 'super_admin' && product.companyId !== reqUser.companyId) throw new Error('Product not found');
  if (!String(batchNumber || '').trim()) {
    throw new Error(`${product.name || 'This product'} requires a Batch Number for this transfer`);
  }
  if (!bestBeforeDate) {
    throw new Error(`${product.name || 'This product'} requires a Best Before (Expiry) Date for this transfer`);
  }
  await validateHeatSensitivePlacement({ product, locationId: toLocationId, actionLabel: 'transferred' });
  
  return sequelize.transaction(async (t) => {
    // 1. Check Source
    const sourceBaseWhere = {
      productId,
      warehouseId: fromWarehouseId,
      locationId: fromLocationId,
      ...(clientId ? { clientId } : {})
    };
    let source = null;
    if (batchNumber) {
      source = await ProductStock.findOne({
        where: { ...sourceBaseWhere, batchNumber },
        transaction: t,
      });
    }
    if (!source) {
      source = await ProductStock.findOne({
        where: sourceBaseWhere,
        order: [['quantity', 'DESC']],
        transaction: t,
      });
    }

    if (!source) {
      throw new Error('Insufficient stock in source location/warehouse');
    }

    const sourceRows = await ProductStock.findAll({
      where: sourceBaseWhere,
      order: [['quantity', 'DESC']],
      transaction: t,
    });
    const availableTotal = sourceRows.reduce((sum, row) => sum + Math.round(Number(row.quantity) || 0), 0);
    if (availableTotal < qty) {
      throw new Error(`Insufficient stock in source location for this product. Available: ${availableTotal}, Attempted: ${qty}`);
    }

    // 2. Add/Find Destination
    const [dest, created] = await ProductStock.findOrCreate({
      where: { 
        productId, 
        warehouseId: toWarehouseId, 
        locationId: toLocationId, 
        batchNumber: batchNumber || source.batchNumber || null 
      },
      defaults: { 
        quantity: 0, 
        reserved: 0, 
        clientId: clientId || source.clientId, 
        bestBeforeDate: bestBeforeDate || source.bestBeforeDate,
        status: 'ACTIVE'
      },
      transaction: t
    });

    // 3. Update Quantities (supports stock split across multiple batch rows)
    let remaining = qty;
    for (const row of sourceRows) {
      if (remaining <= 0) break;
      const rowQty = Number(row.quantity) || 0;
      if (rowQty <= 0) continue;
      const consume = Math.min(rowQty, remaining);
      await row.decrement('quantity', { by: consume, transaction: t });
      remaining -= consume;
    }
    await dest.increment('quantity', { by: qty, transaction: t });

    // SYNC Inventory Table (Warehouse Level)
    const { Inventory } = require('../models');
    
    // Decrement from source warehouse total
    const [sourceInv] = await Inventory.findOrCreate({
        where: { productId, warehouseId: fromWarehouseId },
        defaults: { quantity: 0, reservedQuantity: 0 },
        transaction: t
    });
    await sourceInv.decrement('quantity', { by: qty, transaction: t });

    // Increment to destination warehouse total
    const [destInv] = await Inventory.findOrCreate({
        where: { productId, warehouseId: toWarehouseId },
        defaults: { quantity: 0, reservedQuantity: 0 },
        transaction: t
    });
    await destInv.increment('quantity', { by: qty, transaction: t });


    // 4. Create Logs
    const logBase = {
      productId,
      clientId,
      userId: reqUser.id,
      batchNumber,
      bestBeforeDate: bestBeforeDate || source.bestBeforeDate,
      referenceId: `TRANSFER: ${fromWarehouseId}:${fromLocationId} -> ${toWarehouseId}:${toLocationId}`,
      reason: reason || 'Internal Transfer'
    };

    // Source Log (OUT)
    await InventoryLog.create({
      ...logBase,
      warehouseId: fromWarehouseId,
      locationId: fromLocationId,
      type: 'TRANSFER',
      quantity: -qty
    }, { transaction: t });

    // Destination Log (IN)
    await InventoryLog.create({
      ...logBase,
      warehouseId: toWarehouseId,
      locationId: toLocationId,
      type: 'TRANSFER',
      quantity: qty
    }, { transaction: t });

    // Create a Movement record for the transfer
    const effectiveCompanyId = reqUser.companyId || product.companyId;
    if (!effectiveCompanyId) throw new Error('Company context required for movement tracking');

    await Movement.create({
      companyId: effectiveCompanyId,
      type: 'TRANSFER',
      productId,
      fromWarehouseId,
      toWarehouseId,
      fromLocationId,
      toLocationId,
      quantity: qty,
      reason: reason || 'Internal Transfer',
      createdBy: reqUser.id,
    }, { transaction: t });

    return { success: true };
  });
}


async function transfer(data, reqUser) {
  const { Inventory, InventoryLog, sequelize } = require('../models');
  const { fromWarehouseId, toWarehouseId, productId, quantity } = data;

  if (fromWarehouseId === toWarehouseId) {
    throw new Error('Source and destination warehouses must be different');
  }

  return sequelize.transaction(async (t) => {
    const source = await Inventory.findOne({
      where: { productId, warehouseId: fromWarehouseId },
      transaction: t
    });

    if (!source || source.quantity < quantity) {
      throw new Error('Insufficient stock in source warehouse');
    }

    const [dest] = await Inventory.findOrCreate({
      where: { productId, warehouseId: toWarehouseId },
      defaults: { quantity: 0, reservedQuantity: 0 },
      transaction: t
    });

    await source.decrement('quantity', { by: quantity, transaction: t });
    await dest.increment('quantity', { by: quantity, transaction: t });

    await InventoryLog.create({
      productId,
      warehouseId: fromWarehouseId,
      type: 'TRANSFER',
      quantity,
      referenceId: `To WH: ${toWarehouseId}`
    }, { transaction: t });

    await InventoryLog.create({
      productId,
      warehouseId: toWarehouseId,
      type: 'TRANSFER',
      quantity,
      referenceId: `From WH: ${fromWarehouseId}`
    }, { transaction: t });

    return { success: true };
  });
}

module.exports = {
  listProducts,
  listCategories,
  getProductById,
  createProduct,
  bulkCreateProducts,
  updateProduct,
  addAlternativeSku,
  removeProduct,
  createCategory,
  updateCategory,
  removeCategory,
  listStock,
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
  createBatch,
  getBatchById,
  updateBatch,
  removeBatch,
  listMovements,
  createMovement,
  getMovementById,
  updateMovement,
  removeMovement,
  listInventory,
  listInventoryLogs,
  stockIn,
  stockOut,
  transfer,
  transferStock,
};
