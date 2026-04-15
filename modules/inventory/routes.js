const express = require('express');
const router = express.Router();
const inventoryController = require('../../controllers/inventoryController');
const { authenticate, requireRole, requireAdmin, requireStaff, requireClient } = require('../../middlewares/auth');

router.use(authenticate);

// All roles can read products for scan screen
const readRoles = ['super_admin', 'company_admin', 'inventory_manager', 'warehouse_manager', 'picker', 'packer', 'viewer'];
const writeRoles = ['super_admin', 'company_admin', 'inventory_manager'];
// Scan roles — picker, packer, warehouse_manager can also do adjustments for quick scan
const scanRoles = ['super_admin', 'company_admin', 'inventory_manager', 'warehouse_manager', 'picker', 'packer'];

router.get('/products', requireClient, inventoryController.listProducts);
router.get('/products/:id', requireClient, inventoryController.getProduct);
router.post('/products', requireAdmin, inventoryController.createProduct);
router.post('/products/bulk', requireAdmin, inventoryController.bulkCreateProducts);
router.post('/products/:id/alternative-skus', requireAdmin, inventoryController.addAlternativeSku);
router.put('/products/:id', requireAdmin, inventoryController.updateProduct);
router.delete('/products/:id', requireAdmin, inventoryController.removeProduct);

router.get('/categories', requireClient, inventoryController.listCategories);
router.post('/categories', requireAdmin, inventoryController.createCategory);
router.put('/categories/:id', requireAdmin, inventoryController.updateCategory);
router.delete('/categories/:id', requireAdmin, inventoryController.removeCategory);

router.get('/stock', requireClient, inventoryController.listStock);
router.get('/stock/by-best-before-date', requireClient, inventoryController.listStockByBestBeforeDate);
router.get('/stock/by-location', requireClient, inventoryController.listStockByLocation);
router.post('/stock', requireStaff, inventoryController.createStock);
router.put('/stock/:id', requireStaff, inventoryController.updateStock);
router.delete('/stock/:id', requireAdmin, inventoryController.removeStock);

router.get('/adjustments', requireClient, inventoryController.listAdjustments);
router.post('/adjustments', requireStaff, inventoryController.createAdjustment);

router.get('/cycle-counts', requireClient, inventoryController.listCycleCounts);
router.post('/cycle-counts', requireStaff, inventoryController.createCycleCount);
router.post('/cycle-counts/:id/complete', requireStaff, inventoryController.completeCycleCount);

router.get('/batches', requireClient, inventoryController.listBatches);
router.get('/batches/:id', requireClient, inventoryController.getBatch);
router.post('/batches', requireStaff, inventoryController.createBatch);
router.put('/batches/:id', requireStaff, inventoryController.updateBatch);
router.delete('/batches/:id', requireAdmin, inventoryController.removeBatch);

router.get('/movements', requireClient, inventoryController.listMovements);
router.get('/movements/:id', requireClient, inventoryController.getMovement);
router.post('/movements', requireStaff, inventoryController.createMovement);
router.put('/movements/:id', requireStaff, inventoryController.updateMovement);
router.delete('/movements/:id', requireAdmin, inventoryController.removeMovement);

// New Inventory System
router.get('/', requireClient, inventoryController.listInventory);
router.get('/logs', requireClient, inventoryController.listInventoryLogs);
router.post('/stock-in', requireStaff, inventoryController.stockIn);
router.post('/stock-out', requireStaff, inventoryController.stockOut);
router.post('/transfer', requireStaff, inventoryController.transfer);
router.post('/transfer-stock', requireStaff, inventoryController.transferStock);

module.exports = router;
