const express = require('express');
const router = express.Router();
const warehouseController = require('../controllers/warehouseController');
const { authenticate, requireRole } = require('../middlewares/auth');

router.use(authenticate);
router.get('/', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer'), warehouseController.list);
router.get('/:id', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer'), warehouseController.getById);
router.get('/:id/products', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer'), warehouseController.getProducts);
router.post('/', requireRole('super_admin', 'company_admin'), warehouseController.create);
router.put('/:id', requireRole('super_admin', 'company_admin'), warehouseController.update);
router.delete('/:id', requireRole('super_admin', 'company_admin'), warehouseController.remove);

module.exports = router;
