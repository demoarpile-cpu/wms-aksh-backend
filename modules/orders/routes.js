const express = require('express');
const router = express.Router();
const orderController = require('../../controllers/orderController');
const customerController = require('../../controllers/customerController');
const { authenticate, requireRole } = require('../../middlewares/auth');

router.use(authenticate);

router.get('/sales', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer'), orderController.list);
router.get('/sales/:id', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer'), orderController.getById);
router.post('/sales', requireRole('super_admin', 'company_admin'), orderController.create);
router.put('/sales/:id', requireRole('super_admin', 'company_admin'), orderController.update);
router.delete('/sales/:id', requireRole('super_admin', 'company_admin'), orderController.remove);

router.get('/customers', requireRole('super_admin', 'company_admin', 'inventory_manager', 'picker', 'packer', 'viewer'), customerController.list);
router.get('/customers/:id', requireRole('super_admin', 'company_admin', 'inventory_manager', 'picker', 'packer', 'viewer'), customerController.getById);
router.post('/customers', requireRole('super_admin', 'company_admin'), customerController.create);
router.put('/customers/:id', requireRole('super_admin', 'company_admin'), customerController.update);
router.delete('/customers/:id', requireRole('super_admin', 'company_admin'), customerController.remove);

module.exports = router;
