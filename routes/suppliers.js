const express = require('express');
const router = express.Router();
const supplierController = require('../controllers/supplierController');
const { authenticate, requireRole } = require('../middlewares/auth');

router.use(authenticate);
router.get('/', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'viewer'), supplierController.list);
router.get('/:id', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'viewer'), supplierController.getById);
router.post('/', requireRole('super_admin', 'company_admin'), supplierController.create);
router.put('/:id', requireRole('super_admin', 'company_admin'), supplierController.update);
router.delete('/:id', requireRole('super_admin', 'company_admin'), supplierController.remove);

module.exports = router;
