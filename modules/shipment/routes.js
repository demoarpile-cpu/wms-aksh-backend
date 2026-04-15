const express = require('express');
const router = express.Router();
const shipmentController = require('../../controllers/shipmentController');
const { authenticate, requireRole } = require('../../middlewares/auth');

router.use(authenticate);

router.get('/', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'packer', 'viewer'), shipmentController.list);
router.get('/:id', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'packer', 'viewer'), shipmentController.getById);
router.post('/', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'packer'), shipmentController.create);
router.put('/:id', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'packer'), shipmentController.update);
router.post('/:id/deduct-stock', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'packer'), shipmentController.deductStock);

module.exports = router;
