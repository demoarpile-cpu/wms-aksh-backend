const express = require('express');
const router = express.Router();
const goodsReceiptController = require('../controllers/goodsReceiptController');
const { authenticate, requireRole } = require('../middlewares/auth');

router.use(authenticate);

router.get('/', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'viewer'), goodsReceiptController.list);
router.get('/:id', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'viewer'), goodsReceiptController.getById);
router.post('/', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager'), goodsReceiptController.create);
router.put('/:id/receive', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager'), goodsReceiptController.updateReceived);
router.delete('/:id', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager'), goodsReceiptController.remove);

module.exports = router;
