const express = require('express');
const router = express.Router();
const zoneController = require('../controllers/zoneController');
const { authenticate, requireRole } = require('../middlewares/auth');

router.use(authenticate);
router.get('/', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer'), zoneController.list);
router.get('/:id', requireRole('super_admin', 'company_admin', 'warehouse_manager'), zoneController.getById);
router.post('/', requireRole('super_admin', 'company_admin'), zoneController.create);
router.put('/:id', requireRole('super_admin', 'company_admin'), zoneController.update);
router.delete('/:id', requireRole('super_admin', 'company_admin'), zoneController.remove);

module.exports = router;
