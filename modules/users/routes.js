const express = require('express');
const router = express.Router();
const userController = require('../../controllers/userController');
const { authenticate, requireSuperAdmin, requireCompanyAdmin } = require('../../middlewares/auth');
const { requireRole } = require('../../middlewares/auth');

router.use(authenticate);
router.get('/', requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager'), userController.list);
router.get('/:id', requireRole('super_admin', 'company_admin'), userController.getById);
router.post('/', requireRole('super_admin', 'company_admin'), userController.create);
router.put('/:id', requireRole('super_admin', 'company_admin'), userController.update);
router.delete('/:id', requireRole('super_admin', 'company_admin'), userController.remove);

module.exports = router;
