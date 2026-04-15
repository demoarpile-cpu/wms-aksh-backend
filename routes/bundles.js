const express = require('express');
const router = express.Router();
const bundleController = require('../controllers/bundleController');
const { authenticate, requireRole } = require('../middlewares/auth');

router.use(authenticate);
router.get('/', requireRole('super_admin', 'company_admin', 'inventory_manager', 'viewer'), bundleController.list);
router.get('/:id', requireRole('super_admin', 'company_admin', 'inventory_manager', 'viewer'), bundleController.getById);
router.post('/', requireRole('super_admin', 'company_admin', 'inventory_manager'), bundleController.create);
router.put('/:id', requireRole('super_admin', 'company_admin', 'inventory_manager'), bundleController.update);
router.delete('/:id', requireRole('super_admin', 'company_admin', 'inventory_manager'), bundleController.remove);

module.exports = router;
