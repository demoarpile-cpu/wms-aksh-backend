const express = require('express');
const router = express.Router();
const supplierProductController = require('../controllers/supplierProductController');
const { authenticate, requireRole } = require('../middlewares/auth');

router.use(authenticate);

const writeRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager'];
const readRoles = [...writeRoles, 'viewer'];

router.get('/', requireRole(...readRoles), supplierProductController.list);
router.post('/bulk-upload', requireRole(...writeRoles), supplierProductController.bulkUpload);
router.delete('/:id', requireRole(...writeRoles), supplierProductController.remove);

module.exports = router;
