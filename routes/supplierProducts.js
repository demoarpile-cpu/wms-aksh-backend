const express = require('express');
const multer = require('multer');
const router = express.Router();
const supplierProductController = require('../controllers/supplierProductController');
const { authenticate, requireRole } = require('../middlewares/auth');

const upload = multer({
  dest: 'uploads/',
  limits: { fileSize: 50 * 1024 * 1024 },
});

router.use(authenticate);

const writeRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager'];
const readRoles = [...writeRoles, 'viewer'];

router.get('/', requireRole(...readRoles), supplierProductController.list);
router.get('/:supplierId/products', requireRole(...readRoles), supplierProductController.listMappedProductsBySupplier);
router.post('/bulk-upload', requireRole(...writeRoles), upload.single('file'), supplierProductController.bulkUpload);
router.delete('/:id', requireRole(...writeRoles), supplierProductController.remove);

module.exports = router;
