const express = require('express');
const multer = require('multer');
const router = express.Router();
const purchaseOrderController = require('../controllers/purchaseOrderController');
const { authenticate, requireRole, requireAdmin, requireStaff, requireClient } = require('../middlewares/auth');
const upload = multer({
  dest: 'uploads/',
  limits: { fileSize: 20 * 1024 * 1024 },
});

router.use(authenticate);

router.get('/', requireClient, purchaseOrderController.list);
router.get('/:id', requireClient, purchaseOrderController.getById);
router.post('/', requireStaff, purchaseOrderController.create);
router.post('/upload-csv', requireStaff, upload.single('file'), purchaseOrderController.uploadCsv);
router.put('/:id', requireStaff, purchaseOrderController.update);
router.delete('/:id', requireAdmin, purchaseOrderController.remove);
router.post('/:id/approve', requireAdmin, purchaseOrderController.approve);
router.post('/:id/generate-asn', requireStaff, purchaseOrderController.generateAsn);
router.get('/:id/pdf', requireClient, purchaseOrderController.downloadPdf);


module.exports = router;
