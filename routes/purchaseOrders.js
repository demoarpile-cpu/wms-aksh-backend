const express = require('express');
const router = express.Router();
const purchaseOrderController = require('../controllers/purchaseOrderController');
const { authenticate, requireRole, requireAdmin, requireStaff, requireClient } = require('../middlewares/auth');

router.use(authenticate);

router.get('/', requireClient, purchaseOrderController.list);
router.get('/:id', requireClient, purchaseOrderController.getById);
router.post('/', requireStaff, purchaseOrderController.create);
router.put('/:id', requireStaff, purchaseOrderController.update);
router.delete('/:id', requireAdmin, purchaseOrderController.remove);
router.post('/:id/approve', requireAdmin, purchaseOrderController.approve);
router.post('/:id/generate-asn', requireStaff, purchaseOrderController.generateAsn);


module.exports = router;
