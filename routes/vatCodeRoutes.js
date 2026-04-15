const express = require('express');
const router = express.Router();
const vatCodeController = require('../controllers/vatCodeController');
const { authenticate, requireRole } = require('../middlewares/auth');

router.use(authenticate);
router.get('/', requireRole('super_admin', 'company_admin', 'inventory_manager', 'viewer'), vatCodeController.list);
router.post('/', requireRole('super_admin', 'company_admin', 'inventory_manager'), vatCodeController.create);
router.put('/:id', requireRole('super_admin', 'company_admin', 'inventory_manager'), vatCodeController.update);
router.delete('/:id', requireRole('super_admin', 'company_admin', 'inventory_manager'), vatCodeController.remove);

module.exports = router;
