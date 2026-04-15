const express = require('express');
const router = express.Router();
const dashboardController = require('../controllers/dashboardController');
const { authenticate, requireRole } = require('../middlewares/auth');

const dashboardRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'viewer', 'picker', 'packer'];

router.get('/stats', authenticate, requireRole(...dashboardRoles), dashboardController.stats);
router.get('/charts', authenticate, requireRole(...dashboardRoles), dashboardController.charts);

module.exports = router;
