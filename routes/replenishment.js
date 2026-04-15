const express = require('express');
const router = express.Router();
const replenishmentTaskController = require('../controllers/replenishmentTaskController');
const replenishmentConfigController = require('../controllers/replenishmentConfigController');
const { authenticate, requireRole } = require('../middlewares/auth');

const repRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'viewer'];
const repWriteRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager'];

router.use(authenticate);

// Tasks
router.get('/tasks', requireRole(...repRoles), replenishmentTaskController.list);
router.get('/tasks/:id', requireRole(...repRoles), replenishmentTaskController.getById);
router.post('/tasks', requireRole(...repWriteRoles), replenishmentTaskController.create);
router.put('/tasks/:id', requireRole(...repWriteRoles), replenishmentTaskController.update);
router.post('/tasks/:id/complete', requireRole(...repWriteRoles), replenishmentTaskController.complete);
router.delete('/tasks/:id', requireRole(...repWriteRoles), replenishmentTaskController.remove);

// Configs (Settings)
router.get('/configs', requireRole(...repRoles), replenishmentConfigController.list);
router.get('/configs/auto-check', requireRole(...repRoles), replenishmentConfigController.runAutoCheck);
router.post('/configs/auto-check', requireRole(...repWriteRoles), replenishmentConfigController.runAutoCheckAndCreateTasks);
router.get('/configs/:id', requireRole(...repRoles), replenishmentConfigController.getById);
router.post('/configs', requireRole(...repWriteRoles), replenishmentConfigController.create);
router.put('/configs/:id', requireRole(...repWriteRoles), replenishmentConfigController.update);
router.delete('/configs/:id', requireRole(...repWriteRoles), replenishmentConfigController.remove);

module.exports = router;
