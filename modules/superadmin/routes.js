const express = require('express');
const router = express.Router();
const companyController = require('../../controllers/companyController');
const superadminController = require('../../controllers/superadminController');
const { authenticate, requireSuperAdmin } = require('../../middlewares/auth');

router.use(authenticate, requireSuperAdmin);
router.get('/stats', superadminController.stats);
router.get('/reports', superadminController.reports);
router.get('/companies', companyController.list);
router.get('/companies/:id', companyController.getById);
router.post('/companies', companyController.create);
router.put('/companies/:id', companyController.update);
router.delete('/companies/:id', companyController.remove);

module.exports = router;
