const express = require('express');
const router = express.Router();
const companyController = require('../../controllers/companyController');
const { authenticate, requireCompanyAdmin } = require('../../middlewares/auth');

router.use(authenticate, requireCompanyAdmin);
router.get('/profile', (req, res, next) => {
  req.params.id = req.user.companyId;
  return companyController.getById(req, res, next);
});
router.put('/profile', (req, res, next) => {
  req.params.id = req.user.companyId;
  return companyController.update(req, res, next);
});

module.exports = router;
