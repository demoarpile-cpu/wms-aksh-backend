const express = require('express');
const router = express.Router();
const { authenticate } = require('../middlewares/auth');
const ReturnController = require('../controllers/ReturnController');

console.log('--- Loading Return Routes ---'); // Debug log

// All routes require authentication
router.use((req, res, next) => {
    console.log(`Return Route Hit: ${req.method} ${req.originalUrl}`);
    next();
});
router.use(authenticate);

// RMA Creation & Listing
router.post('/', ReturnController.createRMA);
router.get('/', ReturnController.getAllReturns);
router.get('/:id', ReturnController.getReturnById);

// RMA Lifecycle Actions
router.put('/:id/receive', ReturnController.receiveItem);
router.put('/:id/inspect', ReturnController.inspectRMA);
router.put('/:id/refund', ReturnController.processRefund);
router.put('/:id/close', ReturnController.closeRMA);

module.exports = router;
