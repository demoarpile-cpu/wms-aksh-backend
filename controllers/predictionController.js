const predictionService = require('../services/predictionService');

async function list(req, res, next) {
    try {
        const data = await predictionService.getPredictionData(req.user.companyId);
        res.json({ success: true, data });
    } catch (err) {
        next(err);
    }
}

module.exports = {
    list
};
