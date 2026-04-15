/**
 * Stub labels API - returns empty list until label templates are stored in DB.
 */
async function list(req, res, next) {
  try {
    res.json({ success: true, data: [] });
  } catch (err) {
    next(err);
  }
}

module.exports = { list };
