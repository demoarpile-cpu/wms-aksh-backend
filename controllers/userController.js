const userService = require('../services/userService');

async function list(req, res, next) {
  try {
    const users = await userService.list(req.user, req.query);
    res.json({ success: true, data: users });
  } catch (err) {
    next(err);
  }
}

async function getById(req, res, next) {
  try {
    const user = await userService.getById(req.params.id, req.user);
    res.json({ success: true, data: user });
  } catch (err) {
    if (err.message === 'User not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function create(req, res, next) {
  try {
    const user = await userService.create(req.body, req.user);
    res.status(201).json({ success: true, data: user });
  } catch (err) {
    if (err.message === 'Email already registered') return res.status(400).json({ success: false, message: err.message });
    if (err.message?.includes('companyId') || err.message?.includes('role') || err.message?.includes('Password')) {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

async function update(req, res, next) {
  try {
    const user = await userService.update(req.params.id, req.body, req.user);
    res.json({ success: true, data: user });
  } catch (err) {
    if (err.message === 'User not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await userService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'User deleted' });
  } catch (err) {
    if (err.message === 'User not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, getById, create, update, remove };
