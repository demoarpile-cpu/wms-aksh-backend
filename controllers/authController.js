const authService = require('../services/authService');

async function login(req, res, next) {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ success: false, message: 'Email and password required' });
    }
    const result = await authService.login(email, password);
    if (!result) {
      return res.status(401).json({ success: false, message: 'Invalid email or password' });
    }
    res.json({ success: true, ...result });
  } catch (err) {
    next(err);
  }
}

async function me(req, res, next) {
  try {
    const u = req.user.toJSON();
    delete u.passwordHash;
    res.json({ success: true, user: u });
  } catch (err) {
    next(err);
  }
}

async function updateProfile(req, res, next) {
  try {
    const user = await authService.updateProfile(req.user.id, req.body);
    res.json({ success: true, user });
  } catch (err) {
    if (err.message === 'User not found' || err.message === 'Email already registered' ||
        err.message === 'Current password is incorrect' || err.message === 'Current password required to change password' ||
        err.message === 'New password must be at least 6 characters') {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
}

module.exports = { login, me, updateProfile };
