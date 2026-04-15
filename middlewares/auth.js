const jwt = require('jsonwebtoken');
const { User } = require('../models');

const JWT_SECRET = process.env.JWT_SECRET || 'your-super-secret-jwt-key-change-in-production';

const normalizeRole = (role) => (role || '').toLowerCase().replace(/-/g, '_');

const authenticate = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.log('[AUTH] No token found in header');
      return res.status(401).json({ success: false, message: 'Token required' });
    }
    const token = authHeader.split(' ')[1];
    const decoded = jwt.verify(token, JWT_SECRET);
    
    // Log decoded token (be careful with sensitive info, but for debugging this is what user asked)
    console.log('[AUTH] Decoded Token:', { userId: decoded.userId, iat: decoded.iat });

    const user = await User.findByPk(decoded.userId, {
      attributes: { exclude: ['passwordHash'] },
      include: [
        { association: 'Company', attributes: ['id', 'name', 'code'] }, 
        { association: 'Warehouse', attributes: ['id', 'name'] }
      ],
    });

    if (!user || user.status !== 'ACTIVE') {
      console.log(`[AUTH] User ${decoded.userId} not found or inactive`);
      return res.status(401).json({ success: false, message: 'User not found or inactive' });
    }

    req.user = user;
    req.userId = user.id;
    req.userRole = normalizeRole(user.role);
    
    console.log(`[AUTH] Logged in: ${user.email} | DB Role: ${user.role} | Normalized: ${req.userRole}`);
    
    next();
  } catch (err) {
    console.error('[AUTH] Error:', err.name, err.message);
    if (err.name === 'JsonWebTokenError') return res.status(401).json({ success: false, message: 'Invalid token' });
    if (err.name === 'TokenExpiredError') return res.status(401).json({ success: false, message: 'Token expired' });
    next(err);
  }
};

const requireRole = (...allowedRoles) => (req, res, next) => {
  const roles = allowedRoles.map(normalizeRole);
  if (!req.userRole || !roles.includes(req.userRole)) {
    console.log(`[AUTH] ACCESS DENIED: User ${req.user?.email} (${req.userRole}) tried to access ${req.originalUrl}. Required: ${roles.join(', ')}`);
    return res.status(403).json({ success: false, message: `Access denied. Required role: ${allowedRoles.join(' or ')}` });
  }
  next();
};

const requireSuperAdmin = requireRole('super_admin');
const requireCompanyAdmin = requireRole('company_admin');
const requireWarehouseManager = requireRole('warehouse_manager');
const requireInventoryManager = requireRole('inventory_manager');
const requirePicker = requireRole('picker');
const requirePacker = requireRole('packer');
const requireViewer = requireRole('viewer');

const requireAdmin = requireRole('company_admin', 'super_admin');
const requireStaff = requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer');
const requireClient = requireRole('super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer');

const optionalAuth = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) return next();
    const token = authHeader.split(' ')[1];
    const decoded = jwt.verify(token, JWT_SECRET);
    const user = await User.findByPk(decoded.userId, { attributes: { exclude: ['passwordHash'] } });
    if (user && user.status === 'ACTIVE') {
      req.user = user;
      req.userId = user.id;
      req.userRole = normalizeRole(user.role);
    }
    next();
  } catch (_) {
    next();
  }
};

module.exports = {
  authenticate,
  requireRole,
  requireSuperAdmin,
  requireCompanyAdmin,
  requireWarehouseManager,
  requireInventoryManager,
  requirePicker,
  requirePacker,
  requireViewer,
  requireAdmin,
  requireStaff,
  requireClient,
  optionalAuth,
  JWT_SECRET,
};
