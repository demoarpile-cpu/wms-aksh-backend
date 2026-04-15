/**
 * Creates only Super Admin user (for quick login).
 * Run: node scripts/create-superadmin.js
 */
require('dotenv').config();
const bcrypt = require('bcryptjs');
const { sequelize, User } = require('../models');

const SUPER_ADMIN_EMAIL = 'admin@kiaan-wms.com';
const SUPER_ADMIN_PASSWORD = 'Admin@123';
const SALT_ROUNDS = 10;

async function createSuperAdmin() {
  try {
    await sequelize.authenticate();
    console.log('Database connected.');
    await sequelize.sync({ alter: true });
    console.log('Tables synced.');

    let user = await User.findOne({ where: { email: SUPER_ADMIN_EMAIL } });
    if (user) {
      console.log('Super Admin already exists.');
      console.log('Login: email =', SUPER_ADMIN_EMAIL, ', password =', SUPER_ADMIN_PASSWORD);
      process.exit(0);
      return;
    }

    const passwordHash = await bcrypt.hash(SUPER_ADMIN_PASSWORD, SALT_ROUNDS);
    user = await User.create({
      email: SUPER_ADMIN_EMAIL,
      passwordHash,
      name: 'Super Administrator',
      role: 'super_admin',
      companyId: null,
      warehouseId: null,
      status: 'ACTIVE',
    });
    console.log('Super Admin created successfully.');
    console.log('-----------------------------------');
    console.log('Login with:');
    console.log('  Email   :', SUPER_ADMIN_EMAIL);
    console.log('  Password:', SUPER_ADMIN_PASSWORD);
    console.log('-----------------------------------');
    process.exit(0);
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
}

createSuperAdmin();
