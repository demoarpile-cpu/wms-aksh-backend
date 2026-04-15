require('dotenv').config();
const bcrypt = require('bcryptjs');
const { sequelize, User, Company, Warehouse } = require('../models');

const SALT_ROUNDS = 10;

async function seed() {
  try {
    await sequelize.authenticate();
    const reset = process.argv.includes('--reset');
    if (reset) {
      await sequelize.sync({ force: true });
      console.log('Database reset (all tables recreated).');
    } else {
      // Create tables if missing; no alter so SQLite FK stays valid
      await sequelize.sync();
    }

    const existingSuperAdmin = await User.findOne({ where: { email: 'admin@kiaan-wms.com' } });
    if (!existingSuperAdmin) {
      const passwordHash = await bcrypt.hash('Admin@123', SALT_ROUNDS);
      await User.create({
        email: 'admin@kiaan-wms.com',
        passwordHash,
        name: 'Super Administrator',
        role: 'super_admin',
        companyId: null,
        warehouseId: null,
        status: 'ACTIVE',
      });
      console.log('Super Admin created: admin@kiaan-wms.com / Admin@123');
    } else {
      console.log('Super Admin already exists: admin@kiaan-wms.com');
    }

    const existingCompany = await Company.findOne({ where: { code: 'DEMO' } });
    let companyId;
    if (!existingCompany) {
      const company = await Company.create({
        name: 'Demo Company',
        code: 'DEMO',
        email: 'contact@demo.com',
        phone: '+44 20 1234 5678',
        address: '100 Business Park, London',
        status: 'ACTIVE',
      });
      companyId = company.id;
      console.log('Demo Company created:', company.name);
      const wh = await Warehouse.create({
        companyId: company.id,
        name: 'London Hub',
        code: 'LDN-001',
        address: '123 Warehouse St, London',
        status: 'ACTIVE',
      });
      console.log('Warehouse created:', wh.name);
    } else {
      companyId = existingCompany.id;
    }

    const existingCompanyAdmin = await User.findOne({ where: { email: 'companyadmin@kiaan-wms.com' } });
    if (!existingCompanyAdmin && companyId) {
      const passwordHash = await bcrypt.hash('Admin@123', SALT_ROUNDS);
      await User.create({
        email: 'companyadmin@kiaan-wms.com',
        passwordHash,
        name: 'Company Administrator',
        role: 'company_admin',
        companyId,
        warehouseId: null,
        status: 'ACTIVE',
      });
      console.log('Company Admin created: companyadmin@kiaan-wms.com / Admin@123');
    }

    const demoUsers = [
      { email: 'warehousemanager@kiaan-wms.com', name: 'Warehouse Manager', role: 'warehouse_manager' },
      { email: 'inventorymanager@kiaan-wms.com', name: 'Inventory Manager', role: 'inventory_manager' },
      { email: 'picker@kiaan-wms.com', name: 'Picker User', role: 'picker' },
      { email: 'packer@kiaan-wms.com', name: 'Packer User', role: 'packer' },
      { email: 'viewer@kiaan-wms.com', name: 'Viewer User', role: 'viewer' },
    ];
    const warehouse = await Warehouse.findOne({ where: { companyId } });
    for (const u of demoUsers) {
      const exists = await User.findOne({ where: { email: u.email } });
      if (!exists && companyId) {
        const passwordHash = await bcrypt.hash('Admin@123', SALT_ROUNDS);
        await User.create({
          email: u.email,
          passwordHash,
          name: u.name,
          role: u.role,
          companyId,
          warehouseId: warehouse?.id || null,
          status: 'ACTIVE',
        });
        console.log('User created:', u.email);
      }
    }

    console.log('Seed completed.');
    process.exit(0);
  } catch (err) {
    console.error('Seed failed:', err);
    process.exit(1);
  }
}

seed();
