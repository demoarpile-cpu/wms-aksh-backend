require('dotenv').config();
// Restarting to sync bulk import fixes
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const uploadsDir = path.join(__dirname, 'uploads');
try {
  if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });
} catch (_) {
  /* multer may still work if dir exists elsewhere */
}
const { sequelize } = require('./models');
const routes = require('./routes');
const superadminController = require('./controllers/superadminController');
const purchaseOrderController = require('./controllers/purchaseOrderController');
const goodsReceiptController = require('./controllers/goodsReceiptController');
const orderController = require('./controllers/orderController');
const inventoryController = require('./controllers/inventoryController');
const { authenticate, requireSuperAdmin, requireRole, requireAdmin, requireStaff, requireClient } = require('./middlewares/auth');
const dashboardController = require('./controllers/dashboardController');
const reportController = require('./controllers/reportController');
const analyticsController = require('./controllers/analyticsController');
const cronService = require('./services/cronService');

const app = express();
const PORT = process.env.PORT || 3001;

function getBrokenMySqlTableName(err) {
  const errno = err?.errno ?? err?.original?.errno ?? err?.parent?.errno;
  const sql = err?.sql || err?.original?.sql || err?.parent?.sql || '';
  if (errno !== 1932 || !sql) return null;

  const match = sql.match(/SHOW\s+INDEX\s+FROM\s+`?([a-zA-Z0-9_]+)`?/i);
  return match ? match[1] : null;
}

app.use(cors({
  origin: function (origin, callback) {
    // Allow requests with no origin (like mobile apps or curl)
    if (!origin) return callback(null, true);
    // Allow all origins for now to fix user's Railway connection issue
    callback(null, true);
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Accept', 'X-Requested-With'],
}));

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Sales orders - register FIRST so DELETE /api/orders/sales/:id never 404s
const soRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'picker', 'packer', 'viewer'];
const soWriteRoles = ['super_admin', 'company_admin'];
app.get('/api/orders/sales', authenticate, requireRole(...soRoles), orderController.list);
app.post('/api/orders/sales', authenticate, requireRole(...soWriteRoles), orderController.create);
app.get('/api/orders/sales/:id', authenticate, requireRole(...soRoles), orderController.getById);
app.put('/api/orders/sales/:id', authenticate, requireRole(...soWriteRoles), orderController.update);
app.delete('/api/orders/sales/:id', authenticate, requireRole(...soWriteRoles), orderController.remove);

// Dashboard - single route /api/dashboard/:type so stats + charts dono chalenge
const dashboardRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'viewer', 'picker', 'packer'];
app.get('/api/dashboard/:type', authenticate, requireRole(...dashboardRoles), (req, res, next) => {
  const type = (req.params.type || '').toLowerCase();
  if (type === 'stats') return dashboardController.stats(req, res, next);
  if (type === 'charts') return dashboardController.charts(req, res, next);
  res.status(404).json({ success: false, message: 'Not found. Use /api/dashboard/stats or /api/dashboard/charts' });
});
app.get('/api/reports', authenticate, requireRole(...dashboardRoles), reportController.list);
app.get('/api/reports/:id', authenticate, requireRole(...dashboardRoles), reportController.getById);
app.get('/api/reports/:id/download', authenticate, requireRole(...dashboardRoles), reportController.download);
app.post('/api/reports', authenticate, requireRole(...dashboardRoles), reportController.create);
app.put('/api/reports/:id', authenticate, requireRole(...dashboardRoles), reportController.update);
app.delete('/api/reports/:id', authenticate, requireRole(...dashboardRoles), reportController.remove);

// AI / Predictions
const predictionController = require('./controllers/predictionController');
app.get('/api/predictions', authenticate, requireRole(...dashboardRoles), predictionController.list);

// Analytics
app.post('/api/analytics/pricing-calculate', authenticate, requireRole(...dashboardRoles), analyticsController.pricingCalculate);
app.get('/api/analytics/margins', authenticate, requireRole(...dashboardRoles), analyticsController.marginsReport);

// Super admin APIs - register first so they always work
app.get('/api/superadmin/stats', authenticate, requireSuperAdmin, superadminController.stats);
app.get('/api/superadmin/reports', authenticate, requireSuperAdmin, superadminController.reports);

// Purchase orders - explicit routes so 404 doesn't happen
const poRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager', 'viewer'];
const poWriteRoles = ['super_admin', 'company_admin', 'warehouse_manager', 'inventory_manager'];
app.get('/api/purchase-orders', authenticate, requireClient, purchaseOrderController.list);
app.get('/api/purchase-orders/:id', authenticate, requireClient, purchaseOrderController.getById);
app.post('/api/purchase-orders', authenticate, requireStaff, purchaseOrderController.create);
app.put('/api/purchase-orders/:id', authenticate, requireStaff, purchaseOrderController.update);
app.delete('/api/purchase-orders/:id', authenticate, requireAdmin, purchaseOrderController.remove);
app.post('/api/purchase-orders/:id/approve', authenticate, requireAdmin, purchaseOrderController.approve);

// Goods receiving - explicit routes
app.get('/api/goods-receiving', authenticate, requireClient, goodsReceiptController.list);
app.get('/api/goods-receiving/:id', authenticate, requireClient, goodsReceiptController.getById);
app.post('/api/goods-receiving', authenticate, requireStaff, goodsReceiptController.create);
app.put('/api/goods-receiving/:id/receive', authenticate, requireStaff, goodsReceiptController.updateReceived);
app.put('/api/goods-receiving/:id/asn', authenticate, requireStaff, goodsReceiptController.updateAsnItems);
app.post('/api/goods-receiving/:id/finalize', authenticate, requireStaff, goodsReceiptController.finalizeReceiving);
app.delete('/api/goods-receiving/:id', authenticate, requireAdmin, goodsReceiptController.remove);

// Inventory products - explicit DELETE so /api/inventory/products/:id never 404s
const invProductRoles = ['super_admin', 'company_admin', 'inventory_manager'];
app.delete('/api/inventory/products/:id', authenticate, requireRole(...invProductRoles), inventoryController.removeProduct);

// POST /api/products/:id/alternative-skus (same handler as inventory, so client can call either path)
app.post('/api/products/:id/alternative-skus', authenticate, requireRole(...invProductRoles), inventoryController.addAlternativeSku);

const returnRoutes = require('./routes/returnRoutes');
app.use('/api/returns', returnRoutes);

app.use(routes);

app.use((err, req, res, next) => {
  console.error(err);
  res.status(err.status || 500).json({
    success: false,
    message: err.message || 'Internal server error',
  });
});



async function start() {
  try {
    const dialect = sequelize.getDialect();
    if (dialect === 'sqlite') {
      const storage = sequelize.config.storage || path.join(__dirname, 'warehouse_wms.sqlite');
      const fullPath = path.isAbsolute(storage) ? storage : path.resolve(process.cwd(), storage);
      console.log('--- DB Check ---');
      console.log('Type: SQLite');
      console.log('File:', fullPath);
      console.log('---');
    } else {
      console.log('--- DB Check ---');
      console.log('Type:', dialect.toUpperCase());
      console.log('Host:', sequelize.config.host || 'localhost');
      console.log('Port:', sequelize.config.port || (dialect === 'mysql' ? 3306 : 'default'));
      console.log('User:', sequelize.config.username);
      console.log('DB:', sequelize.config.database);
      console.log('---');
    }

    await sequelize.authenticate();
    console.log('Connected to database successfully.');
    // SQLite: allow alter (drop/recreate tables) by disabling FK checks during sync
    if (dialect === 'sqlite') {
      await sequelize.query('PRAGMA foreign_keys = OFF');
      const [tables] = await sequelize.query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_backup'");
      const queryInterface = sequelize.getQueryInterface();
      for (const t of tables) {
        try {
          await queryInterface.dropTable(t.name);
          console.log('Dropped leftover backup table:', t.name);
        } catch (e) {
          // ignore
        }
      }
    }
    const syncOptions = { alter: dialect === 'sqlite' };
    if (dialect === 'mysql') {
      let syncDone = false;
      for (let attempt = 1; attempt <= 3 && !syncDone; attempt += 1) {
        try {
          await sequelize.sync(syncOptions);
          syncDone = true;
        } catch (syncErr) {
          const brokenTable = getBrokenMySqlTableName(syncErr);
          if (!brokenTable || attempt === 3) {
            throw syncErr;
          }

          console.warn(`[DB] Corrupted table metadata detected for "${brokenTable}". Attempting to drop and recreate.`);
          try {
            await sequelize.query('SET FOREIGN_KEY_CHECKS = 0');
            await sequelize.query(`DROP TABLE IF EXISTS \`${brokenTable}\``);
            await sequelize.query('SET FOREIGN_KEY_CHECKS = 1');
            console.log(`[DB] Dropped broken table "${brokenTable}". Retrying sync (${attempt + 1}/3)...`);
          } catch (dropErr) {
            try {
              await sequelize.query('SET FOREIGN_KEY_CHECKS = 1');
            } catch (_) {
              // ignore cleanup errors
            }
            throw dropErr;
          }
        }
      }
    } else {
      await sequelize.sync(syncOptions);
    }
    
    // MySQL: manual column fixes if alter is skipped
    if (dialect === 'mysql') {
        const manualCols = [
          { t: 'inventory_adjustments', c: 'batch_id', type: 'INT' },
          { t: 'inventory_adjustments', c: 'batch_number', type: 'VARCHAR(255)' },
          { t: 'inventory_adjustments', c: 'client_id', type: 'INT' },
          { t: 'inventory_adjustments', c: 'location_id', type: 'INT' },
          { t: 'inventory_adjustments', c: 'best_before_date', type: 'DATE' },
          { t: 'inventory_adjustments', c: 'created_by', type: 'INT' },
          { t: 'inventory_logs', c: 'batch_id', type: 'INT' },
          { t: 'inventory_logs', c: 'batch_number', type: 'VARCHAR(255)' },
          { t: 'inventory_logs', c: 'client_id', type: 'INT' },
          { t: 'inventory_logs', c: 'location_id', type: 'INT' },
          { t: 'inventory_logs', c: 'best_before_date', type: 'DATE' },
          { t: 'inventory_logs', c: 'user_id', type: 'INT' },
          { t: 'inventory_logs', c: 'reason', type: 'VARCHAR(255)' },
          { t: 'product_stocks', c: 'batch_id', type: 'INT' },
          { t: 'product_stocks', c: 'client_id', type: 'INT' },
          { t: 'product_stocks', c: 'location_id', type: 'INT' },
          { t: 'product_stocks', c: 'batch_number', type: 'VARCHAR(255)' },
          { t: 'product_stocks', c: 'reason', type: 'VARCHAR(255)' },
          { t: 'product_stocks', c: 'best_before_date', type: 'DATE' },
          { t: 'product_stocks', c: 'user_id', type: 'INT' },
          { t: 'categories', c: 'company_id', type: 'INT' },
          { t: 'products', c: 'pack_size', type: 'INT DEFAULT 1' },
          { t: 'products', c: 'color', type: 'VARCHAR(255)' },
          { t: 'products', c: 'supplier_id', type: 'INT' },
          { t: 'products', c: 'alternative_skus', type: 'LONGTEXT' },
          { t: 'products', c: 'supplier_products', type: 'LONGTEXT' },
          { t: 'products', c: 'price_lists', type: 'LONGTEXT' },
          { t: 'products', c: 'cartons', type: 'LONGTEXT' },
          { t: 'products', c: 'images', type: 'LONGTEXT' },
          { t: 'products', c: 'marketplace_skus', type: 'LONGTEXT' },
          { t: 'products', c: 'best_before_date_warning_period_days', type: 'INT DEFAULT 0' },
          { t: 'purchase_orders', c: 'warehouse_id', type: 'INT' },
          { t: 'purchase_orders', c: 'client_id', type: 'INT' },
          { t: 'goods_receipts', c: 'warehouse_id', type: 'INT' },
          { t: 'goods_receipts', c: 'delivery_type', type: 'VARCHAR(255)' },
          { t: 'goods_receipts', c: 'eta', type: 'DATETIME' },
          { t: 'goods_receipts', c: 'total_to_book', type: 'INT DEFAULT 0' },
          { t: 'movements', c: 'from_warehouse_id', type: 'INT' },
          { t: 'movements', c: 'to_warehouse_id', type: 'INT' },
          { t: 'movements', c: 'from_location_id', type: 'INT' },
          { t: 'movements', c: 'to_location_id', type: 'INT' },
          { t: 'movements', c: 'batch_id', type: 'INT' },
          { t: 'movements', c: 'company_id', type: 'INT' },
          { t: 'movements', c: 'created_by', type: 'INT' },
          { t: 'zones', c: 'warehouse_id', type: 'INT' },
          { t: 'zones', c: 'company_id', type: 'INT' },
          { t: 'locations', c: 'warehouse_id', type: 'INT' },
          { t: 'locations', c: 'zone_id', type: 'INT' },
          { t: 'locations', c: 'heat_sensitive', type: 'VARCHAR(255)' },
          { t: 'users', c: 'company_id', type: 'INT' },
          { t: 'users', c: 'warehouse_id', type: 'INT' },
          { t: 'users', c: 'status', type: 'VARCHAR(50) DEFAULT "ACTIVE"' },
          { t: 'supplier_products', c: 'effective_date', type: 'DATE' },
          { t: 'goods_receipt_items', c: 'unit_cost', type: 'DECIMAL(12, 2)' },
          { t: 'reports', c: 'content', type: 'LONGTEXT' },
          { t: 'reports', c: 'last_run_at', type: 'DATETIME' },
          { t: 'reports', c: 'report_name', type: 'VARCHAR(255)' },
          { t: 'reports', c: 'report_type', type: 'VARCHAR(255)' },
          { t: 'reports', c: 'category', type: 'VARCHAR(255)' },
          { t: 'reports', c: 'start_date', type: 'DATE' },
          { t: 'reports', c: 'end_date', type: 'DATE' },
          { t: 'reports', c: 'format', type: 'VARCHAR(50)' },
          { t: 'reports', c: 'schedule', type: 'VARCHAR(50)' },
          { t: 'reports', c: 'status', type: 'VARCHAR(50)' },
        ];
       for (const col of manualCols) {
         try { 
           await sequelize.query(`ALTER TABLE ${col.t} ADD COLUMN ${col.c} ${col.type} NULL`); 
           console.log(`[DB] Column ${col.t}.${col.c} added successfully`);
         } catch (err) {
           if (!err.message.includes('Duplicate column')) {
             console.warn(`[DB] Column ${col.t}.${col.c} potentially exists or error: ${err.message.slice(0, 60)}`);
           }
         }
       }
    }
    console.log('Database synced. (MySQL Manual fixes applied)');
    // AUTO-SEED DEMO USERS if they don't exist (For 'Proper' Live Demo Experience)
    const bcrypt = require('bcryptjs');
    const { User, Company } = require('./models');

    // Ensure at least one company exists for demo users
    let defaultCompany = await Company.findByPk(1);
    if (!defaultCompany) {
      defaultCompany = await Company.create({
        id: 1,
        name: 'KIAAN WMS Demo',
        code: 'KIAAN',
        status: 'ACTIVE'
      });
      console.log('[SEED] Created default demo company');
    }

    const demoUsers = [
      { email: 'admin@kiaan-wms.com', password: 'Admin@123', name: 'Super Admin', role: 'super_admin' },
      { email: 'companyadmin@kiaan-wms.com', password: '123456', name: 'Company Admin', role: 'company_admin' },
      { email: 'inventorymanager@kiaan-wms.com', password: '123456', name: 'Inventory Manager', role: 'inventory_manager' },
      { email: 'warehousemanager@kiaan-wms.com', password: '123456', name: 'Warehouse Manager', role: 'warehouse_manager' },
      { email: 'piker@gmail.com', password: '123456', name: 'Picker', role: 'picker' },
      { email: 'packer@gmail.com', password: '123456', name: 'Packer', role: 'packer' },
    ];
    for (const d of demoUsers) {
      const exists = await User.findOne({ where: { email: d.email } });
      if (!exists) {
        const passwordHash = await bcrypt.hash(d.password, 10);
        await User.create({
          email: d.email,
          passwordHash,
          name: d.name,
          role: d.role,
          companyId: d.role === 'super_admin' ? null : 1, // Fallback to company 1
          status: 'ACTIVE'
        });
        console.log(`[SEED] Created demo user: ${d.email}`);
      }
    }

    // Initialize Cron AFTER database sync is complete
    cronService.init();

    app.listen(PORT, () => {
      console.log(`WMS Backend running at http://localhost:${PORT}`);
      console.log('Auth: POST /auth/login | GET /auth/me (Bearer token)');
      console.log('Super Admin: /api/superadmin/companies');
      console.log('Company: /api/company/profile');
      console.log('Users: /api/users');
      console.log('Warehouses: /api/warehouses');
      console.log('Inventory: /api/inventory/products, /api/inventory/categories, /api/inventory/stock');
      console.log('Orders: /api/orders/sales, /api/orders/customers');
      console.log('Suppliers: /api/suppliers | Bundles: /api/bundles');
      console.log('Picking: /api/picking');
      console.log('Packing: /api/packing');
      console.log('Shipments: /api/shipments');
      console.log('Purchase orders: /api/purchase-orders');
      console.log('Goods receiving: /api/goods-receiving');
    });
  } catch (err) {
    console.error('Unable to start server:', err);
    
    const isConnErr = err?.code === 'ECONNREFUSED' || err?.parent?.code === 'ECONNREFUSED' || err?.code === 'ETIMEDOUT' || err?.parent?.code === 'ETIMEDOUT';
    
    if (isConnErr && (process.env.DB_DIALECT || 'sqlite') === 'mysql') {
      console.error('\n--- Database Connection Error ---');
      console.error('Details:', err.message);
      console.error('\nHow to fix:');
      console.log('1. Check if MySQL is running (Locally or on Cloud)');
      console.log('2. If you are on Railway, make sure you have:');
      console.log('   - Linked a MySQL service to this backend.');
      console.log('   - Added "DB_DIALECT=mysql" in Railway Variables.');
      console.log('   - Check if you need to use MYSQL_URL (Private Networking).');
      console.log('3. Your current DB host was:', sequelize.config.host || 'localhost');
      console.log('   (If this says "localhost" on Railway, it will NOT work!)\n');
    }
    process.exit(1);
  }
}

// Retrying server start to pick up new routes
start();
