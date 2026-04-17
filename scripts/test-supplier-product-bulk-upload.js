/**
 * End-to-end check: login → pick first supplier + product → POST CSV bulk-upload.
 *
 * Usage (PowerShell):
 *   cd Wms-backend
 *   $env:WMS_TEST_EMAIL="you@company.com"; $env:WMS_TEST_PASSWORD="yourpass"; node scripts/test-supplier-product-bulk-upload.js
 *
 * Or:
 *   node scripts/test-supplier-product-bulk-upload.js you@company.com yourpass
 *
 * Optional: $env:TEST_API_BASE="http://127.0.0.1:3001"
 */
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const BASE = process.env.TEST_API_BASE || 'http://127.0.0.1:3001';

async function main() {
  const email = process.env.WMS_TEST_EMAIL || process.argv[2];
  const password = process.env.WMS_TEST_PASSWORD || process.argv[3];

  if (!email || !password) {
    console.error('Missing credentials.');
    console.error(
      '  PowerShell: $env:WMS_TEST_EMAIL="..."; $env:WMS_TEST_PASSWORD="..."; node scripts/test-supplier-product-bulk-upload.js'
    );
    console.error('  Or: node scripts/test-supplier-product-bulk-upload.js email@x.com password');
    process.exit(1);
  }

  const loginRes = await fetch(`${BASE}/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  const loginJson = await loginRes.json().catch(() => ({}));
  if (!loginRes.ok || !loginJson.token) {
    console.error('Login failed:', loginRes.status, loginJson);
    process.exit(1);
  }
  const token = loginJson.token;
  const auth = { Authorization: `Bearer ${token}` };

  const [supRes, prodRes] = await Promise.all([
    fetch(`${BASE}/api/suppliers`, { headers: auth }),
    fetch(`${BASE}/api/inventory/products?limit=5`, { headers: auth }),
  ]);
  const supJson = await supRes.json();
  const prodJson = await prodRes.json();
  const suppliers = Array.isArray(supJson.data) ? supJson.data : [];
  const products = Array.isArray(prodJson.data) ? prodJson.data : [];

  if (!suppliers.length) {
    console.error('No suppliers for this company. Add one under Suppliers, then re-run.');
    process.exit(1);
  }
  if (!products.length) {
    console.error('No products for this company. Add products first, then re-run.');
    process.exit(1);
  }

  const s = suppliers[0];
  const p = products[0];
  const supplierId = s.id;
  const productId = p.id;
  const sku = p.sku || '';
  if (!sku) {
    console.error('First product has no SKU; pick another product in DB.');
    process.exit(1);
  }

  const supplierSku = `BULK-TEST-${Date.now()}`;
  const header = 'supplier_id,product_id,internal_sku,supplier_sku,cost_price\n';
  const row = `${supplierId},${productId},${csvEscape(sku)},${csvEscape(supplierSku)},1.00\n`;
  const csv = header + row;

  const form = new FormData();
  form.append('file', new Blob([csv], { type: 'text/csv;charset=utf-8' }), 'bulk_test.csv');

  const upRes = await fetch(`${BASE}/api/supplier-products/bulk-upload?chunkSize=100`, {
    method: 'POST',
    headers: auth,
    body: form,
  });
  const upJson = await upRes.json().catch(() => ({}));
  console.log('POST /api/supplier-products/bulk-upload →', upRes.status);
  console.log(JSON.stringify(upJson, null, 2));

  if (!upRes.ok || !upJson.success) {
    process.exit(1);
  }
  const r = upJson.results || {};
  const ok = (r.created || 0) + (r.updated || 0) > 0;
  if (!ok && (r.errors || []).length) {
    console.error('No rows saved. See errors above.');
    process.exit(1);
  }
  console.log('\nOK: bulk upload path works (created/updated > 0 or duplicate updated).');
}

function csvEscape(v) {
  const s = v == null ? '' : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
