# WMS Backend

Backend for Kiaan WMS with role hierarchy: **Super Admin → Company → Company Admin → Staff (Warehouse Manager, Inventory Manager, Picker, Packer, Viewer)**.

## Structure

- **config/db.js** – Sequelize connection (SQLite default, MySQL optional)
- **middlewares/auth.js** – JWT auth + role checks
- **models/** – User, Company, Warehouse, Zone, Location, Category, Product, ProductStock, Customer, SalesOrder, OrderItem, PickList, PickListItem, PackingTask, Shipment
- **modules/** – superadmin, company, users, inventory, orders, picking, packing, shipment
- **routes/** – auth, warehouses
- **controllers/** – per entity
- **services/** – business logic
- **server.js** – Express app + DB sync

## Database name: **warehouse_wms**

- **SQLite (default):** File = `wmsbackend/warehouse_wms.sqlite` – data yahi save hota hai
- **MySQL:** Database name = `warehouse_wms`

Full steps: **SETUP-DATABASE.md** dekho.

## Setup (quick)

```bash
cd wmsbackend
npm install
npm start
```
Start hone ke baad console me **"SQLite file: ...warehouse_wms.sqlite"** dikhega – yahi file me data jayega.

Super Admin (login ke liye):
```bash
node scripts/create-superadmin.js
```
Login: **admin@kiaan-wms.com** / **Admin@123**

Default: **SQLite** (`warehouse_wms.sqlite`). MySQL ke liye `.env` me `DB_DIALECT=mysql`, `DB_NAME=warehouse_wms` set karo.

## Seed

- **Super Admin:** `admin@kiaan-wms.com` / `Admin@123`
- **Company Admin:** `companyadmin@kiaan-wms.com` / `Admin@123` (Demo Company)
- **Staff:** `warehousemanager@kiaan-wms.com`, `inventorymanager@kiaan-wms.com`, `picker@kiaan-wms.com`, `packer@kiaan-wms.com`, `viewer@kiaan-wms.com` / `Admin@123`

Reset DB and re-seed: `npm run seed -- --reset`

## API (base URL: `http://localhost:3001`)

| Method | Endpoint | Auth | Description |
|--------|----------|------|--------------|
| POST | `/auth/login` | No | Body: `{ email, password }` → `{ user, token }` |
| GET | `/auth/me` | Bearer | Current user |
| GET/POST/PUT/DELETE | `/api/superadmin/companies` | Super Admin | Companies CRUD |
| GET/PUT | `/api/company/profile` | Company Admin | Company profile |
| GET/POST/PUT/DELETE | `/api/users` | Super Admin / Company Admin | Users CRUD |
| GET/POST/PUT/DELETE | `/api/warehouses` | Company Admin+ | Warehouses CRUD |
| GET/POST/PUT | `/api/inventory/products`, `/api/inventory/categories`, `/api/inventory/stock` | Inventory Manager+ | Products, categories, stock |
| GET/POST | `/api/orders/sales` | Company Admin+ | Sales orders (create order → auto Pick List) |
| GET/POST/PUT/DELETE | `/api/orders/customers` | Company Admin+ | Customers CRUD |
| GET/POST | `/api/picking`, `/api/picking/:id/assign`, `/api/picking/:id/start`, `/api/picking/:id/complete` | Picker / Warehouse Manager | Pick lists |
| GET/POST | `/api/packing`, `/api/packing/:id/assign`, `/api/packing/:id/complete` | Packer / Warehouse Manager | Packing tasks |
| GET/POST/PUT | `/api/shipments` | Packer+ | Shipments (Shipment ID, Sales Order ID, Company ID, Packed By, Courier Name, Tracking Number, Weight, Dispatch Date, Delivery Status) |

All protected routes need header: `Authorization: Bearer <token>`.

## Order flow

1. **Company Admin** creates Sales Order → system creates **Pick List** and **Packing Task**.
2. **Warehouse Manager** assigns **Picker** to pick list.
3. **Picker** starts picking, updates quantities, completes picking → order moves to packing.
4. **Warehouse Manager** assigns **Packer** to packing task.
5. **Packer** completes packing → order status `packed`.
6. Create **Shipment** (courier, tracking, dispatch date, delivery status) → order status `shipped`.
