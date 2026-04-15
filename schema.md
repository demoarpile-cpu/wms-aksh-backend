# WMS Database Schema Reference

## Tables Overview

| Table | Description |
|-------|-------------|
| `companies` | Companies (Super Admin creates; Company Admin manages) |
| `warehouses` | Warehouses per company |
| `users` | Users: super_admin, company_admin, warehouse_manager, inventory_manager, picker, packer, viewer |
| `zones` | Zones per warehouse |
| `locations` | Locations (bins/shelves) per zone |
| `categories` | Product categories per company |
| `products` | Products per company |
| `product_stocks` | Stock: product + warehouse + location, quantity, reserved |
| `customers` | Customers per company |
| `sales_orders` | Sales orders (Company Admin creates) |
| `order_items` | Line items of sales order |
| `pick_lists` | Pick list per order (warehouse, assigned_to = picker) |
| `pick_list_items` | product, quantity_required, quantity_picked |
| `packing_tasks` | Packing task per order (assigned_to = packer) |
| `shipments` | Shipment: Sales Order ID, Company ID, Packed By, Courier, Tracking, Weight, Dispatch Date, Delivery Status |

---

## Column Details

### companies
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| name | VARCHAR(255) | |
| code | VARCHAR(100) UNIQUE | |
| email, phone, address | | |
| status | VARCHAR(20) | ACTIVE, INACTIVE |
| created_at, updated_at | DATETIME | |

### users
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| email | VARCHAR(255) UNIQUE | |
| password_hash | VARCHAR(255) | |
| name | VARCHAR(255) | |
| role | VARCHAR(50) | super_admin, company_admin, warehouse_manager, inventory_manager, picker, packer, viewer |
| company_id | CHAR(36) FK | → companies.id |
| warehouse_id | CHAR(36) FK | → warehouses.id |
| status | VARCHAR(20) | ACTIVE, SUSPENDED |
| created_at, updated_at | DATETIME | |

### warehouses
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| company_id | CHAR(36) FK | → companies.id |
| name | VARCHAR(255) | |
| code | VARCHAR(100) | |
| address | TEXT | |
| status | VARCHAR(20) | ACTIVE |
| created_at, updated_at | DATETIME | |

### zones
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| warehouse_id | CHAR(36) FK | → warehouses.id |
| name | VARCHAR(255) | |
| code | VARCHAR(100) | |
| created_at, updated_at | DATETIME | |

### locations
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| zone_id | CHAR(36) FK | → zones.id |
| name | VARCHAR(255) | |
| code | VARCHAR(100) | |
| created_at, updated_at | DATETIME | |

### categories
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| company_id | CHAR(36) FK | → companies.id |
| name | VARCHAR(255) | |
| code | VARCHAR(100) | |
| created_at, updated_at | DATETIME | |

### products
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| company_id | CHAR(36) FK | → companies.id |
| category_id | CHAR(36) FK | → categories.id |
| name | VARCHAR(255) | |
| sku | VARCHAR(100) | |
| barcode | VARCHAR(100) | |
| price | DECIMAL(12,2) | |
| reorder_level | INT | |
| status | VARCHAR(20) | ACTIVE |
| created_at, updated_at | DATETIME | |

### product_stocks
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| product_id | CHAR(36) FK | → products.id |
| warehouse_id | CHAR(36) FK | → warehouses.id |
| location_id | CHAR(36) FK | → locations.id |
| quantity | INT | |
| reserved | INT | |
| created_at, updated_at | DATETIME | |

### customers
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| company_id | CHAR(36) FK | → companies.id |
| name | VARCHAR(255) | |
| email | VARCHAR(255) | |
| phone | VARCHAR(50) | |
| address | TEXT | |
| created_at, updated_at | DATETIME | |

### sales_orders
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| company_id | CHAR(36) FK | → companies.id |
| order_number | VARCHAR(100) | |
| customer_id | CHAR(36) FK | → customers.id |
| status | VARCHAR(30) | pending, pick_list_created, picking, packing, packed, shipped |
| total_amount | DECIMAL(12,2) | |
| created_by | CHAR(36) FK | → users.id |
| created_at, updated_at | DATETIME | |

### order_items
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| sales_order_id | CHAR(36) FK | → sales_orders.id |
| product_id | CHAR(36) FK | → products.id |
| quantity | INT | |
| unit_price | DECIMAL(12,2) | |
| created_at, updated_at | DATETIME | |

### pick_lists
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| sales_order_id | CHAR(36) FK | → sales_orders.id |
| warehouse_id | CHAR(36) FK | → warehouses.id |
| assigned_to | CHAR(36) FK | → users.id (picker) |
| status | VARCHAR(20) | pending, in_progress, completed |
| created_at, updated_at | DATETIME | |

### pick_list_items
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| pick_list_id | CHAR(36) FK | → pick_lists.id |
| product_id | CHAR(36) FK | → products.id |
| quantity_required | INT | |
| quantity_picked | INT | |
| created_at, updated_at | DATETIME | |

### packing_tasks
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | UUID |
| sales_order_id | CHAR(36) FK | → sales_orders.id |
| pick_list_id | CHAR(36) FK | → pick_lists.id |
| assigned_to | CHAR(36) FK | → users.id (packer) |
| status | VARCHAR(20) | pending, packing, completed |
| packed_at | DATETIME | |
| created_at, updated_at | DATETIME | |

### shipments
| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) PK | Shipment ID |
| sales_order_id | CHAR(36) FK | → sales_orders.id |
| company_id | CHAR(36) FK | → companies.id |
| packed_by | CHAR(36) FK | → users.id |
| courier_name | VARCHAR(255) | |
| tracking_number | VARCHAR(100) | |
| weight | DECIMAL(10,2) | |
| dispatch_date | DATE | |
| delivery_status | VARCHAR(50) | pending, etc. |
| created_at, updated_at | DATETIME | |

---

## How to use schema.sql

**MySQL / MariaDB:**
```bash
mysql -u root -p warehouse_wms < warehouse_wms.sql
```
Or create DB first: `CREATE DATABASE warehouse_wms;` then run warehouse_wms.sql.

**Note:** For SQLite (default in this project), tables are created by Sequelize when you run `npm start` or `npm run seed`. Use `schema.sql` when you want to use MySQL/MariaDB and create tables manually.
