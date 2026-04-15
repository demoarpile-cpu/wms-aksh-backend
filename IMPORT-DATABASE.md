# Database: warehouse_wms – Import kaise karein

## Database name: **warehouse_wms**

- **MySQL:** database name = `warehouse_wms`
- **SQLite:** file name = `warehouse_wms.sqlite` (wmsbackend folder me)

Sab tables me **id = 1, 2, 3...** (integer), badi UUID nahi.

---

## Option 1: MySQL – Import from file

### 1. MySQL me database banao
```sql
CREATE DATABASE warehouse_wms;
```

### 2. File import karo
- **File:** `wmsbackend/warehouse_wms.sql`
- MySQL Workbench / phpMyAdmin / command line se run karo:

**Command line:**
```bash
mysql -u root -p warehouse_wms < wmsbackend/warehouse_wms.sql
```

**Ya** file me pehle hi `CREATE DATABASE IF NOT EXISTS warehouse_wms; USE warehouse_wms;` hai – to seedha file run karo, database + tables dono ban jayenge.

### 3. Backend .env me set karo
```env
DB_DIALECT=mysql
DB_NAME=warehouse_wms
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
```

### 4. Backend start karo
```bash
cd wmsbackend
npm start
```

Super Admin already **warehouse_wms.sql** me insert hai: **admin@kiaan-wms.com** / **Admin@123**

---

## Option 2: SQLite (bina MySQL)

### 1. Purani DB files hatao
`wmsbackend` folder me se delete karo (agar hai to):
- `wms_db.sqlite`
- `database.sqlite`
- `kiaan_wms.sqlite`

### 2. Backend start karo
```bash
cd wmsbackend
npm start
```
- Nayi file **warehouse_wms.sqlite** ban jayegi (integer IDs ke sath).
- Console me path dikhega: `...wmsbackend/warehouse_wms.sqlite`

### 3. Super Admin banao
```bash
node scripts/create-superadmin.js
```
Login: **admin@kiaan-wms.com** / **Admin@123**

---

## Data / ID check

- API response me **id** ab **number** aana chahiye: `"id": 1`, `"companyId": 2`
- MySQL ya SQLite me table open karke bhi dekh sakte ho – **id** column 1, 2, 3...
