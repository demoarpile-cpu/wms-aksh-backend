# WMS Database Setup – Step by Step

## Database name: **warehouse_wms**

- **SQLite (default):** File = `warehouse_wms.sqlite` (wmsbackend folder ke andar)
- **MySQL:** Database name = `warehouse_wms`

---

## Option A: SQLite (sabse simple – install kuch nahi)

### 1. Purani file hatao (agar pehle use ki thi)
- `wmsbackend` folder me jao
- **warehouse_wms.sqlite** ya **database.sqlite** ya **wms_db.sqlite** delete kar do (sirf tab jab data lose karna ho)

### 2. Backend start karo
```bash
cd wmsbackend
npm start
```
- Console me dikhega: **"SQLite file: C:\...\wmsbackend\warehouse_wms.sqlite"**
- Data yahi file me jayega

### 3. Super Admin banao (login ke liye)
Naya terminal me:
```bash
cd wmsbackend
node scripts/create-superadmin.js
```
- Login: **admin@kiaan-wms.com** / **Admin@123**

### 4. Data dekhna ho to
- **DB Browser for SQLite** se file open karo: `wmsbackend\warehouse_wms.sqlite`
- Ya koi bhi SQLite viewer – **same path** jo console me aaya

---

## Option B: MySQL

### 1. MySQL me database banao
```sql
CREATE DATABASE warehouse_wms;
```

### 2. wmsbackend me .env banao
```env
DB_DIALECT=mysql
DB_NAME=warehouse_wms
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
```

### 3. Tables banao
- Ya to **warehouse_wms.sql** run karo MySQL me (warehouse_wms select karke)
- Ya sirf `npm start` chalao – Sequelize tables khud bana dega

### 4. Super Admin
```bash
cd wmsbackend
node scripts/create-superadmin.js
```

---

## IDs

- Sab tables me **integer ID** (1, 2, 3, 22, 34...) – MySQL jaisa AUTO_INCREMENT

---

## Problem ho to

1. **Data dikh nahi raha:**  
   Console me jo **"SQLite file: ..."** path dikhe, wahi file open karo. Dusri file / dusra folder mat dekho.

2. **Error aata hai start pe:**  
   `warehouse_wms.sqlite` delete karo, phir dubara `npm start` + `node scripts/create-superadmin.js`.

3. **MySQL connection error:**  
   .env me DB_HOST, DB_USER, DB_PASSWORD, DB_NAME sahi check karo.
