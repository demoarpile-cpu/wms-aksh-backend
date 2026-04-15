require('dotenv').config();
const path = require('path');
const { Sequelize } = require('sequelize');

// Database name: warehouse_wms (MySQL ya SQLite file)
const dialect = process.env.DB_DIALECT || 'sqlite';
let sequelize;

if (process.env.MYSQL_URL || process.env.DATABASE_URL) {
  // Priority 1: Use connection string (like mysql://user:pass@host:port/database)
  sequelize = new Sequelize(process.env.MYSQL_URL || process.env.DATABASE_URL, {
    dialect: 'mysql',
    logging: process.env.NODE_ENV === 'development' ? console.log : false,
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000
    }
  });
} else if (dialect === 'mysql') {
  // Priority 2: Use individual variables, with support for Railway naming standards
  const dbName = process.env.DB_NAME || process.env.MYSQLDATABASE || 'warehouse_wms';
  const dbUser = process.env.DB_USER || process.env.MYSQLUSER || 'root';
  const dbPass = process.env.DB_PASSWORD || process.env.MYSQLPASSWORD || '';
  const dbHost = process.env.DB_HOST || process.env.MYSQLHOST || 'localhost';
  const dbPort = process.env.DB_PORT || process.env.MYSQLPORT || 3306;

  sequelize = new Sequelize(dbName, dbUser, dbPass, {
    host: dbHost,
    port: dbPort,
    dialect: 'mysql',
    logging: process.env.NODE_ENV === 'development' ? console.log : false,
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000
    }
  });
} else {
  // Priority 3: SQLite (default)
  const sqlitePath = process.env.DB_STORAGE || path.join(__dirname, '..', 'warehouse_wms.sqlite');
  sequelize = new Sequelize({
    dialect: 'sqlite',
    storage: sqlitePath,
    logging: process.env.NODE_ENV === 'development' ? console.log : false,
  });
}

module.exports = { sequelize, Sequelize };
