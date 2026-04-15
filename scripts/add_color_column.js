const { sequelize } = require('../models');

async function addColorColumn() {
    try {
        await sequelize.authenticate();
        console.log('Connected to database.');

        const [results] = await sequelize.query("SHOW COLUMNS FROM products LIKE 'color'");
        if (results.length > 0) {
            console.log('Column "color" already exists in "products" table.');
        } else {
            console.log('Column "color" does not exist. Adding it...');
            await sequelize.query("ALTER TABLE products ADD COLUMN color VARCHAR(255) NULL AFTER description");
            console.log('Column "color" added successfully.');
        }
    } catch (error) {
        console.error('Error adding column:', error);
    } finally {
        await sequelize.close();
    }
}

addColorColumn();
