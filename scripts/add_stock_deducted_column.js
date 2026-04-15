const { Sequelize } = require('sequelize');
require('dotenv').config();

const sequelize = new Sequelize(
    process.env.DB_NAME,
    process.env.DB_USER,
    process.env.DB_PASSWORD,
    {
        host: process.env.DB_HOST,
        dialect: process.env.DB_DIALECT || 'mysql',
        logging: console.log,
    }
);

async function addColumn() {
    try {
        await sequelize.authenticate();
        console.log('Database connected.');

        const queryInterface = sequelize.getQueryInterface();
        const tableInfo = await queryInterface.describeTable('shipments');

        if (!tableInfo.stock_deducted) {
            console.log('Adding stock_deducted column...');
            await queryInterface.addColumn('shipments', 'stock_deducted', {
                type: Sequelize.BOOLEAN,
                defaultValue: false,
                allowNull: false,
            });
            console.log('Column stock_deducted added successfully.');
        } else {
            console.log('Column stock_deducted already exists.');
        }

        process.exit(0);
    } catch (error) {
        console.error('Error adding column:', error);
        process.exit(1);
    }
}

addColumn();
