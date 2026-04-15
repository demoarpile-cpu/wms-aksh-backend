const { sequelize } = require('./models');
const { DataTypes } = require('sequelize');

async function fix() {
  try {
    console.log('Connecting to database...');
    await sequelize.authenticate();
    const qi = sequelize.getQueryInterface();
    
    console.log('Checking zones table...');
    const tableInfo = await qi.describeTable('zones');
    
    if (!tableInfo.company_id && !tableInfo.companyId) {
      console.log('Adding column company_id to zones table...');
      try {
        await qi.addColumn('zones', 'company_id', {
          type: DataTypes.INTEGER,
          allowNull: true
        });
        console.log('Column company_id added!');
      } catch (e) {
        console.log('Note (company_id):', e.message);
      }
      
      try {
        await qi.addColumn('zones', 'companyId', {
          type: DataTypes.INTEGER,
          allowNull: true
        });
        console.log('Column companyId added!');
      } catch (e) {
        console.log('Note (companyId):', e.message);
      }
    } else {
      console.log('One of the columns already exists!');
    }
    
    // NEW: Populate NULL companyIds so they show up for the user
    console.log('Populating NULL companyIds in zones table...');
    await sequelize.query('UPDATE zones SET companyId = 1, company_id = 1 WHERE companyId IS NULL OR company_id IS NULL');
    console.log('Population DONE!');
    
    console.log('--- ALL DONE ---');
    process.exit(0);
  } catch (err) {
    console.error('Error fixing database:', err);
    process.exit(1);
  }
}

fix();
