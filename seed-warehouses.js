const { Warehouse } = require('./models');

async function seed() {
  try {
    const warehouses = [
      { companyId: 1, name: 'FFD Warehouse', code: 'FFD-WH-01', warehouseType: 'INTERNAL', status: 'ACTIVE' },
      { companyId: 1, name: '3PL Warehouse', code: '3PL-WH-01', warehouseType: 'THIRD_PARTY', status: 'ACTIVE' },
      { companyId: 1, name: 'Amazon Prep Warehouse', code: 'AMZ-PREP-01', warehouseType: 'INTERNAL', status: 'ACTIVE' }
    ];

    for (const wh of warehouses) {
      const [record, created] = await Warehouse.findOrCreate({
        where: { name: wh.name, companyId: wh.companyId },
        defaults: wh
      });
      if (created) {
        console.log(`Created warehouse: ${wh.name}`);
      } else {
        console.log(`Warehouse already exists: ${wh.name}`);
      }
    }
    process.exit(0);
  } catch (err) {
    console.error('Error seeding warehouses:', err);
    process.exit(1);
  }
}

seed();
