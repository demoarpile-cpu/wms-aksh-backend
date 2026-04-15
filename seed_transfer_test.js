const { Warehouse, Zone, Location, Product, ProductStock, Company, Customer } = require('./models');

async function seed() {
  try {
    console.log('--- Seeding Data for Transfer Test ---');

    // 1. Ensure a Company exists
    const [company] = await Company.findOrCreate({
      where: { id: 1 },
      defaults: { name: 'Test Corp', code: 'TCORP' }
    });
    console.log('Company check done.');

    // 2. Warehouses
    const wh1 = await Warehouse.create({ companyId: company.id, name: 'Main Warehouse', code: 'WH-MAIN', status: 'ACTIVE' });
    const wh2 = await Warehouse.create({ companyId: company.id, name: 'Secondary Warehouse', code: 'WH-SEC', status: 'ACTIVE' });
    console.log('Warehouses created.');

    // 3. Zones
    const zone1 = await Zone.create({ warehouseId: wh1.id, name: 'Pick Zone A', code: 'ZA' });
    const zone2 = await Zone.create({ warehouseId: wh2.id, name: 'Receiving Zone B', code: 'ZB' });
    console.log('Zones created.');

    // 4. Locations (Bins)
    const bin1A = await Location.create({ zoneId: zone1.id, name: 'A-01-01', code: 'A0101' });
    const bin1B = await Location.create({ zoneId: zone1.id, name: 'A-01-02', code: 'A0102' });
    const bin2A = await Location.create({ zoneId: zone2.id, name: 'B-01-01', code: 'B0101' });
    console.log('Locations (Bins) created.');

    // 5. Product
    const product = await Product.create({
      companyId: company.id,
      name: 'Sample Widget',
      sku: 'WIDGET-001',
      price: 10,
      status: 'ACTIVE'
    });
    console.log('Product created.');

    // 6. Client (Customer)
    const client = await Customer.create({
        companyId: company.id,
        name: 'Default Client',
        email: 'client@example.com'
    });
    console.log('Client created.');

    // 7. Initial Stock in WH1
    await ProductStock.create({
      productId: product.id,
      warehouseId: wh1.id,
      locationId: bin1A.id,
      clientId: client.id,
      quantity: 100,
      status: 'ACTIVE'
    });
    console.log('Stock added to WH1 Bin A-01-01.');

    console.log('Seeding completed successfully!');
    process.exit(0);
  } catch (err) {
    console.error('Seeding failed:', err);
    process.exit(1);
  }
}

seed();
