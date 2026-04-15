const { sequelize, Company, User, Product } = require('./models');
const inventoryService = require('./services/inventoryService');

async function testIsolation() {
    let companyA, companyB, userA, userB, productA, productB;
    try {
        console.log('--- Starting Isolation Test (No Transaction) ---');

        // 1. Create Two Companies
        companyA = await Company.create({ name: 'Isolation Corp A', code: `ISO-A-${Date.now()}`, status: 'ACTIVE' });
        companyB = await Company.create({ name: 'Isolation Corp B', code: `ISO-B-${Date.now()}`, status: 'ACTIVE' });
        console.log(`Created Company A: ${companyA.id}, Company B: ${companyB.id}`);

        // 2. Create Users
        userA = await User.create({
            name: 'Admin A',
            email: `adminA_${Date.now()}@iso.com`,
            passwordHash: 'dummy',
            role: 'company_admin',
            companyId: companyA.id,
            status: 'ACTIVE'
        });

        userB = await User.create({
            name: 'Admin B',
            email: `adminB_${Date.now()}@iso.com`,
            passwordHash: 'dummy',
            role: 'company_admin',
            companyId: companyB.id,
            status: 'ACTIVE'
        });

        // 3. Create Products
        productA = await Product.create({
            name: 'Product A',
            sku: `SKU-A-${Date.now()}`,
            companyId: companyA.id,
            status: 'ACTIVE'
        });

        productB = await Product.create({
            name: 'Product B',
            sku: `SKU-B-${Date.now()}`,
            companyId: companyB.id,
            status: 'ACTIVE'
        });

        // 4. Test User A
        const reqUserA = { id: userA.id, role: 'company_admin', companyId: companyA.id };
        const listA = await inventoryService.listProducts(reqUserA);
        console.log(`User A sees ${listA.length} products.`);

        const seesOnlyA = listA.every(p => p.companyId === companyA.id);
        const seesProductA = listA.some(p => p.id === productA.id);
        const seesProductB = listA.some(p => p.id === productB.id);

        if (seesOnlyA && seesProductA && !seesProductB) {
            console.log('PASS: User A sees only Company A products.');
        } else {
            console.error('FAIL: User A sees mixed data!', { matchesCompany: seesOnlyA, seesOwn: seesProductA, seesOther: seesProductB });
        }

        // 5. Test User B
        const reqUserB = { id: userB.id, role: 'company_admin', companyId: companyB.id };
        const listB = await inventoryService.listProducts(reqUserB);
        console.log(`User B sees ${listB.length} products.`);

        const seesOnlyB = listB.every(p => p.companyId === companyB.id);
        const seesProductA_B = listB.some(p => p.id === productA.id);
        const seesProductB_B = listB.some(p => p.id === productB.id);

        if (seesOnlyB && seesProductB_B && !seesProductA_B) {
            console.log('PASS: User B sees only Company B products.');
        } else {
            console.error('FAIL: User B sees mixed data!', { matchesCompany: seesOnlyB, seesOwn: seesProductB_B, seesOther: seesProductA_B });
        }

        // 6. Test User with NO Company ID (Simulate Auth Failure)

        const reqUserNull = { id: userA.id, role: 'company_admin', companyId: undefined };
        try {
            console.log('Testing undefined companyId...');
            const listNull = await inventoryService.listProducts(reqUserNull);
            console.log(`User Null sees ${listNull.length} products.`);
            // If it sees ANY products, that is a potential leak if companyId is required.
            // But inventoryService throws 'Company required' usually? 
            // No, listProducts just sets where.companyId = undefined which might be ignored.
            if (listNull.length > 0) {
                console.warn('WARN: User with undefined companyId saw data! This confirms the leak if Auth fails.');
            } else {
                console.log('PASS: User with undefined companyId saw no data (correct).');
            }
        } catch (e) {
            console.log('PASS: User with undefined companyId caused error:', e.message);
        }


    } catch (err) {
        console.error('Test Failed:', err);
    } finally {
        // Cleanup
        console.log('--- Cleanup ---');
        if (productA) await productA.destroy();
        if (productB) await productB.destroy();
        if (userA) await userA.destroy();
        if (userB) await userB.destroy();
        if (companyA) await companyA.destroy();
        if (companyB) await companyB.destroy();
    }
}

testIsolation();
