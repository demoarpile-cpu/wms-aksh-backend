const axios = require('axios');
const jwt = require('jsonwebtoken');

const secret = 'your-super-secret-jwt-key-change-in-production';
const token = jwt.sign({ userId: 1, role: 'super_admin', companyId: 1 }, secret, { expiresIn: '1h' });

async function testColor() {
    try {
        const sku = `TEST-${Date.now()}`;
        console.log(`Testing with SKU: ${sku}`);

        // Create Product
        const payload = {
            name: 'Color Test Product',
            sku: sku,
            color: 'Blue',
            companyId: 1,
            categoryId: 1 // Assuming category 1 exists
        };

        console.log('Sending Payload:', payload);

        const res = await axios.post('http://localhost:3001/api/inventory/products', payload, {
            headers: { Authorization: `Bearer ${token}` }
        });

        console.log('Create Response:', res.data);

        if (res.data.data.color === 'Blue') {
            console.log('SUCCESS: Color saved correctly!');
        } else {
            console.log('FAILURE: Color is ' + res.data.data.color);
        }

    } catch (err) {
        console.error('Error:', err.response ? err.response.data : err.message);
    }
}

testColor();
