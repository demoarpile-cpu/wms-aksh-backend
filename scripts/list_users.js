const { User } = require('../models');

async function listUsers() {
    try {
        const users = await User.findAll();
        console.log(JSON.stringify(users, null, 2));
    } catch (error) {
        console.error('Error listing users:', error);
    }
}

listUsers();
