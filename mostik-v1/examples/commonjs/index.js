// node examples/commonjs/index.js
const path = require('path');
const { open } = require('mostik');

async function main() {
	const db = open(path.join(__dirname, 'my-db'));

	await db.put(['user', 1], { name: 'Ann', roles: ['admin'] });
	await db.put(['user', 2], { name: 'Bob', roles: [] });

	console.log(db.get(['user', 1]).name); // 'Ann'
	console.log(db.get(['user', 3])); // undefined

	await db.close();
}

main();
