// node examples/commonjs/index.js
const path = require('path');
const { MostikClient, MongoServerError } = require('mostik');

async function main() {
	const client = await MostikClient.connect(path.join(__dirname, 'data'));
	const products = client.db('shop').collection('products');

	// your own _id instead of a generated ObjectId
	await products.insertOne({ _id: 'sku-1', title: 'Mug', tags: ['kitchen', 'gift'] });
	console.log(await products.findOne({ tags: 'gift' })); // arrays match any element

	try {
		await products.insertOne({ _id: 'sku-1', title: 'Another mug' });
	} catch (error) {
		if (error instanceof MongoServerError && error.code === 11000) console.log('duplicate _id');
		else throw error;
	}

	await client.close();
}

main();
