// node examples/indexes/index.mjs
import { fileURLToPath } from 'node:url';
import { MostikClient } from 'mostik';

const client = await MostikClient.connect(fileURLToPath(new URL('./data', import.meta.url)));
const users = client.db('app').collection('users');

for (let i = 0; i < 50_000; i++) users.insertOne({ name: `user${i}`, email: `user${i}@example.com` });
await users.insertOne({ name: 'last', email: 'last@example.com' });

const time = async (label, query) => {
	const start = performance.now();
	const doc = await users.findOne(query);
	console.log(label, doc?.name, (performance.now() - start).toFixed(3), 'ms');
};

await time('without index:', { email: 'user49999@example.com' }); // scans the collection
await users.createIndex({ email: 1 }); // fills the index from existing documents
await time('with index:   ', { email: 'user49999@example.com' });

await client.close();
