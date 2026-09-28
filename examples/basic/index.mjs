// node examples/basic/index.mjs
import { fileURLToPath } from 'node:url';
import { MostikClient } from 'mostik';

const client = new MostikClient(fileURLToPath(new URL('./data', import.meta.url)));
await client.connect();

const users = client.db('app').collection('users');

const { insertedId } = await users.insertOne({ name: 'Ann', email: 'ann@example.com', address: { city: 'Riga' } });
console.log(insertedId); // new ObjectId('...')

console.log(await users.findOne({ _id: insertedId })); // { _id: ..., name: 'Ann', ... }
console.log(await users.findOne({ 'address.city': 'Riga' })); // the same document
console.log(await users.findOne({ name: 'Bob' })); // null

await client.close();
