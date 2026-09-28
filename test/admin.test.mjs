// dropIndex, listIndexes, drop, listCollections
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { createRequire } from 'node:module';
import { MostikClient, MongoServerError } from 'mostik';
import { tempDir } from './helpers.mjs';

const require = createRequire(import.meta.url);
const { encodeKey, prefixRange } = require('../lib/storage.js');

const code = (n) => (error) => error instanceof MongoServerError && error.code == n;

/**
 * Keys stored under `[kind, db, collection, ...rest]` (the collection by its id, or `id` when
 * given: a dropped collection is no longer in the catalog): what an operation left behind.
 */
function keysUnder(client, [kind, db, name, ...rest], id = client._storage().collectionId(db, name)) {
	assert.ok(id !== undefined, `${db}.${name} has an id`);
	return [...client._storage().keys(...prefixRange([encodeKey([kind, id, ...rest])]))].length;
}

test('listIndexes and dropIndex, by name and by key pattern; queries stay right', async () => {
	const dbPath = path.join(tempDir(), 'db');
	let client = await MostikClient.connect(dbPath);
	const items = client.db('app').collection('items');
	await assert.rejects(items.listIndexes().toArray(), code(26));
	await assert.rejects(items.dropIndex('g_1'), code(26));
	await items.insertMany(Array.from({ length: 3000 }, (_, i) => ({ _id: i, g: i % 10, t: `t${i % 7}`, tags: [i % 3, 5] })));
	assert.deepEqual(await items.listIndexes().toArray(), [{ v: 2, key: { _id: 1 }, name: '_id_' }]);
	assert.equal(await items.createIndex({ g: 1 }), 'g_1');
	assert.equal(await items.createIndex({ t: -1 }, { name: 'by_t' }), 'by_t');
	assert.equal(await items.createIndex({ tags: 1 }), 'tags_1'); // multikey
	assert.deepEqual(await items.listIndexes().toArray(), [
		{ v: 2, key: { _id: 1 }, name: '_id_' },
		{ v: 2, key: { g: 1 }, name: 'g_1' },
		{ v: 2, key: { t: -1 }, name: 'by_t' },
		{ v: 2, key: { tags: 1 }, name: 'tags_1' },
	]);
	await assert.rejects(items.dropIndex('_id_'), code(72));
	await assert.rejects(items.dropIndex({ _id: 1 }), code(72));
	await assert.rejects(items.dropIndex('nope'), code(27));
	await assert.rejects(items.dropIndex({ t: 1 }), code(27), 'the direction must match');
	assert.deepEqual(await items.dropIndex('g_1'), { nIndexesWas: 4, ok: 1 });
	assert.deepEqual(await items.dropIndex({ t: -1 }), { nIndexesWas: 3, ok: 1 });
	assert.equal(keysUnder(client, ['i', 'app', 'items', 'g']), 0, 'the entries are gone');
	assert.equal(keysUnder(client, ['i', 'app', 'items', 't']), 0);
	assert.deepEqual((await items.listIndexes().toArray()).map((i) => i.name), ['_id_', 'tags_1']);
	// the scan answers now
	assert.equal(await items.countDocuments({ g: 3 }), 300);
	assert.equal((await items.find({ t: 't2' }).toArray()).length, 429);
	// writes no longer touch a dropped index; the remaining one still follows them
	await items.insertOne({ _id: 'new', g: 3, tags: [9, 8] });
	await items.updateMany({ g: 3 }, { $set: { tags: [7] } });
	assert.equal(keysUnder(client, ['i', 'app', 'items', 'g']), 0);
	assert.equal(await items.countDocuments({ tags: 7 }), 301);
	assert.equal(await items.countDocuments({ tags: 5 }), 2700);
	await client.close();

	// all of it is on disk
	client = await MostikClient.connect(dbPath);
	const again = client.db('app').collection('items');
	assert.deepEqual((await again.listIndexes().toArray()).map((i) => i.name), ['_id_', 'tags_1']);
	// an index created again is filled from scratch
	await again.createIndex({ g: 1 });
	assert.equal(await again.countDocuments({ g: 3 }), 301);
	assert.equal(keysUnder(client, ['i', 'app', 'items', 'g']), 3001);
	// in the order of creation: g_1 again after tags_1
	assert.deepEqual(await again.listIndexes().toArray(), [
		{ v: 2, key: { _id: 1 }, name: '_id_' },
		{ v: 2, key: { tags: 1 }, name: 'tags_1' },
		{ v: 2, key: { g: 1 }, name: 'g_1' },
	]);
	await client.close();
});

test('index builds and drops hold writes one at a time', async () => {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		const items = client.db('app').collection('items');
		await items.insertMany(Array.from({ length: 20_000 }, (_, i) => ({ _id: i, a: i % 13, b: i % 17 })));
		// two builds at once, writes in between: every index must hold every document
		const writes = Array.from({ length: 200 }, (_, i) => items.insertOne({ _id: `w${i}`, a: i % 13, b: i % 17 }));
		await Promise.all([items.createIndex({ a: 1 }), items.createIndex({ b: 1 }), ...writes, items.dropIndex('a_1').catch(() => {})]);
		const all = await items.find({}).toArray();
		assert.equal(all.length, 20_200);
		assert.equal(keysUnder(client, ['i', 'app', 'items', 'b']), 20_200);
		const aEntries = keysUnder(client, ['i', 'app', 'items', 'a']);
		const hasA = (await items.listIndexes().toArray()).some((i) => i.name == 'a_1');
		assert.equal(aEntries, hasA ? 20_200 : 0);
		for (let v = 0; v < 17; v++) assert.equal(await items.countDocuments({ b: v }), all.filter((d) => d.b == v).length);
	} finally {
		await client.close();
	}
});

test('drop removes documents, indexes and count; other collections stay', async () => {
	const dbPath = path.join(tempDir(), 'db');
	let client = await MostikClient.connect(dbPath);
	const db = client.db('app');
	const items = db.collection('items');
	const other = db.collection('items2');
	assert.equal(await items.drop(), false, 'nothing to drop');
	await items.insertMany(Array.from({ length: 5000 }, (_, i) => ({ _id: i, g: i % 10 })));
	await items.createIndex({ g: 1 });
	await other.insertOne({ _id: 1, g: 1 });
	await other.createIndex({ g: 1 });
	const id = client._storage().collectionId('app', 'items');
	assert.ok(keysUnder(client, ['d', 'app', 'items'], id) > 0);
	assert.equal(await items.drop(), true);
	assert.equal(await items.drop(), false);
	for (const kind of ['d', 'i', 'x', 'n']) assert.equal(keysUnder(client, [kind, 'app', 'items'], id), 0, `nothing left under ${kind}`);
	assert.equal(client._storage().collectionId('app', 'items'), undefined, 'gone from the catalog');
	assert.deepEqual(await items.find({}).toArray(), []);
	assert.equal(await items.countDocuments({}), 0);
	assert.equal(await items.countDocuments({ g: 1 }), 0);
	await assert.rejects(items.listIndexes().toArray(), code(26));
	assert.equal(await other.countDocuments({ g: 1 }), 1);
	assert.deepEqual((await other.listIndexes().toArray()).map((i) => i.name), ['_id_', 'g_1']);
	// the name can be used again, starting empty and without the old indexes
	await items.insertOne({ _id: 'x', g: 1 });
	assert.deepEqual((await items.listIndexes().toArray()).map((i) => i.name), ['_id_']);
	assert.equal(keysUnder(client, ['i', 'app', 'items']), 0);
	assert.equal(await items.countDocuments({}), 1);
	await client.close();
	client = await MostikClient.connect(dbPath);
	assert.deepEqual(await client.db('app').collection('items').find({}).toArray(), [{ _id: 'x', g: 1 }]);
	await client.close();
});

test('listCollections: collections with documents or indexes, by name', async () => {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		const db = client.db('app');
		assert.deepEqual(await db.listCollections().toArray(), []);
		await db.collection('users').insertOne({ name: 'a' });
		await db.collection('logs').insertOne({ at: 1 });
		await db.collection('empty').createIndex({ x: 1 }); // an index makes it exist, as in MongoDB
		await db.collection('cleared').insertOne({ a: 1 });
		await db.collection('cleared').deleteMany({}); // still exists, with no documents
		await client.db('other').collection('elsewhere').insertOne({ a: 1 });
		await client.db('app2').collection('near').insertOne({ a: 1 });
		assert.deepEqual(await db.listCollections({}, { nameOnly: true }).toArray(), [
			{ name: 'cleared', type: 'collection' },
			{ name: 'empty', type: 'collection' },
			{ name: 'logs', type: 'collection' },
			{ name: 'users', type: 'collection' },
		]);
		assert.deepEqual(await db.listCollections({ name: 'users' }).toArray(), [
			{ name: 'users', type: 'collection', options: {}, info: { readOnly: false }, idIndex: { v: 2, key: { _id: 1 }, name: '_id_' } },
		]);
		assert.deepEqual((await db.listCollections({ name: { $in: ['logs', 'nope'] } }).toArray()).map((c) => c.name), ['logs']);
		await db.collection('logs').drop();
		assert.deepEqual((await db.listCollections().toArray()).map((c) => c.name), ['cleared', 'empty', 'users']);
	} finally {
		await client.close();
	}
});

test('renameCollection moves documents, indexes and count to the new name at once', async () => {
	const dbPath = path.join(tempDir(), 'db');
	let client = await MostikClient.connect(dbPath);
	let db = client.db('app');
	const old = db.collection('old');
	await old.createIndex({ g: 1 });
	await old.insertMany(Array.from({ length: 3000 }, (_, i) => ({ _id: i, g: i % 10 })));
	// writes queued before the rename go with it
	const late = old.insertOne({ _id: 'late', g: 3 });
	const renamed = await db.renameCollection('old', 'new');
	await late;
	assert.equal(renamed.collectionName, 'new');
	assert.equal(await renamed.countDocuments({}), 3001);
	assert.equal(await renamed.countDocuments({ g: 3 }), 301, 'by the index');
	assert.deepEqual((await renamed.listIndexes().toArray()).map((i) => i.name), ['_id_', 'g_1']);
	assert.deepEqual(await old.find({}).toArray(), [], 'the old name holds nothing');
	await assert.rejects(old.listIndexes().toArray(), code(26));
	assert.deepEqual((await db.listCollections({}, { nameOnly: true }).toArray()).map((c) => c.name), ['new']);
	// the old name is free again: a new, empty collection
	await old.insertOne({ _id: 1, fresh: true });
	assert.deepEqual(await old.find({}).toArray(), [{ _id: 1, fresh: true }]);
	assert.equal(await renamed.countDocuments({}), 3001);

	await assert.rejects(db.renameCollection('new', 'new'), code(20));
	await assert.rejects(db.renameCollection('missing', 'x'), code(26));
	await assert.rejects(db.renameCollection('new', 'old'), code(48));
	const replaced = await db.renameCollection('new', 'old', { dropTarget: true });
	assert.equal(await replaced.countDocuments({}), 3001);
	assert.deepEqual((await db.listCollections({}, { nameOnly: true }).toArray()).map((c) => c.name), ['old']);
	await client.close();

	client = await MostikClient.connect(dbPath);
	db = client.db('app');
	const again = db.collection('old');
	assert.equal(await again.countDocuments({ g: 3 }), 301);
	assert.deepEqual((await again.listIndexes().toArray()).map((i) => i.name), ['_id_', 'g_1']);
	assert.deepEqual((await db.listCollections({}, { nameOnly: true }).toArray()).map((c) => c.name), ['old']);
	await client.close();
});

test('dropDatabase drops every collection of that database only', async () => {
	const dbPath = path.join(tempDir(), 'db');
	let client = await MostikClient.connect(dbPath);
	const app = client.db('app');
	for (const name of ['a', 'b', 'c']) {
		await app.collection(name).insertMany(Array.from({ length: 500 }, (_, i) => ({ _id: i, g: i % 5 })));
		await app.collection(name).createIndex({ g: 1 });
	}
	await client.db('app2').collection('a').insertOne({ _id: 1 });
	await client.db('other').collection('a').insertOne({ _id: 1 });
	const ids = ['a', 'b', 'c'].map((name) => client._storage().collectionId('app', name));
	assert.equal(await app.dropDatabase(), true);
	assert.deepEqual(await app.listCollections().toArray(), []);
	for (const id of ids) for (const kind of ['d', 'i', 'x', 'n']) assert.equal(keysUnder(client, [kind, 'app', '?'], id), 0);
	assert.equal(await client.db('app2').collection('a').countDocuments({}), 1);
	assert.equal(await client.db('other').collection('a').countDocuments({}), 1);
	assert.equal(await app.dropDatabase(), true, 'nothing left to drop');
	await app.collection('a').insertOne({ _id: 'again' });
	await client.close();
	client = await MostikClient.connect(dbPath);
	assert.deepEqual((await client.db('app').listCollections({}, { nameOnly: true }).toArray()).map((c) => c.name), ['a']);
	assert.deepEqual(await client.db('app').collection('a').find({}).toArray(), [{ _id: 'again' }]);
	await client.close();
});
