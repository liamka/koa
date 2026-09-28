// find, insertMany, deleteOne, deleteMany, countDocuments
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { createRequire } from 'node:module';
import { MostikClient, ObjectId, MongoBulkWriteError, FindCursor } from 'mostik';
import { tempDir } from './helpers.mjs';

const require = createRequire(import.meta.url);
const { encodeKey, prefixRange } = require('../lib/storage.js');

async function withCollection(fn, options) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'), options);
	try {
		await fn(client.db('app').collection('items'), client);
	} finally {
		await client.close();
	}
}

/**
 * The collection agrees with itself: counter = documents, no index entry without its document,
 * and for every indexed field and value the index answers exactly like a full scan.
 */
async function assertConsistent(collection, client, indexedFields) {
	const all = await collection.find({}).toArray();
	assert.equal(await collection.countDocuments({}), all.length, 'counter matches the documents');
	const storage = client._storage();
	const id = storage.collectionId(collection.dbName, collection.collectionName);
	assert.ok(id !== undefined, 'the collection is in the catalog');
	const entries = prefixRange([encodeKey(['i', id])]);
	const docs = encodeKey(['d', id]);
	assert.equal(storage.countRange(...entries, undefined, 0), storage.countRange(...entries, docs, 0), 'no dangling index entries');
	for (const field of indexedFields) {
		const values = new Set(all.map((doc) => doc[field]).filter((v) => v !== undefined));
		for (const value of values) {
			const expected = all.filter((doc) => doc[field] === value).map((doc) => doc._id);
			const viaIndex = (await collection.find({ [field]: value }).toArray()).map((doc) => doc._id);
			assert.deepEqual(viaIndex.sort(), expected.sort(), `index ${field} = ${value}`);
			assert.equal(await collection.countDocuments({ [field]: value }), expected.length, `count ${field} = ${value}`);
		}
	}
}

test('insertMany: result shape, generated ids, one commit for many documents', async () => {
	await withCollection(async (items, client) => {
		const docs = [{ n: 1 }, { _id: 'custom', n: 2 }, { n: 3 }];
		const result = await items.insertMany(docs);
		assert.equal(result.acknowledged, true);
		assert.equal(result.insertedCount, 3);
		assert.deepEqual(Object.keys(result.insertedIds), ['0', '1', '2']);
		assert.ok(docs[0]._id instanceof ObjectId && docs[0]._id === result.insertedIds[0]);
		assert.equal(result.insertedIds[1], 'custom');
		assert.deepEqual(await items.findOne({ n: 3 }), { _id: docs[2]._id, n: 3 });
		assert.deepEqual(await items.insertMany([]), { acknowledged: true, insertedCount: 0, insertedIds: {} });
		await assert.rejects(items.insertMany([{ ok: 1 }, 'nope']), TypeError);
		assert.equal(await items.countDocuments({}), 3, 'an invalid document inserts nothing');
		await assertConsistent(items, client, []);
	});
});

test('insertMany ordered stops at the first duplicate; unordered inserts the rest', async () => {
	await withCollection(async (items, client) => {
		await items.createIndex({ tag: 1 });
		await items.insertOne({ _id: 'taken' });
		await assert.rejects(items.insertMany([{ _id: 'a', tag: 'x' }, { _id: 'taken' }, { _id: 'c', tag: 'x' }]), (error) => {
			assert.ok(error instanceof MongoBulkWriteError);
			assert.equal(error.code, 11000);
			assert.deepEqual(error.writeErrors.map((e) => [e.index, e.code]), [[1, 11000]]);
			assert.equal(error.insertedCount, 1);
			assert.deepEqual(error.insertedIds, { 0: 'a' });
			return true;
		});
		assert.equal(await items.findOne({ _id: 'c' }), null, 'ordered: nothing after the failure');
		await assert.rejects(items.insertMany([{ _id: 'd', tag: 'y' }, { _id: 'taken' }, { _id: 'd' }, { _id: 'e', tag: 'y' }], { ordered: false }), (error) => {
			assert.deepEqual(error.writeErrors.map((e) => e.index), [1, 2], 'unordered: every failure listed');
			assert.equal(error.insertedCount, 2);
			return true;
		});
		assert.deepEqual((await items.find({ tag: 'y' }).toArray()).map((d) => d._id), ['d', 'e']);
		await assertConsistent(items, client, ['tag']);
	});
});

test('insertMany ordered keeps its order across many commits', async () => {
	await withCollection(async (items, client) => {
		await items.insertOne({ _id: 70_000 });
		// ~500 bytes each: a 4 MB batch holds ~8000, so batches split between settles
		const docs = Array.from({ length: 100_000 }, (_, i) => ({ _id: i, pad: 'x'.repeat(500) }));
		await assert.rejects(items.insertMany(docs), (error) => error.insertedCount == 70_000 && error.writeErrors[0].index == 70_000);
		assert.equal(await items.countDocuments({}), 70_001);
		assert.equal(await items.findOne({ _id: 70_001 }), null);
		const unordered = Array.from({ length: 100_000 }, (_, i) => ({ _id: i }));
		await assert.rejects(items.insertMany(unordered, { ordered: false }), (error) => error.insertedCount == 29_999 && error.writeErrors.length == 70_001);
		assert.equal(await items.countDocuments({}), 100_000);
		await assertConsistent(items, client, []);
	});
});

test('find: filters, index and scan paths, cursor API', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ group: 1 });
		await items.insertMany(Array.from({ length: 2000 }, (_, i) => ({ _id: i, group: i % 10, odd: i % 2 == 1, tags: [`t${i % 3}`] })));
		const ids = async (cursor) => (await cursor.toArray()).map((doc) => doc._id);
		assert.equal((await items.find().toArray()).length, 2000);
		assert.deepEqual((await ids(items.find({ group: 3 }))).slice(0, 3), [3, 13, 23]);
		assert.equal((await ids(items.find({ group: 3, odd: true }))).length, 200);
		assert.equal((await ids(items.find({ group: 4, odd: true }))).length, 0);
		assert.equal((await ids(items.find({ tags: 't1', odd: false }))).length, 333);
		assert.deepEqual(await ids(items.find({ _id: 7 })), [7]);
		assert.deepEqual(await ids(items.find({ _id: 7, group: 8 })), []);
		assert.deepEqual(await ids(items.find({ group: 3 }).skip(2).limit(3)), [23, 33, 43]);
		assert.deepEqual(await ids(items.find({}, { skip: 1998, limit: 10 })), [1998, 1999]);
		assert.deepEqual(await ids(items.find({}).limit(-2)), [0, 1]);

		const cursor = items.find({ group: 9 });
		assert.ok(cursor instanceof FindCursor);
		assert.equal(await cursor.hasNext(), true);
		assert.equal((await cursor.next())._id, 9);
		assert.equal((await cursor.next())._id, 19);
		assert.throws(() => cursor.limit(1), /already initialized/);
		let seen = 0;
		for await (const doc of cursor) {
			assert.equal(doc.group, 9);
			if (++seen == 5) break;
		}
		assert.equal(await cursor.next(), null, 'breaking out of for await closes the cursor');
		assert.equal(await items.find({ group: 42 }).next(), null);
		await assert.rejects(async () => items.find({ n: { $mod: [2, 0] } }), /not supported/);
	});
});

test('countDocuments: counter, index, scan, skip and limit', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ group: 1 });
		await items.insertMany(Array.from({ length: 3000 }, (_, i) => ({ _id: i, group: i % 3, big: i >= 1000 })));
		assert.equal(await items.countDocuments(), 3000);
		assert.equal(await items.countDocuments({}, { skip: 2990 }), 10);
		assert.equal(await items.countDocuments({}, { limit: 7 }), 7);
		assert.equal(await items.countDocuments({ group: 1 }), 1000);
		assert.equal(await items.countDocuments({ group: 1 }, { skip: 10, limit: 5 }), 5);
		assert.equal(await items.countDocuments({ group: 5 }), 0);
		assert.equal(await items.countDocuments({ big: false }), 1000);
		assert.equal(await items.countDocuments({ big: true, group: 2 }), 667);
		assert.equal(await items.countDocuments({ _id: 5 }), 1);
		assert.equal(await items.countDocuments({ _id: 5, group: 0 }), 0);
		assert.equal(await items.countDocuments({ missing: null }), 3000, 'null matches missing fields');
	});
});

test('deleteOne and deleteMany keep documents, indexes and the count consistent', async () => {
	await withCollection(async (items, client) => {
		await items.createIndex({ group: 1 });
		await items.createIndex({ 'meta.color': 1 });
		await items.insertMany(Array.from({ length: 5000 }, (_, i) => ({ _id: i, group: i % 10, meta: { color: ['red', 'green', 'blue'][i % 3] }, big: i >= 2500 })));
		assert.deepEqual(await items.deleteOne({ _id: 17 }), { acknowledged: true, deletedCount: 1 });
		assert.deepEqual(await items.deleteOne({ _id: 17 }), { acknowledged: true, deletedCount: 0 });
		assert.deepEqual(await items.deleteOne({ group: 3 }), { acknowledged: true, deletedCount: 1 });
		assert.equal(await items.findOne({ _id: 3 }), null, 'deleteOne takes the first match');
		assert.deepEqual(await items.deleteOne({ big: true, group: 4 }), { acknowledged: true, deletedCount: 1 });
		assert.equal(await items.findOne({ _id: 2504 }), null);
		assert.equal((await items.deleteMany({ group: 5 })).deletedCount, 500);
		// red below 2500: 834, minus _id 3 (deleted above) and the 83 red ones of group 5
		assert.equal((await items.deleteMany({ 'meta.color': 'red', big: false })).deletedCount, 750);
		assert.equal((await items.deleteMany({ group: 5 })).deletedCount, 0);
		assert.equal(await items.countDocuments({}), 5000 - 3 - 500 - 750);
		await assertConsistent(items, client, ['group']);
		assert.equal((await items.deleteMany({})).deletedCount, 5000 - 3 - 500 - 750);
		assert.equal(await items.countDocuments({}), 0);
		assert.equal(await items.findOne({ group: 1 }), null);
		await assertConsistent(items, client, ['group']);
		// the collection keeps working, indexes included
		await items.insertMany([{ _id: 1, group: 1 }, { _id: 2, group: 1 }]);
		assert.equal(await items.countDocuments({ group: 1 }), 2);
		await assertConsistent(items, client, ['group']);
	});
});

test('deleteMany({}) leaves other collections alone', async () => {
	await withCollection(async (items, client) => {
		const other = client.db('app').collection('other');
		await other.createIndex({ k: 1 });
		await items.createIndex({ k: 1 });
		await items.insertMany(Array.from({ length: 200 }, (_, i) => ({ _id: i, k: i % 5 })));
		await other.insertMany(Array.from({ length: 100 }, (_, i) => ({ _id: i, k: i % 5 })));
		assert.equal((await items.deleteMany({})).deletedCount, 200);
		assert.equal(await other.countDocuments({}), 100);
		assert.equal(await other.countDocuments({ k: 3 }), 20);
		await assertConsistent(other, client, ['k']);
	});
});

test('races: concurrent deletes of one document count once; re-inserted documents survive stale deletes', async () => {
	await withCollection(async (items, client) => {
		await items.createIndex({ v: 1 });
		await items.insertOne({ _id: 1, v: 'old' });
		const results = await Promise.all([items.deleteOne({ _id: 1 }), items.deleteOne({ _id: 1 }), items.deleteOne({ v: 'old' })]);
		assert.equal(results.reduce((sum, r) => sum + r.deletedCount, 0), 1);
		await items.insertOne({ _id: 1, v: 'old' });
		// delete then re-insert in the same turn: the insert sees the deletion
		const [deleted, inserted] = await Promise.all([items.deleteOne({ _id: 1 }), items.insertOne({ _id: 1, v: 'new' })]);
		assert.equal(deleted.deletedCount, 1);
		assert.equal(inserted.acknowledged, true);
		assert.deepEqual(await items.findOne({ v: 'new' }), { _id: 1, v: 'new' });
		assert.equal(await items.findOne({ v: 'old' }), null);
		await assertConsistent(items, client, ['v']);
	});
});

test('deletes while an index is being built leave no stale entries', async () => {
	await withCollection(async (items, client) => {
		await items.insertMany(Array.from({ length: 30_000 }, (_, i) => ({ _id: i, v: i % 100 })));
		const building = items.createIndex({ v: 1 });
		const deleting = items.deleteMany({ v: 7 });
		const deletingOne = items.deleteOne({ _id: 29_999 });
		await Promise.all([building, deleting, deletingOne]);
		assert.equal(await items.countDocuments({ v: 7 }), 0);
		assert.equal(await items.countDocuments({}), 30_000 - 300 - 1);
		await assertConsistent(items, client, ['v']);
	});
});

test('random workload matches an in-memory model', async () => {
	await withCollection(async (items, client) => {
		await items.createIndex({ a: 1 });
		const model = new Map();
		let seed = 42;
		const random = (n) => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) % n);
		const make = (id) => ({ _id: id, a: random(20), b: random(5), s: `s${random(50)}` });
		for (let round = 0; round < 60; round++) {
			const op = random(6);
			if (op == 0) {
				const docs = Array.from({ length: random(300) }, () => make(random(3000)));
				const unique = [...new Map(docs.map((d) => [d._id, d])).values()].filter((d) => !model.has(d._id));
				await items.insertMany(unique);
				for (const d of unique) model.set(d._id, d);
			} else if (op == 1) {
				const id = random(3000);
				if (!model.has(id)) {
					const doc = make(id);
					await items.insertOne(doc);
					model.set(id, doc);
				}
			} else if (op == 2) {
				const filter = random(2) ? { a: random(20) } : { b: random(5), a: random(20) };
				const { deletedCount } = await items.deleteMany(filter);
				const doomed = [...model.values()].filter((d) => Object.entries(filter).every(([k, v]) => d[k] === v));
				assert.equal(deletedCount, doomed.length);
				for (const d of doomed) model.delete(d._id);
			} else if (op == 3) {
				const filter = { s: `s${random(50)}` };
				const { deletedCount } = await items.deleteOne(filter);
				const first = [...model.values()].filter((d) => d.s === filter.s).sort((x, y) => x._id - y._id)[0];
				assert.equal(deletedCount, first ? 1 : 0);
				if (first) model.delete(first._id);
			} else {
				const filter = random(2) ? { a: random(20) } : { s: `s${random(50)}`, b: random(5) };
				const expected = [...model.values()].filter((d) => Object.entries(filter).every(([k, v]) => d[k] === v)).map((d) => d._id).sort((x, y) => x - y);
				assert.deepEqual((await items.find(filter).toArray()).map((d) => d._id), expected);
				assert.equal(await items.countDocuments(filter), expected.length);
			}
		}
		assert.equal(await items.countDocuments({}), model.size);
		await assertConsistent(items, client, ['a']);
	});
});

test('counts and indexes survive a restart', async () => {
	const dir = path.join(tempDir(), 'db');
	let client = await MostikClient.connect(dir);
	let items = client.db('app').collection('items');
	await items.createIndex({ g: 1 });
	await items.insertMany(Array.from({ length: 1000 }, (_, i) => ({ _id: i, g: i % 4 })));
	await items.deleteMany({ g: 2 });
	await client.close();
	client = await MostikClient.connect(dir);
	items = client.db('app').collection('items');
	assert.equal(await items.countDocuments({}), 750);
	assert.equal(await items.countDocuments({ g: 1 }), 250);
	assert.equal(await items.countDocuments({ g: 2 }), 0);
	await assertConsistent(items, client, ['g']);
	await client.close();
});

test('native filter agrees with the JS matcher on random nested documents', async () => {
	const { matches } = require('../lib/query.js');
	await withCollection(async (items) => {
		let seed = 7;
		const random = (n) => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) % n);
		const scalars = () => [0, 1, -1, 255, 256, 65536, 2 ** 33, -(2 ** 40), 1.5, true, false, 'a', 'b', 'x'.repeat(40), 'y'.repeat(300), 'ключ', ''];
		const scalar = () => scalars()[random(17)];
		const keys = ['a', 'b', 'c', 'k'.repeat(40), 'кл'];
		const make = (depth) => {
			const kind = random(depth > 2 ? 3 : 6);
			if (kind < 3) return scalar();
			if (kind == 3) return Array.from({ length: random(4) }, () => make(depth + 1));
			if (kind == 4) return Array.from({ length: random(3) ? random(4) : 20 }, () => ({ [keys[random(5)]]: make(depth + 1) }));
			const obj = {};
			const n = random(3) ? random(4) : 20; // 20 fields: map16
			for (let i = 0; i < n; i++) obj[i < 5 ? keys[i] : `f${i}`] = make(depth + 1);
			return obj;
		};
		// top level is always a document whose fields are often arrays, so every branch of the
		// path walk (positions, arrays of documents, nested maps) is exercised
		const top = () => {
			const doc = {};
			for (const key of keys) if (random(4)) doc[key] = random(2) ? make(1) : Array.from({ length: 1 + random(3) }, () => (random(2) ? scalar() : make(2)));
			return doc;
		};
		const docs = Array.from({ length: 3000 }, (_, i) => ({ _id: i, ...top(), extra: [new Date(i), new ObjectId()] }));
		await items.insertMany(docs);
		const stored = await items.find({}).toArray(); // values as read back
		const paths = ['a', 'b', 'a.b', 'a.c', 'b.a', 'a.0', 'a.1.b', `${keys[3]}.a`, 'кл', 'кл.a', 'c.c.c', 'a.b.c', 'extra', 'extra.0'];
		let nonEmpty = 0;
		for (let round = 0; round < 600; round++) {
			const filter = {};
			for (let i = 0, n = 1 + random(2); i < n; i++) filter[paths[random(paths.length)]] = scalar();
			const expected = stored.filter((doc) => matches(doc, filter)).length;
			nonEmpty += expected > 0;
			assert.equal(await items.countDocuments(filter), expected, JSON.stringify(filter));
		}
		assert.ok(nonEmpty > 100, `only ${nonEmpty} filters matched anything`);
	});
});

test('durability option: journal by default, strict on request; close writes a checkpoint', async () => {
	const fs = await import('node:fs');
	assert.throws(() => new MostikClient(path.join(tempDir(), 'db'), { durability: 'eventually' }), /durability/);
	for (const durability of [undefined, 'strict']) {
		const dir = path.join(tempDir(), 'db');
		const client = await MostikClient.connect(dir, { durability });
		const items = client.db('app').collection('items');
		await items.insertMany(Array.from({ length: 1000 }, (_, i) => ({ _id: i })));
		for (let i = 0; i < 20; i++) await items.deleteOne({ _id: i });
		const journals = () => ['0', '1'].reduce((sum, n) => sum + fs.statSync(path.join(dir, `data.mostik-journal${n}`)).size, 0);
		assert.ok(journals() > 0, 'writes go to the journal');
		await client.close();
		assert.equal(journals(), 0, 'closing checkpoints and empties the journal');
		const again = await MostikClient.connect(dir);
		assert.equal(await again.db('app').collection('items').countDocuments({}), 980);
		await again.close();
	}
});

test('a killed process keeps every acknowledged write, in both durability modes', async () => {
	const { spawn } = await import('node:child_process');
	const { fileURLToPath } = await import('node:url');
	for (const durability of ['journal', 'strict']) {
		const dir = path.join(tempDir(), 'db');
		const writer = spawn(
			process.execPath,
			[
				'--input-type=module',
				'-e',
				`import { MostikClient } from 'mostik';
				const items = (await MostikClient.connect(${JSON.stringify(dir)}, { durability: '${durability}' })).db('app').collection('items');
				for (let i = 0; ; i++) {
					await items.insertOne({ _id: i });
					console.log(i);
				}`,
			],
			{ cwd: path.dirname(fileURLToPath(import.meta.url)), stdio: ['ignore', 'pipe', 'inherit'] },
		);
		let acknowledged = -1;
		writer.stdout.on('data', (chunk) => {
			const lines = chunk.toString().trim().split('\n');
			acknowledged = Number(lines.at(-1));
		});
		await new Promise((resolve) => setTimeout(resolve, 600));
		writer.kill('SIGKILL');
		await new Promise((resolve) => writer.once('exit', resolve));
		const client = await MostikClient.connect(dir);
		const items = client.db('app').collection('items');
		assert.ok(acknowledged > 10, `${durability}: only ${acknowledged} writes`);
		assert.ok((await items.countDocuments({})) >= acknowledged + 1, `${durability}: acknowledged writes survive`);
		assert.deepEqual(await items.findOne({ _id: acknowledged }), { _id: acknowledged });
		await client.close();
	}
});

test('deleteMany({}) takes documents and index entries in one commit; writes meanwhile wait', async () => {
	const dir = path.join(tempDir(), 'db');
	let client = await MostikClient.connect(dir);
	let items = client.db('app').collection('items');
	await items.createIndex({ g: 1 });
	await items.insertMany(Array.from({ length: 30_000 }, (_, i) => ({ _id: i, g: i % 3 })));
	const cleared = items.deleteMany({});
	const later = Array.from({ length: 50 }, (_, i) => items.insertOne({ _id: `late${i}`, g: i % 3 }));
	assert.equal((await cleared).deletedCount, 30_000);
	await Promise.all(later);
	assert.equal(await items.countDocuments({}), 50);
	assert.equal(await items.countDocuments({ g: 1 }), 17);
	await assertConsistent(items, client, ['g']);
	await client.close();
	client = await MostikClient.connect(dir);
	items = client.db('app').collection('items');
	await assertConsistent(items, client, ['g']);
	await client.close();
});

test('find returns documents larger than the cursor buffer, by index and by scan', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ g: 1 });
		const big = (i) => ({ _id: i, g: i % 2, body: String(i).repeat(40_000) }); // 200+ KB each
		await items.insertMany(Array.from({ length: 12 }, (_, i) => big(i)));
		await items.insertMany(Array.from({ length: 500 }, (_, i) => ({ _id: 1000 + i, g: i % 2 })));
		const viaIndex = await items.find({ g: 0 }).toArray();
		assert.equal(viaIndex.length, 6 + 250);
		for (const doc of viaIndex.filter((d) => d._id < 12)) assert.equal(doc.body, big(doc._id).body);
		const viaScan = await items.find({ body: big(5).body }).toArray();
		assert.deepEqual(viaScan.map((d) => d._id), [5]);
		assert.deepEqual((await items.find({ g: 1 }).skip(3).limit(2).toArray()).map((d) => d._id), [7, 9]);
		assert.equal((await items.find({ g: 1 }).skip(300).toArray()).length, 0);
	});
});

test('large results come out whole and in order, by index and by scan, including early stops', async () => {
	await withCollection(async (items) => {
		const N = 30_000;
		const docs = [];
		for (let i = 0; i < N; i++) docs.push({ _id: i, g: i % 3, h: i % 5, rare: i % 997 == 13, text: 'item ' + i });
		await items.insertMany(docs);
		await items.createIndex({ g: 1 });
		// several chunks through the index, each prefetched while the previous one is decoded
		const g1 = await items.find({ g: 1 }).toArray();
		assert.deepEqual(g1.map((d) => d._id), docs.filter((d) => d.g == 1).map((d) => d._id));
		// index plus a condition checked on the documents
		const g1h2 = await items.find({ g: 1, h: 2 }).toArray();
		assert.deepEqual(g1h2.map((d) => d._id), docs.filter((d) => d.g == 1 && d.h == 2).map((d) => d._id));
		// stopping early leaves a prefetch behind; later reads are unaffected
		let seen = 0;
		for await (const doc of items.find({ g: 2 })) if (++seen == 1500) break;
		assert.equal((await items.find({ g: 2 }).skip(5000).limit(3).toArray()).map((d) => d._id).join(), '15002,15005,15008');
		// sparse matches over many leaves, checked in parallel, with and without limits
		const rare = docs.filter((d) => d.rare).map((d) => d._id);
		assert.deepEqual((await items.find({ rare: true }).toArray()).map((d) => d._id), rare);
		assert.deepEqual((await items.find({ rare: true }).limit(7).toArray()).map((d) => d._id), rare.slice(0, 7));
		assert.equal(await items.countDocuments({ rare: true }), rare.length);
		assert.equal(await items.countDocuments({ h: 4 }), N / 5);
		// writes between chunks: a scan keeps going from where it was
		const cursor = items.find({ h: 0 });
		const first = await cursor.next();
		await items.deleteMany({ g: 0 });
		const rest = await cursor.toArray();
		assert.equal(first._id, 0);
		assert.ok(rest.every((d) => d.h == 0 && d._id > 0));
		assert.ok(rest.length >= docs.filter((d) => d.h == 0 && d.g != 0).length - 1);
	});
});

test('field names are stored once per shape, and the shapes survive reopening', async () => {
	const dbPath = path.join(tempDir(), 'db');
	const docs = [];
	for (let i = 0; i < 200; i++) docs.push({ _id: i, name: 'n' + (i % 10), age: i % 7, profile: { city: 'c' + (i % 5), tags: ['t' + (i % 3)] } });
	// more shapes than are shared: the rest are defined inside each document
	for (let i = 0; i < 60; i++) docs.push({ _id: 1000 + i, ['only' + i]: i, name: 'n' + (i % 10), nested: { ['deep' + i]: { city: 'c' + (i % 5) } } });
	let client = await MostikClient.connect(dbPath);
	await client.db('app').collection('items').insertMany(docs);
	const storage = client._storage();
	const raw = [...storage.scan(...prefixRange([encodeKey(['d', storage.collectionId('app', 'items')])]), { entries: true })];
	assert.ok(raw.slice(0, 200).every(({ bytes }) => !bytes.includes('name') && !bytes.includes('city')), 'shared field names are not in the values');
	await client.close();
	client = await MostikClient.connect(dbPath);
	const items = client.db('app').collection('items');
	try {
		assert.deepEqual(await items.find({}).toArray(), docs);
		const expect = (predicate) => docs.filter(predicate).map((d) => d._id);
		// natively filtered scans and counts read the stored shapes
		assert.deepEqual((await items.find({ name: 'n3', age: 2 }).toArray()).map((d) => d._id), expect((d) => d.name == 'n3' && d.age == 2));
		assert.equal(await items.countDocuments({ 'profile.city': 'c4' }), expect((d) => d.profile?.city == 'c4').length);
		assert.equal(await items.countDocuments({ 'profile.tags': 't1' }), expect((d) => d.profile?.tags.includes('t1')).length);
		assert.equal(await items.countDocuments({ 'nested.deep7.city': 'c2' }), 1);
		assert.equal(await items.countDocuments({ only59: 59 }), 1);
		// new shapes after reopening extend the saved ones
		await items.insertOne({ _id: 'x', fresh: 'yes', name: 'n1' });
		assert.equal(await items.countDocuments({ fresh: 'yes' }), 1);
		assert.equal(await items.countDocuments({ name: 'n1' }), expect((d) => d.name == 'n1').length + 1);
	} finally {
		await client.close();
	}
	client = await MostikClient.connect(dbPath);
	try {
		assert.deepEqual(await client.db('app').collection('items').findOne({ fresh: 'yes' }), { _id: 'x', fresh: 'yes', name: 'n1' });
	} finally {
		await client.close();
	}
});
