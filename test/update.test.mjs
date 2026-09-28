// updateOne, updateMany: $set, $unset, $inc, upsert
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { MostikClient, ObjectId, MongoServerError } from 'mostik';
import { tempDir } from './helpers.mjs';

async function withCollection(fn) {
	const dbPath = path.join(tempDir(), 'db');
	const client = await MostikClient.connect(dbPath);
	try {
		await fn(client.db('app').collection('items'), dbPath);
	} finally {
		await client.close();
	}
}

/** Every index answers like a scan of the documents. */
async function assertIndexed(items, fields) {
	const all = await items.find({}).toArray();
	assert.equal(await items.countDocuments({}), all.length);
	for (const field of fields) {
		const values = new Set(all.flatMap((doc) => (Array.isArray(doc[field]) ? doc[field] : [doc[field] ?? null])));
		for (const value of values) {
			const expected = all.filter((doc) => (Array.isArray(doc[field]) ? doc[field].includes(value) : (doc[field] ?? null) === value)).map((d) => d._id);
			const found = (await items.find({ [field]: value }).toArray()).map((d) => d._id);
			assert.deepEqual(found.sort(), expected.sort(), `${field} = ${value}`);
			assert.equal(await items.countDocuments({ [field]: value }), expected.length, `count ${field} = ${value}`);
		}
	}
}

test('$set, $unset and $inc change the first matching document', async () => {
	await withCollection(async (items) => {
		await items.insertMany([
			{ _id: 1, name: 'a', n: 1, profile: { city: 'x' } },
			{ _id: 2, name: 'b', n: 5 },
		]);
		const result = await items.updateOne({ name: 'a' }, { $set: { 'profile.zip': '123', title: 'T' }, $inc: { n: 2, fresh: 3 }, $unset: { 'profile.city': '' } });
		assert.deepEqual(result, { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedCount: 0, upsertedId: null });
		// existing fields keep their place; new ones follow, in path order
		assert.deepEqual(Object.entries(await items.findOne({ _id: 1 })), Object.entries({ _id: 1, name: 'a', n: 3, profile: { zip: '123' }, fresh: 3, title: 'T' }));
		assert.deepEqual(await items.findOne({ _id: 2 }), { _id: 2, name: 'b', n: 5 });
		// embedded documents are created on the way
		await items.updateOne({ _id: 2 }, { $set: { 'a.b.c': true } });
		assert.deepEqual((await items.findOne({ _id: 2 })).a, { b: { c: true } });
		// nothing to change: matched, not modified
		assert.deepEqual(await items.updateOne({ _id: 2 }, { $set: { name: 'b' }, $unset: { missing: 1 }, $inc: { n: 0 } }), updateResult(1, 0));
		assert.deepEqual(await items.updateOne({ name: 'none' }, { $set: { x: 1 } }), updateResult(0, 0));
		assert.deepEqual(await items.updateOne({}, { $unset: { 'a.b.c.d': 1, 'nothing.here': 1 } }), updateResult(1, 0));
	});
});

function updateResult(matchedCount, modifiedCount) {
	return { acknowledged: true, matchedCount, modifiedCount, upsertedCount: 0, upsertedId: null };
}

test('array positions: set, pad with nulls, unset to null', async () => {
	await withCollection(async (items) => {
		await items.insertOne({ _id: 1, list: [1, 2, 3], docs: [{ v: 1 }, { v: 2 }] });
		await items.updateOne({ _id: 1 }, { $set: { 'list.1': 20, 'list.5': 6, 'docs.1.v': 22 }, $unset: { 'list.0': 1 }, $inc: { 'docs.0.v': 10 } });
		assert.deepEqual(await items.findOne({ _id: 1 }), { _id: 1, list: [null, 20, 3, null, null, 6], docs: [{ v: 11 }, { v: 22 }] });
		await assert.rejects(items.updateOne({ _id: 1 }, { $set: { 'list.x': 1 } }), (e) => e.code == 28);
	});
});

test('update errors match MongoDB', async () => {
	await withCollection(async (items) => {
		await items.insertOne({ _id: 1, s: 'text', n: 1, p: 5, nothing: null });
		const code = (n) => (error) => error instanceof MongoServerError && error.code == n;
		await assert.rejects(items.updateOne({ _id: 1 }, { $inc: { s: 1 } }), code(14));
		await assert.rejects(items.updateOne({ _id: 1 }, { $inc: { n: 'one' } }), code(14));
		await assert.rejects(items.updateOne({ _id: 1 }, { $set: { a: 1 }, $inc: { a: 1 } }), code(40));
		await assert.rejects(items.updateOne({ _id: 1 }, { $set: { a: {} }, $unset: { 'a.b': 1 } }), code(40));
		await assert.rejects(items.updateOne({ _id: 1 }, { $set: { _id: 2 } }), code(66));
		await assert.rejects(items.updateOne({ _id: 1 }, { $unset: { _id: 1 } }), code(66));
		await assert.rejects(items.updateOne({ _id: 1 }, { $set: { 'p.q': 1 } }), code(28));
		await assert.rejects(items.updateOne({ _id: 1 }, { $set: { 'nothing.q': 1 } }), code(28));
		await assert.rejects(items.updateOne({ _id: 1 }, { $set: { 'a..b': 1 } }), code(56));
		await assert.rejects(items.updateOne({ _id: 1 }, { name: 'replacement' }), /atomic operators/);
		await assert.rejects(items.updateOne({ _id: 1 }, {}), /atomic operators/);
		await assert.rejects(items.updateOne({ _id: 1 }, { $pop: { list: 1 } }), /\$pop is not supported/);
		await assert.rejects(items.updateOne({ _id: 1 }, { $set: { 'a.$': 1 } }), /positional/);
		// the same _id is no change
		assert.equal((await items.updateOne({ _id: 1 }, { $set: { _id: 1 } })).modifiedCount, 0);
		assert.deepEqual(await items.findOne({ _id: 1 }), { _id: 1, s: 'text', n: 1, p: 5, nothing: null });
	});
});

test('index entries follow updated values', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ g: 1 });
		await items.createIndex({ tags: 1 });
		await items.createIndex({ 'meta.k': 1 });
		const docs = [];
		for (let i = 0; i < 300; i++) docs.push({ _id: i, g: i % 7, tags: ['t' + (i % 3), 't' + (i % 5)], meta: { k: i % 4 } });
		await items.insertMany(docs);
		await items.updateOne({ _id: 3 }, { $set: { g: 100, 'tags.1': 'new' }, $unset: { meta: 1 } });
		assert.deepEqual(await items.findOne({ g: 100 }), { _id: 3, g: 100, tags: ['t0', 'new'] });
		assert.equal(await items.findOne({ 'meta.k': 3, _id: 3 }), null);
		assert.equal((await items.updateMany({ g: 2 }, { $inc: { g: 10 } })).modifiedCount, docs.filter((d) => d.g == 2).length);
		assert.equal(await items.countDocuments({ g: 2 }), 0);
		await items.updateMany({ tags: 't1' }, { $set: { tags: ['t1', 'moved'] } });
		await items.updateMany({ 'meta.k': 0 }, { $unset: { g: 1 } });
		await assertIndexed(items, ['g', 'tags']);
		assert.equal(await items.countDocuments({ 'meta.k': 0 }), docs.filter((d) => d.meta.k == 0).length);
		assert.equal(await items.countDocuments({ g: null }), docs.filter((d) => d.meta.k == 0).length);
	});
});

test('updateMany: many documents, through an index and by scan; the changes survive reopening', async () => {
	await withCollection(async (items, dbPath) => {
		const N = 30_000;
		await items.createIndex({ g: 1 });
		const docs = [];
		for (let i = 0; i < N; i++) docs.push({ _id: i, g: i % 3, h: i % 5, n: 0 });
		await items.insertMany(docs);
		// through the index, moving every document out of the walked range
		assert.deepEqual(await items.updateMany({ g: 1 }, { $set: { g: 4 }, $inc: { n: 1 } }), updateResult(N / 3, N / 3));
		// scan with a native filter
		assert.deepEqual(await items.updateMany({ h: 2 }, { $inc: { n: 10 } }), updateResult(N / 5, N / 5));
		// every document, once
		assert.deepEqual(await items.updateMany({}, { $inc: { n: 100 } }), updateResult(N, N));
		assert.equal(await items.countDocuments({ g: 1 }), 0);
		assert.equal(await items.countDocuments({ g: 4 }), N / 3);
		const client = new MostikClient(dbPath);
		const again = client.db('app').collection('items');
		// the same file, shared: every write above is visible
		const all = await again.find({}).toArray();
		assert.equal(all.length, N);
		for (const doc of all) {
			const i = doc._id;
			assert.equal(doc.n, 100 + (i % 3 == 1 ? 1 : 0) + (i % 5 == 2 ? 10 : 0), `n of ${i}`);
			assert.equal(doc.g, i % 3 == 1 ? 4 : i % 3);
		}
		await client.close();
	});
});

test('updateMany stops at an error; documents updated before it stay updated', async () => {
	await withCollection(async (items) => {
		await items.insertMany([{ _id: 1, n: 1 }, { _id: 2, n: 2 }, { _id: 3, n: 'three' }, { _id: 4, n: 4 }]);
		await assert.rejects(items.updateMany({}, { $inc: { n: 1 } }), (e) => e.code == 14);
		assert.deepEqual((await items.find({}).toArray()).map((d) => d.n), [2, 3, 'three', 4]);
	});
});

test('concurrent updates of one document all apply', async () => {
	await withCollection(async (items) => {
		await items.insertOne({ _id: 'counter', n: 0 });
		const results = await Promise.all(Array.from({ length: 200 }, () => items.updateOne({ _id: 'counter' }, { $inc: { n: 1 } })));
		assert.ok(results.every((r) => r.modifiedCount == 1));
		assert.equal((await items.findOne({ _id: 'counter' })).n, 200);
	});
});

test('upsert inserts the filter fields with the update applied, only when nothing matches', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ email: 1 });
		const filter = { email: 'a@x', 'profile.lang': 'ru', profile2: { deep: 1 } };
		const result = await items.updateOne(filter, { $set: { name: 'A', 'profile2.more': 2 }, $inc: { visits: 1 } }, { upsert: true });
		assert.ok(result.upsertedId instanceof ObjectId);
		assert.deepEqual({ ...result, upsertedId: null }, { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 1, upsertedId: null });
		assert.deepEqual(filter.profile2, { deep: 1 }, "the caller's filter is untouched");
		const doc = await items.findOne({ email: 'a@x' });
		assert.deepEqual(doc, { _id: result.upsertedId, email: 'a@x', profile: { lang: 'ru' }, profile2: { deep: 1, more: 2 }, name: 'A', visits: 1 });
		// now it matches: updated, not inserted
		assert.deepEqual(await items.updateOne({ email: 'a@x' }, { $inc: { visits: 1 } }, { upsert: true }), updateResult(1, 1));
		assert.equal(await items.countDocuments({}), 1);
		// a filter _id becomes the document's _id, first
		const byId = await items.updateOne({ _id: 'u1', kind: 'k' }, { $set: { v: 1 } }, { upsert: true });
		assert.equal(byId.upsertedId, 'u1');
		assert.deepEqual(Object.keys(await items.findOne({ _id: 'u1' })), ['_id', 'kind', 'v']);
		// an _id from $set also comes first
		await items.updateOne({ kind: 'z' }, { $set: { _id: 'u2' } }, { upsert: true });
		assert.deepEqual(Object.keys(await items.findOne({ _id: 'u2' })), ['_id', 'kind']);
		// the _id exists but the rest of the filter does not match: duplicate key, as in MongoDB
		await assert.rejects(items.updateOne({ _id: 'u1', kind: 'other' }, { $set: { v: 2 } }, { upsert: true }), (e) => e.code == 11000);
		await assert.rejects(items.updateOne({ _id: 'u3' }, { $set: { _id: 'u4' } }, { upsert: true }), (e) => e.code == 66);
		// updateMany upserts one document when nothing matches
		const many = await items.updateMany({ kind: 'batch' }, { $set: { done: false } }, { upsert: true });
		assert.equal(many.upsertedCount, 1);
		assert.equal(await items.countDocuments({ kind: 'batch' }), 1);
		assert.equal((await items.updateMany({ kind: 'batch' }, { $set: { done: true } }, { upsert: true })).upsertedCount, 0);
		await assertIndexed(items, ['email']);
	});
});

const code = (n) => (error) => error instanceof MongoServerError && error.code == n;

test('$push with $each, $position, $sort, $slice; $addToSet; $pull by value and by condition', async () => {
	await withCollection(async (items) => {
		await items.insertOne({ _id: 1, a: [3, 1], docs: [{ k: 2, v: 'b' }, { k: 1, v: 'a' }], n: 5, set: [1, { x: 1 }] });
		const get = async () => items.findOne({ _id: 1 });
		await items.updateOne({ _id: 1 }, { $push: { a: 7, fresh: 'x', 'deep.list': 1 } });
		assert.deepEqual((await get()).a, [3, 1, 7]);
		assert.deepEqual((await get()).fresh, ['x'], 'a missing field becomes an array');
		assert.deepEqual((await get()).deep, { list: [1] });
		await items.updateOne({ _id: 1 }, { $push: { a: { $each: [9, 0], $position: 1 } } });
		assert.deepEqual((await get()).a, [3, 9, 0, 1, 7]);
		await items.updateOne({ _id: 1 }, { $push: { a: { $each: [4], $position: -1 } } });
		assert.deepEqual((await get()).a, [3, 9, 0, 1, 4, 7]);
		await items.updateOne({ _id: 1 }, { $push: { a: { $each: [], $sort: -1, $slice: 3 } } });
		assert.deepEqual((await get()).a, [9, 7, 4]);
		await items.updateOne({ _id: 1 }, { $push: { a: { $each: [5], $slice: -2 } } });
		assert.deepEqual((await get()).a, [4, 5]);
		await items.updateOne({ _id: 1 }, { $push: { docs: { $each: [{ k: 0, v: 'c' }], $sort: { k: 1 } } } });
		assert.deepEqual((await get()).docs.map((d) => d.k), [0, 1, 2]);
		assert.deepEqual(await items.updateOne({ _id: 1 }, { $push: { a: { $each: [], $slice: 5 } } }), updateResult(1, 0), 'nothing changed');

		await items.updateOne({ _id: 1 }, { $addToSet: { set: { x: 1 }, other: 'y' } });
		assert.deepEqual((await get()).set, [1, { x: 1 }], 'an equal element is not added twice');
		assert.deepEqual((await get()).other, ['y']);
		await items.updateOne({ _id: 1 }, { $addToSet: { set: { $each: [2, 1, 2, { x: 1, y: 2 }] } } });
		assert.deepEqual((await get()).set, [1, { x: 1 }, 2, { x: 1, y: 2 }]);
		assert.equal((await items.updateOne({ _id: 1 }, { $addToSet: { set: 2 } })).modifiedCount, 0);

		await items.updateOne({ _id: 1 }, { $pull: { set: { x: 1 } } });
		assert.deepEqual((await get()).set, [1, 2], 'a document condition removes documents matching it');
		await items.updateOne({ _id: 1 }, { $pull: { a: { $gte: 5 }, docs: { v: { $in: ['a', 'c'] } } } });
		assert.deepEqual((await get()).a, [4]);
		assert.deepEqual((await get()).docs, [{ k: 2, v: 'b' }]);
		await items.updateOne({ _id: 1 }, { $pull: { a: 4 } });
		assert.deepEqual((await get()).a, []);
		assert.equal((await items.updateOne({ _id: 1 }, { $pull: { missing: 1, a: 1 } })).modifiedCount, 0);

		await assert.rejects(items.updateOne({ _id: 1 }, { $push: { n: 1 } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $addToSet: { n: 1 } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $pull: { n: 1 } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $push: { a: { $each: 1 } } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $push: { a: { $each: [1], $slice: 'x' } } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $push: { a: { $each: [1], $sort: 2 } } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $addToSet: { a: { $each: [1], $slice: 1 } } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $push: { a: 1 }, $pull: { a: 1 } }), code(40));
	});
});

test('$min / $max in MongoDB order; $setOnInsert on inserts only; $rename', async () => {
	await withCollection(async (items) => {
		await items.insertOne({ _id: 1, n: 5, s: 'b', d: new Date(1000), nest: { a: 1 }, list: [{ x: 1 }], name: 'z' });
		const get = async () => items.findOne({ _id: 1 });
		assert.equal((await items.updateOne({ _id: 1 }, { $min: { n: 7 } })).modifiedCount, 0);
		assert.equal((await items.updateOne({ _id: 1 }, { $max: { n: 1 } })).modifiedCount, 0);
		await assert.rejects(items.updateOne({ _id: 1 }, { $min: { n: 7 }, $max: { n: 1 } }), code(40));
		await items.updateOne({ _id: 1 }, { $min: { n: 2, s: 'a', d: new Date(500), fresh: 3 }, $max: { top: 1 } });
		assert.deepEqual(await get(), { _id: 1, n: 2, s: 'a', d: new Date(500), nest: { a: 1 }, list: [{ x: 1 }], name: 'z', fresh: 3, top: 1 });
		await items.updateOne({ _id: 1 }, { $max: { n: 'text' }, $min: { s: null } });
		assert.equal((await get()).n, 'text', 'strings sort after numbers');
		assert.equal((await get()).s, null, 'null sorts before strings');

		// $setOnInsert: ignored when the document exists...
		assert.equal((await items.updateOne({ _id: 1 }, { $setOnInsert: { made: true } }, { upsert: true })).modifiedCount, 0);
		assert.equal((await get()).made, undefined);
		// ...applied when the upsert inserts
		const up = await items.updateOne({ _id: 2 }, { $setOnInsert: { made: true }, $set: { v: 1 } }, { upsert: true });
		assert.equal(up.upsertedId, 2);
		assert.deepEqual(await items.findOne({ _id: 2 }), { _id: 2, made: true, v: 1 });

		await items.updateOne({ _id: 1 }, { $rename: { name: 'title', 'nest.a': 'moved.deep.b' } });
		const renamed = await get();
		assert.equal(renamed.title, 'z');
		assert.equal('name' in renamed, false);
		assert.deepEqual(renamed.nest, {});
		assert.deepEqual(renamed.moved, { deep: { b: 1 } });
		assert.equal((await items.updateOne({ _id: 1 }, { $rename: { missing: 'x' } })).modifiedCount, 0);
		await items.updateOne({ _id: 1 }, { $rename: { title: 'n' } });
		assert.equal((await get()).n, 'z', 'the target is overwritten');
		await assert.rejects(items.updateOne({ _id: 1 }, { $rename: { n: 'n' } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $rename: { moved: 'moved.x' } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $rename: { 'list.0.x': 'y' } }), code(2));
		await assert.rejects(items.updateOne({ _id: 1 }, { $rename: { n: 'm' }, $set: { m: 1 } }), code(40));
		await assert.rejects(items.updateOne({ _id: 1 }, { $rename: { _id: 'id' } }), code(66));
		await assert.rejects(items.updateOne({ _id: 1 }, { $rename: { n: 5 } }), code(2));
	});
});

test('index entries follow $push, $pull, $addToSet, $min and $rename', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ tags: 1 });
		await items.createIndex({ score: 1 });
		await items.createIndex({ label: 1 });
		const docs = Array.from({ length: 300 }, (_, i) => ({ _id: i, tags: ['t' + (i % 3)], score: i % 50, name: 'n' + (i % 4) }));
		await items.insertMany(docs);
		await items.updateMany({ score: { $lt: 10 } }, { $push: { tags: 'low' } });
		await items.updateMany({ tags: 't1' }, { $addToSet: { tags: 'was1' } });
		await items.updateMany({ tags: 'was1' }, { $pull: { tags: 't1' } });
		await items.updateMany({}, { $min: { score: 20 } });
		await items.updateMany({ _id: { $lt: 100 } }, { $rename: { name: 'label' } });
		await assertIndexed(items, ['tags', 'score', 'label']);
		assert.equal(await items.countDocuments({ tags: 'low' }), 60);
		assert.equal(await items.countDocuments({ tags: 't1' }), 0);
		assert.equal(await items.countDocuments({ score: 20 }), 300 - docs.filter((d) => d.score < 20).length);
		assert.equal(await items.countDocuments({ label: 'n1' }), 25);
		assert.equal(await items.countDocuments({ label: null }), 200);
	});
});

test('$pull and $rename through a value in the way fail as in MongoDB 7; $unset does nothing', async () => {
	await withCollection(async (items) => {
		await items.insertMany([
			{ _id: 1, a: 5 },
			{ _id: 2, a: 'ab' },
			{ _id: 3, a: null },
			{ _id: 4, a: [1, { x: [1] }] },
			{ _id: 5, a: [{ x: [1] }, { x: [2] }] },
			{ _id: 6 },
		]);
		const code = (n) => (error) => error instanceof MongoServerError && error.code == n;
		for (const _id of [1, 2, 3, 4]) {
			for (const path of ['a.x', 'a.0.x', 'a.x.y']) {
				await assert.rejects(items.updateOne({ _id }, { $pull: { [path]: 1 } }), code(28), `$pull ${path} on ${_id}`);
				await assert.rejects(items.updateOne({ _id }, { $rename: { [path]: 'z' } }), code(28), `$rename ${path} on ${_id}`);
				assert.equal((await items.updateOne({ _id }, { $unset: { [path]: '' } })).modifiedCount, 0);
			}
		}
		await assert.rejects(items.updateOne({ _id: 5 }, { $pull: { 'a.x': 1 } }), code(28));
		await assert.rejects(items.updateOne({ _id: 5 }, { $rename: { 'a.x': 'z' } }), code(28));
		// through an array element that is a document: the source cannot be an array element
		await assert.rejects(items.updateOne({ _id: 5 }, { $rename: { 'a.0.x': 'z' } }), code(2));
		assert.equal((await items.updateOne({ _id: 5 }, { $pull: { 'a.0.x': 1 } })).modifiedCount, 1);
		assert.deepEqual((await items.findOne({ _id: 5 })).a, [{ x: [] }, { x: [2] }]);
		// nothing there: nothing to do
		assert.equal((await items.updateOne({ _id: 6 }, { $pull: { 'a.x': 1 }, $rename: { 'b.c': 'z' } })).modifiedCount, 0);
	});
});
