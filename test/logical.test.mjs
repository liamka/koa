// query operators $all, $size, $type, $not, $nor
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { MostikClient, ObjectId } from 'mostik';
import { tempDir } from './helpers.mjs';

async function withCollection(fn) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		await fn(client.db('app').collection('items'));
	} finally {
		await client.close();
	}
}

const ids = (docs) => docs.map((d) => String(d._id)).sort();

test('$type tells stored values apart as MongoDB does; names, codes, lists, "number"', async () => {
	await withCollection(async (items) => {
		const oid = new ObjectId('650000000000000000000001');
		await items.insertMany([
			{ _id: 1, v: 5 },
			{ _id: 2, v: 2.5 },
			{ _id: 3, v: 2 ** 31 },
			{ _id: 4, v: 'text' },
			{ _id: 5, v: null },
			{ _id: 6 },
			{ _id: 7, v: true },
			{ _id: 8, v: new Date(0) },
			{ _id: 9, v: oid },
			{ _id: 10, v: Buffer.from('ab') },
			{ _id: 11, v: { a: 1 } },
			{ _id: 12, v: [1, 'x'] },
			{ _id: 13, v: [] },
			{ _id: 14, v: 2n ** 40n },
			{ _id: 15, v: -3 },
			{ _id: 16, v: 7.0 },
		]);
		const find = async (type) => (await items.find({ v: { $type: type } }).toArray()).map((d) => d._id).sort((a, b) => a - b);
		const count = (type) => items.countDocuments({ v: { $type: type } });
		const cases = [
			['int', [1, 12, 15, 16]],
			[16, [1, 12, 15, 16]],
			['double', [2, 3]],
			['long', [14]],
			['number', [1, 2, 3, 12, 14, 15, 16]],
			['string', [4, 12]],
			[2, [4, 12]],
			['null', [5]],
			['bool', [7]],
			['date', [8]],
			['objectId', [9]],
			['binData', [10]],
			['object', [11]],
			['array', [12, 13]],
			[['string', 'bool'], [4, 7, 12]],
			['regex', []],
			['decimal', []],
		];
		const check = async (label) => {
			for (const [type, want] of cases) {
				assert.deepEqual(await find(type), want, `${label} ${JSON.stringify(type)}`);
				assert.equal(await count(type), want.length, `${label} count ${JSON.stringify(type)}`);
			}
		};
		await check('scan');
		await items.createIndex({ v: 1 });
		await check('index');
	});
});

test('$all, $size, $not, $nor: forms and edge cases', async () => {
	await withCollection(async (items) => {
		await items.insertMany([
			{ _id: 1, t: ['a', 'b', 'c'], n: 5 },
			{ _id: 2, t: ['a'], n: 15 },
			{ _id: 3, t: 'a', n: 'x' },
			{ _id: 4, t: [['a', 'b']], o: [{ k: 1, v: 2 }, { k: 2, v: 5 }] },
			{ _id: 5 },
			{ _id: 6, t: [], o: [{ k: 1, v: 9 }] },
		]);
		const find = async (filter) => (await items.find(filter).toArray()).map((d) => d._id).sort((a, b) => a - b);
		assert.deepEqual(await find({ t: { $all: ['a', 'b'] } }), [1]);
		assert.deepEqual(await find({ t: { $all: ['a'] } }), [1, 2, 3]);
		assert.deepEqual(await find({ t: { $all: [['a', 'b']] } }), [4]);
		assert.deepEqual(await find({ t: { $all: [] } }), []);
		assert.deepEqual(await find({ t: { $all: [/^b/, 'c'] } }), [1]);
		assert.deepEqual(await find({ o: { $all: [{ $elemMatch: { k: 1 } }, { $elemMatch: { v: { $gt: 4 } } }] } }), [4, 6]);
		assert.deepEqual(await find({ t: { $size: 1 } }), [2, 4]);
		assert.deepEqual(await find({ t: { $size: 0 } }), [6]);
		assert.deepEqual(await find({ 'o.v': { $size: 2 } }), []);
		assert.deepEqual(await find({ n: { $not: { $gt: 10 } } }), [1, 3, 4, 5, 6], 'missing and other types do not compare: $not keeps them');
		assert.deepEqual(await find({ n: { $not: { $gte: 5, $lte: 10 } } }), [2, 3, 4, 5, 6]);
		assert.deepEqual(await find({ t: { $not: /^a/ } }), [4, 5, 6]);
		assert.deepEqual(await find({ t: { $not: { $size: 1 } } }), [1, 3, 5, 6]);
		assert.deepEqual(await find({ $nor: [{ n: 5 }, { t: 'a' }] }), [4, 5, 6]);
		assert.deepEqual(await find({ $nor: [{ n: { $exists: true } }], _id: { $gt: 4 } }), [5, 6]);
		assert.deepEqual(await find({ o: { $elemMatch: { v: { $not: { $lt: 5 } } } } }), [4, 6]);
		assert.deepEqual(await find({ t: { $elemMatch: { $size: 2 } } }), [4]);
		assert.deepEqual(await find({ t: { $elemMatch: { $type: 'array' } } }), [4]);
	});
});

test('bad $all, $size, $type, $not, $nor are rejected', async () => {
	await withCollection(async (items) => {
		const bad = (filter, message) => assert.rejects(items.findOne(filter), message, JSON.stringify(filter));
		await bad({ a: { $all: 1 } }, /\$all needs an array/);
		await bad({ a: { $all: [{ $gt: 1 }] } }, /no \$ expressions in \$all/);
		await bad({ a: { $all: [{ $elemMatch: { b: 1 } }, 2] } }, /has to be consistent/);
		await bad({ a: { $size: '1' } }, /\$size needs a number/);
		await bad({ a: { $size: 1.5 } }, /whole number/);
		await bad({ a: { $size: -1 } }, /may not be negative/);
		await bad({ a: { $type: 'text' } }, /Unknown type name alias: text/);
		await bad({ a: { $type: 99 } }, /Invalid numerical type code: 99/);
		await bad({ a: { $type: [] } }, /at least one type/);
		await bad({ a: { $not: 5 } }, /\$not needs a regex or a document/);
		await bad({ a: { $not: {} } }, /\$not cannot be empty/);
		await bad({ a: { $not: { b: 1 } } }, /unknown operator: b/);
		await bad({ $nor: [] }, /\$nor must be a non-empty array/);
		await bad({ $nor: [1] }, /entries need to be full objects/);
	});
});

// a reference apart from lib/query.js, on the shapes the generator makes
const eq = (v, x) => v === x || (Array.isArray(v) && v.includes(x));
const typeRef = (v) => (v === null ? 'null' : Array.isArray(v) ? 'array' : typeof v == 'number' ? (Number.isInteger(v) ? 'int' : 'double') : typeof v == 'string' ? 'string' : typeof v == 'boolean' ? 'bool' : 'object');
function fieldRef(d, f, c) {
	const v = d[f];
	if (typeof c != 'object' || c === null) return c === null ? v === undefined || eq(v, null) : eq(v, c);
	return Object.entries(c).every(([op, x]) => {
		switch (op) {
			case '$all':
				return x.length > 0 && x.every((y) => eq(v, y));
			case '$size':
				return Array.isArray(v) && v.length == x;
			case '$type':
				return v !== undefined && (typeRef(v) == x || (Array.isArray(v) && v.some((e) => typeRef(e) == x)));
			case '$not':
				return !fieldRef(d, f, x);
			case '$gt':
				return (typeof v == 'number' && v > x) || (Array.isArray(v) && v.some((e) => typeof e == 'number' && e > x));
			case '$in':
				return x.some((y) => fieldRef(d, f, y));
			case '$exists':
				return (v !== undefined) == x;
		}
		throw new Error(op);
	});
}
function ref(d, filter) {
	return Object.entries(filter).every(([k, c]) => (k == '$nor' ? !c.some((p) => ref(d, p)) : k == '$or' ? c.some((p) => ref(d, p)) : fieldRef(d, k, c)));
}

test('$all, $size, $type, $not, $nor agree with a reference: scan, indexes, native counts', async () => {
	await withCollection(async (items) => {
		let seed = 9;
		const random = (n) => {
			seed = (seed * 16807) % 2147483647;
			return seed % n;
		};
		const pick = (list) => list[random(list.length)];
		const scalar = () => pick([() => random(6), () => random(6) + 0.5, () => pick(['a', 'b', 'c']), () => pick([true, false, null])])();
		const docs = Array.from({ length: 1500 }, (_, i) => {
			const d = { _id: i, g: random(5) };
			if (random(6)) d.t = random(3) ? Array.from({ length: random(4) }, scalar) : scalar();
			if (random(4)) d.n = scalar();
			return d;
		});
		await items.insertMany(docs);
		const conditions = () =>
			pick([
				() => ({ t: { $all: Array.from({ length: 1 + random(2) }, () => pick([1, 2, 'a', 'b', true])) } }),
				() => ({ t: { $size: random(4) } }),
				() => ({ t: { $type: pick(['int', 'double', 'string', 'bool', 'null', 'array']) } }),
				() => ({ n: { $type: pick(['int', 'double', 'string', 'bool', 'null']) } }),
				() => ({ n: { $not: { $gt: random(6) } } }),
				() => ({ t: { $not: { $size: random(3) } } }),
				() => ({ t: { $not: { $in: [1, 'a', null] } } }),
				() => ({ t: { $not: { $type: 'string' } } }),
				() => ({ n: { $not: { $exists: true } } }),
				() => ({ g: random(5) }),
				() => ({ t: pick(['a', 2, null]) }),
			])();
		const filters = [];
		for (let i = 0; i < 300; i++) {
			const f = pick([
				() => conditions(),
				() => ({ ...conditions(), ...conditions() }),
				() => ({ $nor: [conditions(), conditions()] }),
				() => ({ $nor: [conditions()], g: random(5) }),
				() => ({ $or: [conditions(), { $nor: [conditions()] }] }),
			])();
			filters.push(f);
		}
		const check = async (label) => {
			for (const f of filters) {
				const expected = ids(docs.filter((d) => ref(d, f)));
				assert.deepEqual(ids(await items.find(f).toArray()), expected, `${label}: ${JSON.stringify(f)}`);
				assert.equal(await items.countDocuments(f), expected.length, `${label} count: ${JSON.stringify(f)}`);
			}
		};
		await check('scan');
		await items.createIndex({ t: 1 });
		await items.createIndex({ g: 1, n: 1 });
		await check('indexes');
	});
});

test('paths through arrays as MongoDB 7 answers: null, positions, $eq', async () => {
	await withCollection(async (items) => {
		await items.insertMany([
			{ _id: 10, a: [new Date(0)] },
			{ _id: 11, a: [1, 2] },
			{ _id: 12, a: 5 },
			{ _id: 13, a: [{ k: 1 }] },
			{ _id: 14, a: [{ k: 1 }, 3] },
			{ _id: 15, a: [] },
			{ _id: 16, a: [{ j: 1 }] },
			{ _id: 17, a: { k: null } },
			{ _id: 18, a: [[{ k: 1 }]] },
			{ _id: 19, a: 'bb' },
			{ _id: 20, a: [{ 0: 'x' }, { k: 2 }] },
		]);
		const find = async (filter) => (await items.find(filter).toArray()).map((d) => d._id).sort((x, y) => x - y);
		const check = async (label) => {
			// an array element that is not a document has no missing field
			assert.deepEqual(await find({ 'a.k': null }), [12, 16, 17, 19, 20], label);
			assert.deepEqual(await find({ 'a.k': { $in: [null, 1] } }), [12, 13, 14, 16, 17, 19, 20], label);
			assert.deepEqual(await find({ 'a.k': { $ne: null } }), [10, 11, 13, 14, 15, 18], label);
			assert.equal(await items.countDocuments({ 'a.k': null }), 5, label);
			assert.deepEqual(await find({ 'a.k': { $exists: false } }), [10, 11, 12, 15, 16, 18, 19], label);
			// a number: the position, and the field of that name in the array's documents
			assert.deepEqual(await find({ 'a.0': 'x' }), [20], label);
			assert.deepEqual(await find({ 'a.0': { k: 1 } }), [13, 14], label);
			assert.deepEqual(await find({ 'a.0': null }), [12, 13, 14, 16, 17, 19, 20], label);
			assert.deepEqual(await find({ a: { $eq: 5 } }), [12], label);
			assert.deepEqual(await find({ a: { $eq: [1, 2] } }), [11], label);
			assert.deepEqual(await find({ a: { $eq: /b/ } }), [], 'a regular expression in $eq is a value');
			assert.deepEqual(await find({ a: { $eq: { k: null } } }), [17], label);
			assert.deepEqual(await find({ 'a.k': { $eq: 1 } }), [13, 14], label);
		};
		await check('scan');
		await items.createIndex({ 'a.k': 1 });
		await items.createIndex({ a: 1 });
		await check('indexes');
	});
});

test('an array a path ends at by a position matches as a whole value, as in MongoDB 7', async () => {
	await withCollection(async (items) => {
		await items.insertMany([{ _id: 1, a: [[{ k: 1 }]] }, { _id: 2, a: [[1, 2]] }, { _id: 3, a: [{ k: [1, 2] }] }, { _id: 4, a: [[3], 5] }, { _id: 5, a: { 0: [1, 9] } }, { _id: 6, a: [{ 0: [1, 7] }] }, { _id: 7, a: [[[1]]] }]);
		// MongoDB 7.0's answers
		const cases = [
			[{ 'a.0': { k: 1 } }, []],
			[{ 'a.0': 1 }, [5, 6]],
			[{ 'a.0': [1, 2] }, [2]],
			[{ 'a.0.k': 1 }, [1, 3]],
			[{ 'a.0': { $gt: 0 } }, [5, 6]],
			[{ 'a.0': { $size: 2 } }, [2, 5, 6]],
			[{ 'a.0': { $type: 'array' } }, [1, 2, 4, 5, 6, 7]],
			[{ 'a.0': { $type: 'int' } }, [5, 6]],
			[{ 'a.0.0': 1 }, [2, 5, 6]],
			[{ 'a.0': { $elemMatch: { $gt: 1 } } }, [2, 4, 5, 6]],
			[{ 'a.0': { $in: [1, 3] } }, [5, 6]],
			[{ 'a.0': { $all: [1] } }, [5, 6]],
			[{ 'a.0': { $ne: 1 } }, [1, 2, 3, 4, 7]],
			[{ 'a.0': null }, [3]],
			[{ 'a.0': 7 }, [6]],
			[{ 'a.0.0.0': 1 }, [6, 7]],
		];
		const gone = new Set();
		const check = async (label) => {
			for (const [filter, all] of cases) {
				const want = all.filter((id) => !gone.has(id));
				assert.deepEqual((await items.find(filter).toArray()).map((d) => d._id).sort(), want, `${label} ${JSON.stringify(filter)}`);
				assert.equal(await items.countDocuments(filter), want.length, `${label} count ${JSON.stringify(filter)}`);
			}
		};
		await check('scan');
		// a position that array elements also have as a field name: MongoDB refuses to index it
		const ambiguous = (e) => e.code == 16746;
		await assert.rejects(items.createIndex({ 'a.0': 1 }), ambiguous);
		await items.deleteOne({ _id: 6 });
		gone.add(6);
		await items.createIndex({ 'a.0': 1 });
		await items.createIndex({ 'a.0.0': 1 });
		await assert.rejects(items.insertOne({ a: [5, { 0: 1 }] }), ambiguous);
		await check('indexes');
	});
});

test('$elemMatch on fields takes the documents of the array only, not arrays in it', async () => {
	await withCollection(async (items) => {
		await items.insertMany([{ _id: 1, b: [[{ k: 0 }]] }, { _id: 2, b: [{ k: 0 }] }, { _id: 3, b: [[1, { k: 0 }], 5] }]);
		const check = async (label) => {
			assert.deepEqual((await items.find({ b: { $elemMatch: { k: 0 } } }).toArray()).map((d) => d._id), [2], label);
			assert.equal(await items.countDocuments({ b: { $elemMatch: { k: 0 } } }), 1, label);
		};
		await check('scan');
		await items.createIndex({ 'b.k': 1 });
		await check('index');
	});
});
