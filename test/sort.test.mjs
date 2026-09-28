// find().sort()
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { MostikClient, ObjectId, MongoServerError } from 'mostik';
import { tempDir } from './helpers.mjs';

async function withCollection(fn) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		await fn(client.db('app').collection('items'));
	} finally {
		await client.close();
	}
}

// ---- a reference written apart from lib/query.js: MongoDB's sort order ----

const EMPTY = Symbol('empty array');

function typeRank(v) {
	if (v === EMPTY) return 0;
	if (v === null || v === undefined) return 1;
	if (typeof v == 'number') return 2;
	if (typeof v == 'string') return 3;
	if (Array.isArray(v)) return 5;
	if (v instanceof Uint8Array) return 6;
	if (v instanceof ObjectId) return 7;
	if (typeof v == 'boolean') return 8;
	if (v instanceof Date) return 9;
	return 4;
}

function cmp(a, b) {
	const d = typeRank(a) - typeRank(b);
	if (d) return d;
	switch (typeRank(a)) {
		case 2:
			if (Number.isNaN(a) || Number.isNaN(b)) return Number(!Number.isNaN(a)) - Number(!Number.isNaN(b));
			return a < b ? -1 : a > b ? 1 : 0;
		case 3:
			return Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));
		case 4: {
			const [ka, kb] = [Object.keys(a), Object.keys(b)];
			for (let i = 0; i < Math.min(ka.length, kb.length); i++) {
				const o = typeRank(a[ka[i]]) - typeRank(b[kb[i]]) || Buffer.compare(Buffer.from(ka[i]), Buffer.from(kb[i])) || cmp(a[ka[i]], b[kb[i]]);
				if (o) return o;
			}
			return ka.length - kb.length;
		}
		case 5:
			for (let i = 0; i < Math.min(a.length, b.length); i++) {
				const o = cmp(a[i], b[i]);
				if (o) return o;
			}
			return a.length - b.length;
		case 6:
			return a.length - b.length || Buffer.compare(a, b);
		case 7:
			return a.toHexString() < b.toHexString() ? -1 : a.toHexString() > b.toHexString() ? 1 : 0;
		case 8:
			return Number(a) - Number(b);
		case 9:
			return a.getTime() - b.getTime();
		default:
			return 0;
	}
}

/** The sort key of `doc` on a top-level field: smallest (ascending) or largest element of an array. */
function refKey(doc, field, descending) {
	if (!(field in doc)) return null;
	const v = doc[field];
	const items = Array.isArray(v) ? (v.length ? v : [EMPTY]) : [v];
	return items.reduce((best, x) => (descending ? (cmp(x, best) > 0 ? x : best) : cmp(x, best) < 0 ? x : best));
}

function keysOf(doc, sort) {
	return sort.map(([f, d]) => refKey(doc, f, d < 0));
}

function compareKeys(a, b, sort) {
	for (let i = 0; i < sort.length; i++) {
		const o = cmp(a[i], b[i]) * sort[i][1];
		if (o) return Math.sign(o);
	}
	return 0;
}

function generator(seed) {
	const next = () => {
		seed = (seed + 0x6d2b79f5) | 0;
		let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32;
	};
	const random = (n) => Math.floor(next() * n);
	const pick = (list) => list[random(list.length)];
	const ids = [1, 2, 3].map((i) => new ObjectId(`65000000000000000000000${i}`));
	const scalar = () =>
		pick([
			() => pick([0, 1, -1, 2.5, 7, 100, -100, 1e9, NaN, Infinity, -Infinity]),
			() => pick(['', 'a', 'b', 'B', 'ab', 'я', '￿', '\u{10000}']),
			() => pick([true, false, null]),
			() => pick(ids),
			() => new Date(pick([0, 1e12, 2e12])),
			() => pick([{ x: 1 }, { x: 2 }, { y: 1 }, { x: 1, z: 0 }]),
			() => Buffer.from(pick(['a', 'b', 'aa'])),
		])();
	const doc = (i) => {
		const d = { _id: pick([() => i, () => `s${i}`, () => new ObjectId(`66${String(i).padStart(22, '0')}`)])() };
		if (random(6)) d.a = random(3) ? scalar() : random(5) ? Array.from({ length: 1 + random(3) }, scalar) : [];
		if (random(6)) d.b = random(2) ? random(50) : pick(['p', 'q', 'r', null, true]);
		d.n = random(10);
		return d;
	};
	const sortSpec = () => {
		const fields = ['a', 'b', '_id', 'n'];
		const first = pick(fields);
		const spec = [[first, pick([1, -1])]];
		if (random(3) == 0) spec.push([pick(fields.filter((f) => f != first)), pick([1, -1])]);
		return spec;
	};
	const filter = () => pick([{}, {}, { n: random(10) }, { n: { $lt: 5 } }, { b: { $gte: 10 } }, { a: { $ne: null } }, { b: { $in: ['p', 3, null] } }, { n: { $in: [1, 2] }, b: { $lt: 40 } }]);
	return { doc, sortSpec, filter, random };
}

test('sort agrees with a reference on mixed types, arrays and missing fields: in memory and by index', async () => {
	await withCollection(async (items) => {
		const g = generator(5);
		const docs = Array.from({ length: 600 }, (_, i) => g.doc(i));
		await items.insertMany(docs);
		const stored = await items.find({}).toArray();
		const check = async (label) => {
			const r = generator(label.length * 101);
			for (let round = 0; round < 250; round++) {
				const sort = r.sortSpec();
				const filter = r.filter();
				const [skip, limit] = r.random(3) == 0 ? [r.random(20), 1 + r.random(30)] : [0, 0];
				const matching = stored.filter((d) => {
					// the filters used: equality and simple comparisons on n and b, $ne null on a
					for (const [f, c] of Object.entries(filter)) {
						const v = d[f];
						if (typeof c != 'object' || c === null) {
							if (v !== c) return false;
							continue;
						}
						for (const [op, x] of Object.entries(c)) {
							const t = typeof v == typeof x && v !== null && x !== null;
							if (op == '$lt' && !(t && v < x)) return false;
							if (op == '$gte' && !(t && v >= x)) return false;
							if (op == '$ne' && (v === x || (x === null && v === undefined) || (Array.isArray(v) && v.includes(x)))) return false;
							if (op == '$in' && !x.some((y) => v === y || (y === null && v === undefined))) return false;
						}
					}
					return true;
				});
				const expected = matching.map((d) => keysOf(d, sort)).sort((a, b) => compareKeys(a, b, sort));
				let cursor = items.find(filter).sort(Object.fromEntries(sort));
				if (skip) cursor = cursor.skip(skip);
				if (limit) cursor = cursor.limit(limit);
				const found = await cursor.toArray();
				const what = `${label}: ${JSON.stringify(filter)} sort ${JSON.stringify(sort)} skip ${skip} limit ${limit}`;
				const want = expected.slice(skip, limit ? skip + limit : undefined);
				assert.equal(found.length, want.length, what);
				const got = found.map((d) => keysOf(d, sort));
				for (let i = 0; i < got.length; i++) assert.equal(compareKeys(got[i], want[i], sort), 0, `${what}: position ${i}`);
				if (!skip && !limit) {
					assert.deepEqual(new Set(found.map((d) => String(d._id))), new Set(matching.map((d) => String(d._id))), what);
				}
			}
		};
		await check('memory');
		await items.createIndex({ a: 1 }); // multikey, every type
		await items.createIndex({ b: -1 });
		await items.createIndex({ n: 1 });
		await check('indexes');
	});
});

test('sort forms: object, pairs, string, directions by name; sort before reading only', async () => {
	await withCollection(async (items) => {
		await items.insertMany([
			{ _id: 1, a: 2, b: 'x' },
			{ _id: 2, a: 1, b: 'y' },
			{ _id: 3, a: 2, b: 'z' },
		]);
		const ids = async (cursor) => (await cursor.toArray()).map((d) => d._id);
		assert.deepEqual(await ids(items.find().sort({ a: 1, b: -1 })), [2, 3, 1]);
		assert.deepEqual(await ids(items.find().sort([['a', 'desc'], ['_id', 'asc']])), [1, 3, 2]);
		assert.deepEqual(await ids(items.find().sort(['b', -1])), [3, 2, 1]);
		assert.deepEqual(await ids(items.find().sort('b')), [1, 2, 3]);
		assert.deepEqual(await ids(items.find({}, { sort: { _id: -1 }, limit: 2 })), [3, 2]);
		assert.deepEqual(await ids(items.find().sort({ _id: -1 }).skip(1).limit(1)), [2]);
		assert.throws(() => items.find().sort({ a: 2 }), (e) => e instanceof MongoServerError && e.code == 2);
		const started = items.find().sort({ a: 1 });
		await started.next();
		assert.throws(() => started.sort({ b: 1 }), /already initialized/);
	});
});

test('big sorts: by index in both directions with a limit; without one through temporary files, or error 292 if refused', async () => {
	await withCollection(async (items) => {
		const N = 60_000;
		await items.insertMany(Array.from({ length: N }, (_, i) => ({ _id: i, g: (i * 7919) % 1000, s: 'x'.repeat(2000) })));
		await items.createIndex({ g: 1 });
		const top = await items.find({}).sort({ g: -1 }).limit(5).toArray();
		assert.deepEqual(top.map((d) => d.g), [999, 999, 999, 999, 999]);
		const low = await items.find({ _id: { $gte: 30_000 } }).sort({ g: 1, _id: 1 }).limit(3).toArray();
		assert.deepEqual(low.map((d) => d.g), [0, 0, 0]);
		const byId = await items.find({ g: 5 }).sort({ _id: -1 }).limit(2).toArray();
		const withG5 = Array.from({ length: N }, (_, i) => i).filter((i) => (i * 7919) % 1000 == 5);
		assert.deepEqual(byId.map((d) => d._id), withG5.slice(-2).reverse());
		// all of them by an unindexed field: more than a sort holds in memory, the rest goes to
		// temporary files as in MongoDB 7; refused, error 292
		const all = await items.find({}).sort({ s: 1, _id: 1 }).toArray();
		assert.deepEqual(all.map((d) => d._id), Array.from({ length: N }, (_, i) => i));
		await assert.rejects(items.find({}).sort({ s: 1, _id: 1 }).allowDiskUse(false).toArray(), (e) => e.code == 292);
		await assert.rejects(items.find({}, { sort: { g: 1, s: 1 }, allowDiskUse: false }).toArray(), (e) => e.code == 292);
		// by the index, it streams
		let previous = -1;
		let n = 0;
		for await (const doc of items.find({}).sort({ g: 1 })) {
			assert.ok(doc.g >= previous);
			previous = doc.g;
			n++;
		}
		assert.equal(n, N);
	});
});

test('the first documents of a sort without an index agree with a reference: types, directions, filters, ties', async () => {
	await withCollection(async (items) => {
		// use up the shared structures, so some shapes are defined inside documents
		await items.insertMany(Array.from({ length: 40 }, (_, i) => ({ _id: `fill${i}`, [`f${i}`]: 1, n: 0 })));
		const g = generator(17);
		const docs = Array.from({ length: 5000 }, (_, i) => {
			const d = g.doc(i);
			// mostly simple values: they are placed where they are read
			if (g.random(4)) d.a = g.random(3) ? g.random(1000) - 500 : ['x', 'y', null, true, NaN, -0, new Date(g.random(3) * 1e12)][g.random(7)];
			if (g.random(10) == 0) d.nest = { v: g.random(50) };
			return d;
		});
		await items.insertMany(docs);
		const stored = await items.find({}).toArray();
		const r = generator(99);
		const pick = (list) => list[r.random(list.length)];
		// the reference's key on a dot path through embedded documents
		const at = (d, f) => f.split('.').reduce((v, s) => (v !== null && typeof v == 'object' && !Array.isArray(v) && s in v ? v[s] : undefined), d);
		const keyOf = (d, f, descending) => {
			const v = at(d, f);
			return refKey(v === undefined ? {} : { k: v }, 'k', descending);
		};
		for (let round = 0; round < 200; round++) {
			const sort = r.sortSpec();
			if (r.random(4) == 0) sort.push(['nest.v', pick([1, -1])]);
			const filter = pick([{}, {}, { n: r.random(10) }, { n: { $lt: 5 } }, { b: { $gte: 10 } }, { n: { $in: [1, 2] }, b: { $lt: 40 } }]);
			const [skip, limit] = [r.random(3) ? 0 : r.random(30), 1 + r.random(40)];
			const what = `${JSON.stringify(filter)} sort ${JSON.stringify(sort)} skip ${skip} limit ${limit}`;
			const matching = stored.filter((d) => Object.entries(filter).every(([f, c]) => {
				const v = d[f];
				if (typeof c != 'object') return v === c;
				return Object.entries(c).every(([op, x]) => {
					const t = typeof v == typeof x && v !== null;
					return op == '$lt' ? t && v < x : op == '$gte' ? t && v >= x : x.includes(v);
				});
			}));
			let expected;
			const keys = (d) => sort.map(([f, dir]) => keyOf(d, f, dir < 0));
			try {
				// a document with arrays under two of the fields cannot be sorted: MongoDB refuses
				const arrays = (d) => sort.filter(([f]) => Array.isArray(at(d, f))).length;
				if (sort.length > 1 && matching.some((d) => arrays(d) > 1)) throw new Error('parallel');
				expected = matching.map((d) => ({ d, k: keys(d) })).sort((a, b) => compareKeys(a.k, b.k, sort)).slice(skip, skip + limit);
			} catch (e) {
				if (e.message != 'parallel') throw e;
				await assert.rejects(items.find(filter).sort(Object.fromEntries(sort)).skip(skip).limit(limit).toArray(), (err) => err.code == 2, what);
				continue;
			}
			const found = await items.find(filter).sort(Object.fromEntries(sort)).skip(skip).limit(limit).toArray();
			assert.equal(found.length, expected.length, what);
			for (let i = 0; i < found.length; i++) assert.equal(compareKeys(keys(found[i]), expected[i].k, sort), 0, `${what}: position ${i}`);
		}
	});
});
