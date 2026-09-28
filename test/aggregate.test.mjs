// aggregate: $match, $group, $count
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { MostikClient, ObjectId, MongoServerError, AggregationCursor } from 'mostik';
import { tempDir } from './helpers.mjs';

async function withCollection(fn) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		await fn(client.db('app').collection('items'));
	} finally {
		await client.close();
	}
}

const byId = (a, b) => (JSON.stringify(a._id) < JSON.stringify(b._id) ? -1 : JSON.stringify(a._id) > JSON.stringify(b._id) ? 1 : 0);

test('accumulators follow MongoDB: missing and null values, types, first and last', async () => {
	await withCollection(async (items) => {
		const oid = new ObjectId('650000000000000000000001');
		await items.insertMany([
			{ _id: 1, k: 'a', v: 5, s: 'x', o: { p: 1 }, t: [1, 2] },
			{ _id: 2, k: 'a', v: 2.5, s: null, o: { p: 2 } },
			{ _id: 3, k: 'a', v: 'text', s: oid },
			{ _id: 4, k: 'b', o: { p: 1 } },
			{ _id: 5, k: 'b', v: null },
			{ _id: 6, v: 1 },
			{ _id: 7, k: null, v: 2 },
		]);
		const out = await items
			.aggregate([
				{
					$group: {
						_id: '$k',
						n: { $sum: 1 },
						c: { $count: {} },
						sum: { $sum: '$v' },
						avg: { $avg: '$v' },
						min: { $min: '$v' },
						max: { $max: '$v' },
						minS: { $min: '$s' },
						first: { $first: '$v' },
						last: { $last: '$v' },
						push: { $push: '$v' },
						set: { $addToSet: '$o' },
						firstT: { $first: '$t' },
						lit: { $first: { $literal: '$v' } },
					},
				},
			])
			.toArray();
		out.sort(byId);
		assert.deepEqual(out, [
			// null and missing k are one group
			{ _id: null, n: 2, c: 2, sum: 3, avg: 1.5, min: 1, max: 2, minS: null, first: 1, last: 2, push: [1, 2], set: [], firstT: null, lit: '$v' },
			{ _id: 'a', n: 3, c: 3, sum: 7.5, avg: 3.75, min: 2.5, max: 'text', minS: 'x', first: 5, last: 'text', push: [5, 2.5, 'text'], set: [{ p: 1 }, { p: 2 }], firstT: [1, 2], lit: '$v' },
			{ _id: 'b', n: 2, c: 2, sum: 0, avg: null, min: null, max: null, minS: null, first: null, last: null, push: [null], set: [{ p: 1 }], firstT: null, lit: '$v' },
		].sort(byId));
	});
});

test('group keys: equal values share a group; paths, documents, constants', async () => {
	await withCollection(async (items) => {
		await items.insertMany([
			{ _id: 1, n: 1, a: { b: 'x', c: 1 } },
			{ _id: 2, n: 1.0, a: { b: 'x', c: 2 } },
			{ _id: 3, n: -0, a: { b: 'y' } },
			{ _id: 4, n: 0, a: [{ b: 'z' }, { b: 'w' }] },
			{ _id: 5, n: NaN },
			{ _id: 6, n: NaN },
			{ _id: 7, n: '1' },
		]);
		const groups = async (id) => (await items.aggregate([{ $group: { _id: id, ids: { $push: '$_id' } } }]).toArray()).map((g) => [g._id, g.ids]);
		const byNumber = await groups('$n');
		assert.equal(byNumber.length, 4, 'numbers 1 and 1.0 together, -0 with 0, NaN with NaN, "1" apart');
		assert.deepEqual(byNumber.find(([id]) => id === 1)[1], [1, 2]);
		assert.deepEqual(byNumber.find(([id]) => Object.is(id, -0) || id === 0)[1], [3, 4]);
		assert.deepEqual(byNumber.find(([id]) => Number.isNaN(id))[1], [5, 6]);
		const byPath = await groups('$a.b');
		assert.deepEqual(byPath.find(([id]) => id === 'x')[1], [1, 2]);
		assert.deepEqual(byPath.find(([id]) => Array.isArray(id))[0], ['z', 'w'], 'through an array: the array of values');
		assert.deepEqual(byPath.find(([id]) => id === null)[1], [5, 6, 7]);
		const compound = await groups({ b: '$a.b', missing: '$nope' });
		assert.deepEqual(compound.find(([id]) => id.b === 'x'), [{ b: 'x' }, [1, 2]], 'missing fields are left out of the key');
		assert.deepEqual(await groups(7), [[7, [1, 2, 3, 4, 5, 6, 7]]]);
	});
});

test('$count and $match, before and after $group; zero documents give nothing', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ g: 1 });
		await items.insertMany(Array.from({ length: 5000 }, (_, i) => ({ _id: i, g: i % 10, p: i % 7 })));
		const run = (pipeline) => items.aggregate(pipeline).toArray();
		assert.deepEqual(await run([{ $match: { g: 3 } }, { $count: 'n' }]), [{ n: 500 }]);
		assert.deepEqual(await run([{ $match: { p: { $lt: 2 } } }, { $count: 'n' }]), [{ n: 1430 }]);
		assert.deepEqual(await run([{ $count: 'all' }]), [{ all: 5000 }]);
		assert.deepEqual(await run([{ $match: { g: 99 } }, { $count: 'n' }]), []);
		assert.deepEqual(await run([{ $group: { _id: '$p' } }, { $count: 'groups' }]), [{ groups: 7 }]);
		const big = await run([{ $match: { g: { $in: [1, 2] } } }, { $group: { _id: '$g', n: { $sum: 1 }, p: { $max: '$p' } } }, { $match: { n: { $gte: 500 } } }]);
		assert.deepEqual(big.sort(byId), [{ _id: 1, n: 500, p: 6 }, { _id: 2, n: 500, p: 6 }]);
		assert.deepEqual(await run([{ $match: { g: 1 } }, { $match: { p: 3 } }, { $count: 'n' }]), [{ n: 71 }]);
		assert.deepEqual(await run([]), await items.find({}).toArray(), 'an empty pipeline returns the documents');
	});
});

test('aggregate agrees with a reference on random documents, with and without indexes', async () => {
	await withCollection(async (items) => {
		let seed = 3;
		const random = (n) => {
			seed = (seed * 16807) % 2147483647;
			return seed % n;
		};
		const pick = (list) => list[random(list.length)];
		const docs = Array.from({ length: 2000 }, (_, i) => {
			const d = { _id: i, g: random(12), h: pick(['x', 'y', null, 3, true]), v: random(3) ? random(1000) / 4 : pick(['s', null, [1, 2]]) };
			if (random(4)) d.nest = { k: random(5), w: random(9) };
			// more shapes than are shared: some documents define their own structures
			if (random(20) == 0) d[`extra${random(60)}`] = { deep: random(3) };
			return d;
		});
		await items.insertMany(docs);
		const reference = (filter, key) => {
			const groups = new Map();
			for (const d of docs) {
				if (filter && !filter(d)) continue;
				const k = key(d);
				const id = JSON.stringify(k ?? null);
				const g = groups.get(id) ?? { _id: k ?? null, n: 0, sum: 0, cnt: 0, max: null, set: new Set() };
				g.n++;
				if (typeof d.v == 'number') {
					g.sum += d.v;
					g.cnt++;
				}
				if (typeof d.nest?.w == 'number' && (g.max === null || d.nest.w > g.max)) g.max = d.nest.w;
				g.set.add(d.g);
				groups.set(id, g);
			}
			return [...groups.values()].map((g) => ({ _id: g._id, n: g.n, sum: g.sum, avg: g.cnt ? g.sum / g.cnt : null, max: g.max, gs: g.set.size })).sort(byId);
		};
		const cases = [
			[{}, null, '$g', (d) => d.g],
			[{ g: 3 }, (d) => d.g == 3, '$h', (d) => d.h],
			[{ g: { $in: [1, 5, 7] } }, (d) => [1, 5, 7].includes(d.g), '$nest.k', (d) => d.nest?.k],
			[{ h: 'x' }, (d) => d.h == 'x', null, () => null],
			[{ v: { $gte: 100 } }, (d) => typeof d.v == 'number' && d.v >= 100, { g: '$g', k: '$nest.k' }, (d) => (d.nest ? { g: d.g, k: d.nest.k } : { g: d.g })],
		];
		const check = async (label) => {
			for (const [match, filter, id, key] of cases) {
				const pipeline = [...(Object.keys(match).length ? [{ $match: match }] : []), { $group: { _id: id, n: { $sum: 1 }, sum: { $sum: '$v' }, avg: { $avg: '$v' }, max: { $max: '$nest.w' }, gs: { $addToSet: '$g' } } }];
				const got = (await items.aggregate(pipeline).toArray()).map((g) => ({ ...g, gs: g.gs.length })).sort(byId);
				assert.deepEqual(got, reference(filter, key), `${label}: ${JSON.stringify(pipeline)}`);
			}
		};
		await check('scan');
		await items.createIndex({ g: 1 });
		await items.createIndex({ h: 1 });
		await check('indexes');
	});
});

test('aggregation cursor, and pipelines that are rejected', async () => {
	await withCollection(async (items) => {
		await items.insertMany([{ k: 1 }, { k: 2 }, { k: 2 }, { k: 3 }]);
		const cursor = items.aggregate([{ $group: { _id: '$k', n: { $sum: 1 } } }]);
		assert.ok(cursor instanceof AggregationCursor);
		const seen = [];
		for await (const g of cursor) seen.push(g._id);
		assert.deepEqual(seen.sort(), [1, 2, 3]);
		assert.deepEqual(await items.aggregate([{ $group: { _id: '$k', n: { $sum: 1 } } }]).sort({ n: -1, _id: 1 }).limit(2).toArray(), [
			{ _id: 2, n: 2 },
			{ _id: 1, n: 1 },
		]);
		const code = (n) => (e) => e instanceof MongoServerError && e.code == n;
		assert.throws(() => items.aggregate({ $match: {} }), /array/);
		assert.throws(() => items.aggregate([{ $match: {}, $count: 'n' }]), code(40323));
		assert.throws(() => items.aggregate([{ match: {} }]), code(40324));
		assert.throws(() => items.aggregate([{ $lookup: {} }]), /\$lookup is not supported/);
		assert.throws(() => items.aggregate([{ $group: { n: { $sum: 1 } } }]), code(15955));
		assert.throws(() => items.aggregate([{ $group: { _id: null, n: 1 } }]), code(40234));
		assert.throws(() => items.aggregate([{ $group: { _id: null, 'a.b': { $sum: 1 } } }]), code(40235));
		assert.throws(() => items.aggregate([{ $group: { _id: null, n: { $median: '$k' } } }]), /\$median is not supported/);
		assert.throws(() => items.aggregate([{ $group: { _id: { $add: ['$k', 1] } } }]), /\$add is not supported/);
		assert.throws(() => items.aggregate([{ $count: '$n' }]), code(40158));
		assert.throws(() => items.aggregate([{ $count: 'a.b' }]), code(40160));
		assert.throws(() => items.aggregate([{ $group: { _id: null, n: { $count: { x: 1 } } } }]), code(5362100));
	});
});

test('documents whose kept fields use a structure defined in another field are read whole', async () => {
	await withCollection(async (items) => {
		// use up the shared structures, so new shapes are defined inside each document
		await items.insertMany(Array.from({ length: 40 }, (_, i) => ({ _id: `fill${i}`, [`f${i}`]: 1 })));
		// { q: .. } is defined in `a`, then used again in `b`
		await items.insertMany(Array.from({ length: 20 }, (_, i) => ({ _id: i, a: { q: i % 2 }, b: { q: i % 3 } })));
		const out = await items.aggregate([{ $match: { _id: { $gte: 0 } } }, { $group: { _id: '$b.q', n: { $sum: 1 } } }]).toArray();
		assert.deepEqual(out.sort(byId), [{ _id: 0, n: 7 }, { _id: 1, n: 7 }, { _id: 2, n: 6 }]);
	});
});
