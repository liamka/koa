// aggregate stages: $sort, $skip, $limit, $project, $unwind
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { MostikClient, MongoServerError } from 'mostik';
import { tempDir } from './helpers.mjs';

async function withCollection(fn) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		await fn(client.db('app').collection('items'));
	} finally {
		await client.close();
	}
}

test('$project: fields kept or left out, embedded documents and arrays, computed fields', async () => {
	await withCollection(async (items) => {
		await items.insertOne({ _id: 1, a: { b: 1, c: 2 }, d: [{ b: 1, c: 1 }, { c: 2 }, 3, [{ b: 4 }]], e: 5, f: 'x' });
		const run = async (spec) => (await items.aggregate([{ $project: spec }]).toArray())[0];
		assert.deepEqual(await run({ 'a.b': 1, 'd.b': 1 }), { _id: 1, a: { b: 1 }, d: [{ b: 1 }, {}, [{ b: 4 }]] });
		assert.deepEqual(await run({ a: { c: 1 }, e: true, _id: 0 }), { a: { c: 2 }, e: 5 });
		assert.deepEqual(await run({ 'a.b': 0, 'd.c': 0, e: 0 }), { _id: 1, a: { c: 2 }, d: [{ b: 1 }, {}, 3, [{ b: 4 }]], f: 'x' });
		assert.deepEqual(await run({ _id: 0 }), { a: { b: 1, c: 2 }, d: [{ b: 1, c: 1 }, { c: 2 }, 3, [{ b: 4 }]], e: 5, f: 'x' });
		assert.deepEqual(await run({ _id: 1 }), { _id: 1 });
		assert.deepEqual(await run({ _id: 0, x: '$a.b', y: { $literal: '$e' }, z: 'text', n: null, list: ['$e', '$nope'], nope: '$nope' }), { x: 1, y: '$e', z: 'text', n: null, list: [5, null] });
		// fields named keep the document's order; computed ones come after
		assert.deepEqual(Object.keys(await run({ z: '$e', f: 1, a: 1 })), ['_id', 'a', 'f', 'z']);
		assert.deepEqual(await run({ a: { b: 1, k: '$e' }, 'q.r': '$f' }), { _id: 1, a: { b: 1, k: 5 }, q: { r: 'x' } });
		assert.deepEqual(await run({ whole: '$$ROOT.a', e: 1 }), { _id: 1, e: 5, whole: { b: 1, c: 2 } });
	});
});

test('$unwind: arrays, options, values that are not arrays, embedded paths', async () => {
	await withCollection(async (items) => {
		await items.insertMany([{ _id: 1, t: [1, 2] }, { _id: 2, t: [] }, { _id: 3, t: null }, { _id: 4 }, { _id: 5, t: 7 }, { _id: 6, o: { t: ['x', 'y'] } }]);
		const run = (stage) => items.aggregate([{ $unwind: stage }]).toArray();
		assert.deepEqual(await run('$t'), [{ _id: 1, t: 1 }, { _id: 1, t: 2 }, { _id: 5, t: 7 }]);
		assert.deepEqual(await run({ path: '$t', includeArrayIndex: 'i', preserveNullAndEmptyArrays: true }), [
			{ _id: 1, t: 1, i: 0 },
			{ _id: 1, t: 2, i: 1 },
			{ _id: 2, i: null },
			{ _id: 3, t: null, i: null },
			{ _id: 4, i: null },
			{ _id: 5, t: 7, i: null },
			{ _id: 6, o: { t: ['x', 'y'] }, i: null },
		]);
		assert.deepEqual(await run('$o.t'), [{ _id: 6, o: { t: 'x' } }, { _id: 6, o: { t: 'y' } }]);
		// then grouped: one output per element
		const counts = await items.aggregate([{ $unwind: '$t' }, { $group: { _id: null, n: { $sum: 1 }, s: { $sum: '$t' } } }]).toArray();
		assert.deepEqual(counts, [{ _id: null, n: 3, s: 10 }]);
	});
});

test('$sort, $skip, $limit agree with a reference, with and without indexes, with other stages', async () => {
	await withCollection(async (items) => {
		let seed = 5;
		const random = (n) => {
			seed = (seed * 16807) % 2147483647;
			return seed % n;
		};
		const docs = Array.from({ length: 3000 }, (_, i) => {
			const d = { _id: i, g: random(10), s: ['a', 'b', 'c', 'd'][random(4)], tags: Array.from({ length: random(3) }, () => random(5)) };
			if (random(10)) d.n = random(100);
			return d;
		});
		await items.insertMany(docs);
		// missing n sorts first ascending (as null), numbers after
		const key = (d, f) => (d[f] === undefined ? -Infinity : d[f]);
		const by = (spec) => (a, b) => {
			for (const [f, dir] of Object.entries(spec)) {
				const [x, y] = [key(a, f), key(b, f)];
				if (x < y) return -dir;
				if (x > y) return dir;
			}
			return 0;
		};
		const cases = [];
		for (const spec of [{ n: 1, _id: 1 }, { n: -1, _id: -1 }, { s: 1, n: -1, _id: 1 }, { g: -1, n: 1, _id: 1 }, { _id: -1 }]) {
			for (const [match, test] of [[null, () => true], [{ g: 3 }, (d) => d.g == 3], [{ n: { $gte: 50 } }, (d) => d.n >= 50]]) {
				for (const [skip, limit] of [[0, 0], [0, 7], [20, 5], [2990, 50], [0, 1]]) cases.push({ spec, match, test, skip, limit });
			}
		}
		const check = async (label) => {
			for (const { spec, match, test, skip, limit } of cases) {
				const pipeline = [...(match ? [{ $match: match }] : []), { $sort: spec }, ...(skip ? [{ $skip: skip }] : []), ...(limit ? [{ $limit: limit }] : [])];
				let want = docs.filter(test).sort(by(spec)).slice(skip);
				if (limit) want = want.slice(0, limit);
				const got = await items.aggregate(pipeline).toArray();
				assert.deepEqual(got.map((d) => d._id), want.map((d) => d._id), `${label}: ${JSON.stringify(pipeline)}`);
			}
			// stages after the sort, and a sort after other stages
			const top = await items.aggregate([{ $sort: { n: -1, _id: 1 } }, { $project: { _id: 0, n: 1 } }, { $limit: 3 }]).toArray();
			assert.deepEqual(top, docs.sort(by({ n: -1, _id: 1 })).slice(0, 3).map((d) => ({ n: d.n })));
			const unwound = await items.aggregate([{ $unwind: '$tags' }, { $sort: { tags: -1, _id: 1 } }, { $skip: 1 }, { $limit: 2 }, { $project: { tags: 1 } }]).toArray();
			const flat = docs.flatMap((d) => d.tags.map((t) => ({ _id: d._id, tags: t }))).sort(by({ tags: -1, _id: 1 }));
			assert.deepEqual(unwound, flat.slice(1, 3));
			const groups = await items.aggregate([{ $group: { _id: '$g', n: { $sum: 1 } } }, { $sort: { n: -1, _id: 1 } }, { $limit: 3 }]).toArray();
			const counted = [...Map.groupBy(docs, (d) => d.g)].map(([g, list]) => ({ _id: g, n: list.length })).sort(by({ n: -1, _id: 1 }));
			assert.deepEqual(groups, counted.slice(0, 3));
			assert.deepEqual((await items.aggregate([{ $skip: 2998 }]).toArray()).length, 2);
			assert.deepEqual((await items.aggregate([{ $match: { g: 1 } }, { $limit: 4 }, { $count: 'n' }]).toArray()), [{ n: 4 }]);
		};
		await check('scan');
		await items.createIndex({ n: -1 });
		await items.createIndex({ g: 1, n: 1 });
		await check('indexes');
	});
});

test('$sort of all documents is not held to the 100 MB of find', async () => {
	await withCollection(async (items) => {
		await items.insertMany(Array.from({ length: 60_000 }, (_, i) => ({ _id: i, k: (i * 7919) % 60_000, s: 'x'.repeat(2000) })));
		let previous = -1;
		let n = 0;
		for await (const doc of items.aggregate([{ $sort: { k: 1 } }])) {
			assert.ok(doc.k > previous);
			previous = doc.k;
			n++;
		}
		assert.equal(n, 60_000);
	});
});

test('bad $sort, $skip, $limit, $project and $unwind stages are rejected', async () => {
	await withCollection(async (items) => {
		const code = (n) => (e) => e instanceof MongoServerError && e.code == n;
		const bad = (stage, n) => assert.throws(() => items.aggregate([stage]), code(n), JSON.stringify(stage));
		bad({ $sort: {} }, 15976);
		bad({ $sort: 1 }, 15973);
		bad({ $sort: { a: 2 } }, 15975);
		bad({ $limit: 0 }, 15958);
		bad({ $limit: 1.5 }, 5107201);
		bad({ $skip: -1 }, 5107200);
		bad({ $skip: 'x' }, 5107200);
		bad({ $project: {} }, 51272);
		bad({ $project: { a: 1, b: 0 } }, 31254);
		bad({ $project: { a: 0, b: 1 } }, 31253);
		bad({ $project: { a: 0, b: '$x' } }, 31253);
		bad({ $project: { a: 1, 'a.b': 1 } }, 31250);
		bad({ $project: { a: {} } }, 51270);
		bad({ $unwind: 't' }, 28818);
		bad({ $unwind: { path: '$t', other: 1 } }, 28811);
		bad({ $unwind: { includeArrayIndex: 'i' } }, 28812);
		bad({ $unwind: { path: '$t', includeArrayIndex: '$i' } }, 28822);
		bad({ $unwind: { path: '$t', preserveNullAndEmptyArrays: 1 } }, 28809);
		assert.throws(() => items.aggregate([{ $project: { a: { $add: [1, 2] } } }]), /\$add is not supported/);
		assert.throws(() => items.aggregate([{ $sort: { a: { $meta: 'textScore' } } }]), /\$meta is not supported/);
	});
});

test('$sort on _id alone compares whole values, as MongoDB does; other sorts take array elements', async () => {
	await withCollection(async (items) => {
		await items.insertMany([{ _id: 1, v: [0] }, { _id: 2, v: 3 }, { _id: 3, v: 1 }, { _id: 4, v: [] }, { _id: 5, v: null }, { _id: 6, v: [5, -1] }, { _id: 7 }]);
		const ids = async (pipeline) => (await items.aggregate(pipeline).toArray()).map((d) => d._id);
		assert.deepEqual(await ids([{ $group: { _id: '$v' } }, { $sort: { _id: 1 } }]), [null, 1, 3, [], [0], [5, -1]]);
		assert.deepEqual(await ids([{ $group: { _id: '$v' } }, { $sort: { _id: -1 } }]), [[5, -1], [0], [], 3, 1, null]);
		assert.deepEqual(await ids([{ $group: { _id: '$v', n: { $sum: 1 } } }, { $sort: { n: 1, _id: 1 } }]), [[], [5, -1], [0], 1, 3, null]);
		assert.deepEqual(await ids([{ $sort: { v: 1, _id: 1 } }]), [4, 5, 7, 6, 1, 3, 2]);
		assert.deepEqual(await ids([{ $group: { _id: '$_id', w: { $first: '$v' } } }, { $sort: { w: -1, _id: 1 } }]), [6, 2, 3, 1, 5, 7, 4]);
	});
});

test('field paths in expressions and sorts by parallel arrays, as MongoDB 7 answers', async () => {
	await withCollection(async (items) => {
		const shapes = [[[{ x: 1 }], { x: 2 }, 3, [[{ x: 4 }]], []], [[]], [{ x: [] }], [{ x: [[5]] }], [[1, 2]], { x: [1] }, [{ y: 1 }], [], [{ x: { z: 1 } }]];
		await items.insertMany(shapes.map((a, _id) => ({ _id, a })));
		const values = async (expr) => (await items.aggregate([{ $project: { v: expr } }, { $sort: { _id: 1 } }]).toArray()).map((d) => d.v);
		// MongoDB 7.0's answers. Arrays met on the way: their documents only, the rest of the path
		// in each of them
		assert.deepEqual(await values('$a.x'), [[2], [], [[]], [[[5]]], [], [1], [], [], [{ z: 1 }]]);
		assert.deepEqual(await values('$a.x.z'), [[], [], [[]], [[]], [], [], [], [], [1]]);
		await items.deleteMany({});
		await items.insertMany([{ _id: 1, a: [1, 2], b: [3] }, { _id: 2, a: { x: [1], y: [2] } }, { _id: 3, a: [{ b: 1, c: 2 }] }]);
		const parallel = (e) => e instanceof MongoServerError && e.code == 2 && /parallel arrays/.test(e.message);
		await assert.rejects(items.find({ _id: 1 }).sort({ a: 1, b: 1 }).toArray(), parallel);
		await assert.rejects(items.aggregate([{ $sort: { 'a.x': 1, 'a.y': 1 } }]).toArray(), parallel);
		// the same array: fine
		assert.equal((await items.find({ _id: 3 }).sort({ 'a.b': 1, 'a.c': 1 }).toArray()).length, 1);
		assert.equal((await items.find({ _id: 1 }).sort({ a: 1, 'a.0': 1 }).toArray()).length, 1);
	});
});
