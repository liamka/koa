// work done off the JS thread gives what the JS side gives: $group, sorts; the event loop turns
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { createRequire } from 'node:module';
import { MostikClient, ObjectId } from 'mostik';
import { tempDir } from './helpers.mjs';

const { setLimits } = createRequire(import.meta.url)('../lib/spill.js');

async function withCollection(fn) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		await fn(client.db('app').collection('items'));
	} finally {
		await client.close();
	}
}

/** `fn` natively, then with the native layer given no memory (the JS side does the work). */
async function nativelyAndNot(fn) {
	const native = await fn();
	const previous = setLimits({ native: 0 });
	try {
		return [native, await fn()];
	} finally {
		setLimits(previous);
	}
}

function docs(n) {
	let seed = 11;
	const random = (k) => {
		seed = (seed * 16807) % 2147483647;
		return seed % k;
	};
	const pick = (list) => list[random(list.length)];
	const ids = [1, 2, 3].map((i) => new ObjectId(`65000000000000000000000${i}`));
	return Array.from({ length: n }, (_, i) => {
		const d = { _id: i, g: random(7), n: random(100) / 8, s: pick(['a', 'b', 'c']) };
		// every kind of plain value, missing ones too
		const v = pick([() => random(50) - 25, () => random(1000) / 7, () => pick(['x', 'yy', '']), () => null, () => pick([true, false]), () => new Date(random(4) * 1e11), () => pick(ids), () => pick([NaN, -0, 0, 0.1, 0.2])])();
		if (random(6)) d.v = v;
		if (random(3)) d.o = { k: random(4), w: pick(['p', 'q', null]) };
		return d;
	});
}

test('$group off the JS thread gives what the JS side gives', async () => {
	await withCollection(async (items) => {
		await items.insertMany(docs(4000));
		const byId = (list) => [...list].sort((a, b) => (JSON.stringify(a._id) < JSON.stringify(b._id) ? -1 : 1));
		const specs = [
			{ _id: '$g', n: { $sum: 1 }, s: { $sum: '$n' }, a: { $avg: '$n' }, mn: { $min: '$v' }, mx: { $max: '$v' }, f: { $first: '$v' }, l: { $last: '$v' }, c: { $count: {} }, k: { $sum: 2.5 } },
			{ _id: '$v', n: { $sum: 1 }, a: { $avg: '$v' }, s: { $sum: '$v' } },
			{ _id: '$o.k', w: { $first: '$o.w' }, x: { $max: '$o.w' }, m: { $min: '$s' } },
			{ _id: null, s: { $sum: '$v' }, a: { $avg: '$n' }, f: { $first: '$s' }, l: { $last: '$o' } },
			{ _id: '$missing', n: { $count: {} } },
		];
		for (const spec of specs) {
			for (const match of [{}, { g: { $lt: 4 } }, { s: 'b', n: { $gte: 3 } }]) {
				const pipeline = [...(Object.keys(match).length ? [{ $match: match }] : []), { $group: spec }];
				const [native, js] = await nativelyAndNot(() => items.aggregate(pipeline).toArray());
				assert.deepEqual(byId(native), byId(js), JSON.stringify(pipeline));
				assert.ok(native.length > 0);
			}
		}
		// by an index too
		await items.createIndex({ g: 1 });
		const pipeline = [{ $match: { g: 3 } }, { $group: { _id: '$s', n: { $sum: 1 }, m: { $max: '$n' } } }];
		const [native, js] = await nativelyAndNot(() => items.aggregate(pipeline).toArray());
		assert.deepEqual(byId(native), byId(js));
	});
});

test('sorts off the JS thread give what the JS side gives, through temporary files too', async () => {
	await withCollection(async (items) => {
		await items.insertMany(docs(4000));
		for (const [filter, sort, limit] of [
			[{}, { v: 1, _id: 1 }, 0],
			[{}, { v: -1, n: 1, _id: 1 }, 20_000],
			[{ g: { $lt: 3 } }, { s: -1, n: -1, _id: -1 }, 0],
			[{}, { 'o.w': 1, _id: -1 }, 0],
		]) {
			const run = () => items.find(filter).sort(sort).limit(limit).toArray();
			const [native, js] = await nativelyAndNot(run);
			assert.deepEqual(native.map((d) => d._id), js.map((d) => d._id), JSON.stringify({ filter, sort }));
			// runs on disk: a few kilobytes of memory
			const previous = setLimits({ own: 4 << 10, shared: 1 << 30, native: 4 << 10 });
			try {
				assert.deepEqual((await run()).map((d) => d._id), js.map((d) => d._id), `on disk ${JSON.stringify({ filter, sort })}`);
			} finally {
				setLimits(previous);
			}
		}
	});
});

test('long reads give the event loop turns', async () => {
	await withCollection(async (items) => {
		await items.insertMany(Array.from({ length: 200_000 }, (_, i) => ({ _id: i, g: i % 100, s: 'x'.repeat(50) + i })));
		let longest = 0;
		let last = performance.now();
		const timer = setInterval(() => {
			const now = performance.now();
			longest = Math.max(longest, now - last);
			last = now;
		}, 1);
		const previous = setLimits({ own: 1 << 20, shared: 1 << 30, native: 1 << 20 });
		try {
			last = performance.now();
			let n = 0;
			for await (const _ of items.find({}).sort({ s: -1 })) n++;
			assert.equal(n, 200_000);
			assert.equal((await items.aggregate([{ $group: { _id: '$s' } }]).toArray()).length, 200_000);
			assert.equal(await items.countDocuments({ s: { $regex: '9$' } }), 20_000);
		} finally {
			setLimits(previous);
			clearInterval(timer);
		}
		assert.ok(longest < 250, `the event loop waited ${longest.toFixed(0)} ms`);
	});
});
