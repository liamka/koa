// sorts and groups that do not fit in memory go through temporary files
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { MostikClient, ObjectId } from 'mostik';
import { tempDir } from './helpers.mjs';

const { setLimits } = createRequire(import.meta.url)('../lib/spill.js');

async function withCollection(fn) {
	const dir = path.join(tempDir(), 'db');
	const client = await MostikClient.connect(dir);
	try {
		await fn(client.db('app').collection('items'), dir);
	} finally {
		await client.close();
	}
}

/** Runs `fn` twice: with room for everything in memory, then with a few kilobytes only. */
async function inMemoryAndOnDisk(fn) {
	const memory = await fn();
	const previous = setLimits({ own: 4 << 10, shared: 1 << 30 });
	try {
		return [memory, await fn()];
	} finally {
		setLimits(previous);
	}
}

const docs = (n) => {
	let seed = 3;
	const random = (k) => {
		seed = (seed * 16807) % 2147483647;
		return seed % k;
	};
	const pick = (list) => list[random(list.length)];
	return Array.from({ length: n }, (_, i) => {
		const d = { _id: i, g: random(40), n: random(1000) };
		// values of every kind the sort orders
		d.v = pick([() => random(100) - 50, () => pick(['a', 'b', 'ab', '']), () => null, () => [], () => [random(9), random(9)], () => new Date(random(3) * 1e12), () => new ObjectId(), () => ({ x: random(3) }), () => pick([true, false])])();
		if (random(5) == 0) delete d.v;
		d.pad = 'x'.repeat(random(200));
		return d;
	});
};

test('find().sort() through temporary files gives what a sort in memory gives', async () => {
	await withCollection(async (items, dir) => {
		await items.insertMany(docs(3000));
		for (const [filter, sort, skip, limit] of [
			[{}, { v: 1, _id: 1 }, 0, 0],
			[{}, { v: -1, n: 1, _id: -1 }, 0, 0],
			[{ g: { $lt: 20 } }, { n: -1, _id: 1 }, 17, 400],
			[{}, { pad: 1, _id: 1 }, 0, 2500],
			[{ n: { $gte: 500 } }, { v: 1, g: -1, _id: 1 }, 3, 0],
		]) {
			const run = () => items.find(filter).sort(sort).skip(skip).limit(limit).toArray();
			const [memory, disk] = await inMemoryAndOnDisk(run);
			assert.deepEqual(disk.map((d) => d._id), memory.map((d) => d._id), JSON.stringify({ filter, sort, skip, limit }));
			assert.ok(memory.length > 0);
		}
		// refused: error 292
		const previous = setLimits({ own: 4 << 10, shared: 1 << 30 });
		try {
			await assert.rejects(items.find({}).sort({ n: 1 }).allowDiskUse(false).toArray(), (e) => e.code == 292);
			await assert.rejects(items.aggregate([{ $sort: { n: 1 } }], { allowDiskUse: false }).toArray(), (e) => e.code == 292);
			await assert.rejects(items.aggregate([{ $group: { _id: '$_id' } }], { allowDiskUse: false }).toArray(), (e) => e.code == 292);
		} finally {
			setLimits(previous);
		}
		// no temporary file is left behind, a cursor given up on included
		const cursor = items.find({}).sort({ v: 1 });
		const previousLimits = setLimits({ own: 4 << 10, shared: 1 << 30 });
		try {
			await cursor.next();
			await cursor.close();
		} finally {
			setLimits(previousLimits);
		}
		assert.deepEqual(fs.readdirSync(dir).filter((name) => name.includes('-spill-')), []);
	});
});

test('$sort and $group through temporary files give what they give in memory', async () => {
	await withCollection(async (items) => {
		await items.insertMany(docs(3000));
		const pipelines = [
			[{ $sort: { v: 1, _id: 1 } }],
			[{ $match: { g: { $lt: 30 } } }, { $sort: { n: -1, _id: 1 } }, { $skip: 5 }, { $limit: 300 }],
			[{ $group: { _id: '$g', n: { $sum: 1 }, s: { $sum: '$n' }, a: { $avg: '$n' }, mn: { $min: '$v' }, mx: { $max: '$v' }, f: { $first: '$n' }, l: { $last: '$n' }, p: { $push: '$n' }, set: { $addToSet: '$v' }, c: { $count: {} } } }, { $sort: { _id: 1 } }],
			[{ $group: { _id: '$_id', n: { $sum: '$n' } } }, { $sort: { _id: 1 } }],
			[{ $group: { _id: { g: '$g', odd: { $literal: 1 } }, ids: { $push: '$_id' } } }, { $sort: { _id: 1 } }],
			// a sort on _id alone compares whole values: an order with no ties
			[{ $group: { _id: '$v', n: { $sum: 1 } } }, { $sort: { _id: 1 } }],
			[{ $project: { _id: 0, n: 1, v: 1 } }, { $sort: { n: 1, v: -1 } }, { $limit: 1000 }],
		];
		for (const pipeline of pipelines) {
			const [memory, disk] = await inMemoryAndOnDisk(() => items.aggregate(pipeline).toArray());
			assert.deepEqual(disk, memory, JSON.stringify(pipeline));
			assert.ok(memory.length > 0);
		}
		// a group that alone passes the limit for one accumulator: refused, as MongoDB does
		const big = 'y'.repeat(1 << 20);
		await items.insertMany(Array.from({ length: 110 }, (_, i) => ({ _id: `big${i}`, g: 'big', big })));
		await assert.rejects(items.aggregate([{ $group: { _id: '$g', all: { $push: '$big' } } }]).toArray(), (e) => e.code == 146);
	});
});
