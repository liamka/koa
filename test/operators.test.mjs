// Comparison operators: $gt, $gte, $lt, $lte, $in, $ne
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

// ---- a reference written apart from lib/query.js: MongoDB semantics for these documents ----

function kind(v) {
	if (typeof v == 'number') return 'number';
	if (typeof v == 'string') return 'string';
	if (typeof v == 'boolean') return 'boolean';
	if (v instanceof ObjectId) return 'objectId';
	if (v instanceof Date) return 'date';
	return 'other';
}

function order(a, b) {
	if (kind(a) != kind(b) || kind(b) == 'other') return undefined;
	switch (kind(b)) {
		case 'number':
			if (Number.isNaN(a) || Number.isNaN(b)) return Number.isNaN(a) && Number.isNaN(b) ? 0 : undefined;
			return a < b ? -1 : a > b ? 1 : 0;
		case 'string':
			return Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));
		case 'boolean':
			return Number(a) - Number(b);
		case 'objectId':
			return Math.sign(a.toHexString().localeCompare(b.toHexString()));
		case 'date':
			return a.getTime() < b.getTime() ? -1 : a.getTime() > b.getTime() ? 1 : 0;
	}
}

function same(a, b) {
	if (a instanceof ObjectId || b instanceof ObjectId) return a instanceof ObjectId && b instanceof ObjectId && a.toHexString() == b.toHexString();
	if (a instanceof Date || b instanceof Date) return a instanceof Date && b instanceof Date && a.getTime() == b.getTime();
	if (Array.isArray(a) || Array.isArray(b)) return Array.isArray(a) && Array.isArray(b) && a.length == b.length && a.every((x, i) => same(x, b[i]));
	if (typeof a == 'number' && typeof b == 'number') return a == b || (Number.isNaN(a) && Number.isNaN(b));
	return a === b;
}

/** Values at `path` (top level, or one level down: 'nest.v'), and whether it is missing. */
function at(doc, path) {
	const [head, tail] = path.split('.');
	if (!(head in doc)) return { values: [], missing: true };
	const v = doc[head];
	if (tail === undefined) return { values: [v], missing: false };
	if (v !== null && typeof v == 'object' && !Array.isArray(v) && !(v instanceof ObjectId) && !(v instanceof Date))
		return tail in v ? { values: [v[tail]], missing: false } : { values: [], missing: true };
	return { values: [], missing: true };
}

function eq(doc, path, value) {
	const { values, missing } = at(doc, path);
	if (value === null && (missing || values.some((v) => v === null || (Array.isArray(v) && v.includes(null))))) return true;
	return values.some((v) => same(v, value) || (Array.isArray(v) && v.some((x) => same(x, value))));
}

function holds(doc, path, op, operand) {
	if (op == '$in') return operand.some((value) => eq(doc, path, value));
	if (op == '$ne') return !eq(doc, path, operand);
	if (op == '$nin') return !operand.some((value) => eq(doc, path, value));
	if (op == '$exists') return at(doc, path).values.length > 0 == Boolean(operand);
	if (operand === null) return (op == '$gte' || op == '$lte') && eq(doc, path, null);
	const test = (v) => {
		const o = order(v, operand);
		if (o === undefined) return false;
		return op == '$gt' ? o > 0 : op == '$gte' ? o >= 0 : op == '$lt' ? o < 0 : o <= 0;
	};
	return at(doc, path).values.some((v) => test(v) || (Array.isArray(v) && v.some(test)));
}

function reference(doc, filter) {
	return Object.entries(filter).every(([path, cond]) =>
		path == '$and'
			? cond.every((part) => reference(doc, part))
			: path == '$or'
			? cond.some((part) => reference(doc, part))
			: cond !== null && typeof cond == 'object' && !(cond instanceof ObjectId) && !(cond instanceof Date) && Object.keys(cond)[0]?.startsWith('$')
			? Object.entries(cond).every(([op, operand]) => holds(doc, path, op, operand))
			: eq(doc, path, cond),
	);
}

// ---- random documents and filters ----

function generator(seed) {
	// mulberry32: integer arithmetic stays exact
	const next = () => {
		seed = (seed + 0x6d2b79f5) | 0;
		let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32;
	};
	const random = (n) => Math.floor(next() * n);
	const numbers = [0, 1, -1, 2, 3, 7, 10, 64, 100, 127, 128, 255, 256, 1e6, -1e6, 1.5, -0.25, 2 ** 40, -(2 ** 40), 1e300, -Infinity, Infinity, NaN];
	const strings = ['', 'a', 'ab', 'b', 'B', 'z', 'Ω', 'я', '￿', '\u{10000}', '\u{1F600}', 'a\u0001', 'x'.repeat(40)];
	const ids = [0, 1, 2, 3].map((i) => new ObjectId(`65000000000000000000000${i}`));
	const dates = [new Date(0), new Date(1e12), new Date(2e12)];
	const scalar = () => {
		switch (random(8)) {
			case 0:
			case 1:
			case 2:
				return numbers[random(numbers.length)];
			case 3:
			case 4:
				return strings[random(strings.length)];
			case 5:
				return [true, false, null][random(3)];
			case 6:
				return ids[random(ids.length)];
			default:
				return dates[random(dates.length)];
		}
	};
	const value = () => (random(4) == 0 ? Array.from({ length: random(4) }, scalar) : scalar());
	const doc = (i) => {
		const d = { _id: random(5) == 0 ? `s${i}` : i };
		if (random(8)) d.a = value();
		if (random(8)) d.b = scalar();
		if (random(3)) d.nest = random(5) ? { v: scalar() } : scalar();
		return d;
	};
	const operand = () => scalar();
	const condition = () => {
		const c = {};
		const ops = ['$gt', '$gte', '$lt', '$lte', '$in', '$ne'];
		for (let k = 0, n = 1 + random(2); k < n; k++) {
			const op = ops[random(ops.length)];
			if (op == '$in') c.$in = Array.from({ length: random(4) }, operand);
			else c[op] = operand();
		}
		// comparisons of the same type most of the time, so ranges are not always empty
		if (random(2) && '$gt' in c && '$lt' in c && kind(c.$gt) != kind(c.$lt)) c.$lt = typeof c.$gt == 'number' ? c.$gt + 50 : c.$gt;
		return c;
	};
	const filter = () => {
		const f = {};
		const paths = ['a', 'b', 'nest.v', '_id'];
		for (let k = 0, n = 1 + random(2); k < n; k++) {
			const p = paths[random(paths.length)];
			f[p] = random(4) == 0 ? scalar() : p == '_id' && random(2) ? { [['$gt', '$gte', '$lt', '$lte'][random(4)]]: random(3) ? random(400) : `s${random(400)}` } : condition();
		}
		return f;
	};
	// $and / $or of conditions (with $nin and $exists too), nested
	const logical = (depth = 0) => {
		const f = {};
		const paths = ['a', 'b', 'nest.v', '_id'];
		for (let k = 0, n = 1 + random(2); k < n; k++) {
			const kind = random(depth < 2 ? 6 : 4);
			if (kind == 4) f.$or = Array.from({ length: 1 + random(3) }, () => logical(depth + 1));
			else if (kind == 5) f.$and = Array.from({ length: 1 + random(2) }, () => logical(depth + 1));
			else {
				const p = paths[random(paths.length)];
				f[p] = pick([
					() => scalar(),
					() => condition(),
					() => ({ $nin: Array.from({ length: random(3) }, scalar) }),
					() => ({ $exists: random(2) == 0 }),
					() => (p == '_id' ? { $in: [random(400), `s${random(400)}`, random(400)] } : { $exists: true, $ne: scalar() }),
				])();
			}
		}
		return f;
	};
	return { doc, filter, logical };
}

const pick = (list) => list[Math.floor(Math.random() * list.length)];

const show = (filter) =>
	JSON.stringify(filter, (_, v) => (typeof v == 'number' && !Number.isFinite(v) ? String(v) : v instanceof ObjectId ? `ObjectId(${v.toHexString()})` : v));

test('operators agree with a reference on random documents: by scan, by index, by _id', async () => {
	await withCollection(async (items) => {
		const { doc, filter } = generator(11);
		const docs = Array.from({ length: 400 }, (_, i) => doc(i));
		await items.insertMany(docs);
		const stored = await items.find({}).toArray();
		const check = async (label) => {
			let nonEmpty = 0;
			const { filter: next } = generator(label.length * 7919);
			for (let round = 0; round < 700; round++) {
				const f = next();
				const expected = stored.filter((d) => reference(d, f)).map((d) => String(d._id)).sort();
				nonEmpty += expected.length > 0;
				const found = (await items.find(f).toArray()).map((d) => String(d._id)).sort();
				assert.deepEqual(found, expected, `${label}: find ${show(f)}`);
				assert.equal(await items.countDocuments(f), expected.length, `${label}: count ${show(f)}`);
				const one = await items.findOne(f);
				assert.equal(one === null, expected.length == 0, `${label}: findOne ${show(f)}`);
			}
			assert.ok(nonEmpty > 150, `${label}: only ${nonEmpty} filters matched anything`);
		};
		await check('scan');
		await items.createIndex({ a: 1 }); // multikey: some values are arrays
		await items.createIndex({ b: 1 });
		await items.createIndex({ 'nest.v': 1 });
		await check('indexes');
	});
});

test('comparisons stay within a type; null, missing fields and NaN follow MongoDB', async () => {
	await withCollection(async (items) => {
		await items.insertMany([
			{ _id: 1, v: 5 },
			{ _id: 2, v: '5' },
			{ _id: 3, v: null },
			{ _id: 4 },
			{ _id: 5, v: [1, 9] },
			{ _id: 6, v: NaN },
			{ _id: 7, v: true },
			{ _id: 8, v: new Date(1000) },
		]);
		const ids = async (filter) => (await items.find(filter).toArray()).map((d) => d._id).sort((a, b) => a - b);
		for (const indexed of [false, true]) {
			if (indexed) await items.createIndex({ v: 1 });
			assert.deepEqual(await ids({ v: { $gt: 4 } }), [1, 5]);
			assert.deepEqual(await ids({ v: { $gt: 4, $lt: 6 } }), [1, 5], 'each bound may hold for another element');
			assert.deepEqual(await ids({ v: { $gte: '5' } }), [2]);
			assert.deepEqual(await ids({ v: { $gt: 4, $lt: '9' } }), []);
			assert.deepEqual(await ids({ v: { $gte: null } }), [3, 4]);
			assert.deepEqual(await ids({ v: { $lt: null } }), []);
			assert.deepEqual(await ids({ v: { $ne: null } }), [1, 2, 5, 6, 7, 8]);
			assert.deepEqual(await ids({ v: { $ne: 9 } }), [1, 2, 3, 4, 6, 7, 8], '$ne excludes arrays holding the value');
			assert.deepEqual(await ids({ v: { $in: [null, '5'] } }), [2, 3, 4]);
			assert.deepEqual(await ids({ v: { $in: [] } }), []);
			assert.deepEqual(await ids({ v: { $gte: NaN } }), [6]);
			assert.deepEqual(await ids({ v: { $gt: -Infinity } }), [1, 5]);
			assert.deepEqual(await ids({ v: { $gte: false } }), [7]);
			assert.deepEqual(await ids({ v: { $lt: new Date(2000) } }), [8]);
			assert.equal(await items.countDocuments({ v: { $gt: 0 } }), 2, 'an array with two matching elements counts once');
			assert.deepEqual(await ids({ v: { $gt: 0 } }), [1, 5], 'and comes out once');
		}
	});
});

test('_id ranges and $in read documents by key; results are the same with other conditions', async () => {
	await withCollection(async (items) => {
		const oids = Array.from({ length: 5 }, (_, i) => new ObjectId(`6500000000000000000000${i}0`));
		await items.insertMany([
			...Array.from({ length: 50 }, (_, i) => ({ _id: i, n: i % 5 })),
			...['a', 'b', 'c'].map((s) => ({ _id: s, n: 9 })),
			...oids.map((id, i) => ({ _id: id, n: i })),
		]);
		const ids = async (filter) => (await items.find(filter).toArray()).map((d) => (d._id instanceof ObjectId ? d._id.toHexString() : d._id));
		assert.deepEqual(await ids({ _id: { $gte: 45 } }), [45, 46, 47, 48, 49]);
		assert.deepEqual(await ids({ _id: { $gt: 10, $lte: 12 } }), [11, 12]);
		assert.deepEqual(await ids({ _id: { $lt: 'b' } }), ['a']);
		assert.deepEqual(await ids({ _id: { $gt: oids[2] } }), [oids[3].toHexString(), oids[4].toHexString()]);
		assert.deepEqual(await ids({ _id: { $in: [3, 'c', 999, 3] } }), [3, 'c']);
		assert.deepEqual(await ids({ _id: { $in: [1, 2, 3, 4] }, n: { $ne: 2 } }), [1, 3, 4]);
		assert.deepEqual(await ids({ _id: { $gt: 40 }, n: 0 }), [45]);
		assert.equal(await items.countDocuments({ _id: { $lt: 20 } }), 20);
		assert.equal(await items.countDocuments({ _id: { $gte: 20, $lt: 1e9 } }), 30);
	});
});

test('updates and deletes take operator filters; upsert ignores operator conditions', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ n: 1 });
		await items.insertMany(Array.from({ length: 100 }, (_, i) => ({ _id: i, n: i })));
		assert.equal((await items.updateMany({ n: { $gte: 90 } }, { $set: { top: true } })).modifiedCount, 10);
		assert.equal((await items.deleteMany({ n: { $lt: 10 } })).deletedCount, 10);
		assert.equal((await items.deleteOne({ n: { $in: [50, 51] } })).deletedCount, 1);
		assert.equal(await items.countDocuments({ n: { $gte: 0 } }), 89);
		assert.equal(await items.countDocuments({ top: true, n: { $ne: 95 } }), 9);
		const up = await items.updateOne({ n: { $gt: 1000 }, kind: 'new' }, { $set: { v: 1 } }, { upsert: true });
		assert.deepEqual(await items.findOne({ _id: up.upsertedId }), { _id: up.upsertedId, kind: 'new', v: 1 });
	});
});

test('bad operator filters are rejected', async () => {
	await withCollection(async (items) => {
		await assert.rejects(items.findOne({ a: { $in: 5 } }), /\$in needs an array/);
		await assert.rejects(items.findOne({ a: { $gt: {} } }), /compares/);
		await assert.rejects(items.findOne({ a: { $gt: [1] } }), /compares/);
		await assert.rejects(items.findOne({ a: { $gt: 1, b: 2 } }), /unknown operator: b/);
		await assert.rejects(items.findOne({ a: { $mod: [2, 0] } }), /\$mod is not supported/);
		await assert.rejects(items.findOne({ a: { $in: [{ $gt: 1 }] } }), /values only/);
	});
});

test('$and, $or, $nin, $exists agree with a reference: scan, indexes, unions of index ranges', async () => {
	await withCollection(async (items) => {
		const { doc } = generator(23);
		const docs = Array.from({ length: 400 }, (_, i) => doc(i));
		await items.insertMany(docs);
		const stored = await items.find({}).toArray();
		const check = async (label) => {
			const { logical } = generator(label.length * 131);
			let nonEmpty = 0;
			for (let round = 0; round < 500; round++) {
				const f = logical();
				const expected = stored.filter((d) => reference(d, f)).map((d) => String(d._id)).sort();
				nonEmpty += expected.length > 0;
				const found = (await items.find(f).toArray()).map((d) => String(d._id));
				assert.deepEqual([...found].sort(), expected, `${label}: find ${show(f)}`);
				assert.equal(new Set(found).size, found.length, `${label}: each document once ${show(f)}`);
				assert.equal(await items.countDocuments(f), expected.length, `${label}: count ${show(f)}`);
				assert.equal((await items.findOne(f)) === null, expected.length == 0, `${label}: findOne ${show(f)}`);
				const counted = await items.aggregate([{ $match: f }, { $group: { _id: null, n: { $sum: 1 } } }]).toArray();
				assert.equal(counted[0]?.n ?? 0, expected.length, `${label}: aggregate ${show(f)}`);
			}
			assert.ok(nonEmpty > 100, `${label}: only ${nonEmpty} filters matched anything`);
		};
		await check('scan');
		await items.createIndex({ a: 1 });
		await items.createIndex({ b: 1 });
		await items.createIndex({ 'nest.v': 1 });
		await check('indexes');
	});
});

test('$or over indexed fields reads index ranges; bad logical filters are rejected', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ g: 1 });
		await items.createIndex({ tags: 1 });
		await items.insertMany(Array.from({ length: 1000 }, (_, i) => ({ _id: i, g: i % 10, tags: [i % 3, i % 7], h: i % 2 })));
		const ids = async (filter) => (await items.find(filter).toArray()).map((d) => d._id).sort((a, b) => a - b);
		// documents found by both alternatives come out once
		assert.equal((await ids({ $or: [{ g: 1 }, { tags: 1 }] })).length, 1000 - [...Array(1000).keys()].filter((i) => i % 10 != 1 && i % 3 != 1 && i % 7 != 1).length);
		assert.deepEqual(await ids({ $or: [{ _id: 5 }, { _id: { $gte: 998 } }, { g: 99 }] }), [5, 998, 999]);
		assert.deepEqual(await ids({ $or: [{ g: 99 }, { tags: 99 }] }), []);
		assert.equal(await items.countDocuments({ $or: [{ g: 3 }, { g: 4 }], h: 1 }), 100);
		assert.equal(await items.countDocuments({ $and: [{ g: { $in: [1, 2] } }, { g: { $nin: [2] } }] }), 100);
		assert.equal(await items.countDocuments({ g: { $exists: false } }), 0);
		assert.equal(await items.countDocuments({ nope: { $exists: false } }), 1000);
		const up = await items.updateOne({ $or: [{ _id: 5000 }], kind: 'x' }, { $set: { v: 1 } }, { upsert: true });
		assert.deepEqual(await items.findOne({ _id: up.upsertedId }), { _id: up.upsertedId, kind: 'x', v: 1 }, 'upsert takes no $or');
		await assert.rejects(items.findOne({ $or: {} }), /non-empty array/);
		await assert.rejects(items.findOne({ $and: [1] }), /full objects/);
		await assert.rejects(items.findOne({ a: { $nin: 1 } }), /\$nin needs an array/);
	});
});
