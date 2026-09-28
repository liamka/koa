// createIndex: unique and compound indexes
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { MostikClient, MongoServerError, MongoBulkWriteError } from 'mostik';
import { tempDir } from './helpers.mjs';

async function withCollection(fn) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		await fn(client.db('app').collection('users'), client);
	} finally {
		await client.close();
	}
}

const duplicate = (keyValue) => (e) => e.code == 11000 && (!keyValue || JSON.stringify(e.keyValue) == JSON.stringify(keyValue));

test('a unique index refuses a second document with the same value, whatever the write', async () => {
	await withCollection(async (users) => {
		assert.equal(await users.createIndex({ email: 1 }, { unique: true }), 'email_1');
		await users.insertOne({ _id: 1, email: 'a@x', login: 'a' });
		await assert.rejects(users.insertOne({ _id: 2, email: 'a@x' }), (e) => e instanceof MongoServerError && duplicate({ email: 'a@x' })(e) && /index: email_1 dup key: \{ email: "a@x" \}/.test(e.message));
		// insertMany: ordered stops at the duplicate, the ones before it are written
		await assert.rejects(users.insertMany([{ _id: 3, email: 'c@x' }, { _id: 4, email: 'c@x' }, { _id: 5, email: 'e@x' }]), (e) => e instanceof MongoBulkWriteError && e.code == 11000);
		assert.deepEqual((await users.find({}).toArray()).map((d) => d._id), [1, 3]);
		await assert.rejects(users.updateOne({ _id: 3 }, { $set: { email: 'a@x' } }), duplicate({ email: 'a@x' }));
		await assert.rejects(users.updateOne({ _id: 9 }, { $set: { email: 'a@x' } }, { upsert: true }), duplicate({ email: 'a@x' }));
		// a document keeps its own value; a freed value can be taken
		await users.updateOne({ _id: 1 }, { $set: { email: 'a@x', n: 1 } });
		await users.updateOne({ _id: 1 }, { $set: { email: 'b@x' } });
		await users.insertOne({ _id: 2, email: 'a@x' });
		await users.deleteOne({ _id: 2 });
		await users.insertOne({ _id: 6, email: 'a@x' });
		// updateMany giving every document the same value: the first gets it
		await assert.rejects(users.updateMany({}, { $set: { email: 'z@x' } }), duplicate({ email: 'z@x' }));
		assert.equal(await users.countDocuments({ email: 'z@x' }), 1);
		const all = await users.find({}).toArray();
		assert.equal(new Set(all.map((d) => d.email)).size, all.length);
	});
});

test('unique: missing is null (one allowed), arrays hold each element, 1 and 1.0 are equal', async () => {
	await withCollection(async (users) => {
		await users.createIndex({ login: 1 }, { unique: true });
		await users.insertOne({ _id: 1 });
		await assert.rejects(users.insertOne({ _id: 2 }), duplicate({ login: null }));
		await assert.rejects(users.insertOne({ _id: 2, login: null }), duplicate({ login: null }));
		await users.insertOne({ _id: 3, login: ['x', 'y'] });
		await assert.rejects(users.insertOne({ _id: 4, login: 'y' }), duplicate({ login: 'y' }));
		// one document may repeat its own element
		await users.insertOne({ _id: 5, login: ['z', 'z'] });
		await users.insertOne({ _id: 6, login: 1 });
		await assert.rejects(users.insertOne({ _id: 7, login: 1.0 }), duplicate());
		await users.insertOne({ _id: 8, login: '1' });
		assert.equal(await users.countDocuments({}), 5);
	});
});

test('building a unique index over duplicates fails and leaves no index; options conflict', async () => {
	await withCollection(async (users) => {
		await users.insertMany(Array.from({ length: 3000 }, (_, i) => ({ _id: i, email: `u${i}`, g: i % 10 })));
		await users.updateOne({ _id: 2500 }, { $set: { email: 'u17' } });
		await assert.rejects(users.createIndex({ email: 1 }, { unique: true }), duplicate({ email: 'u17' }));
		assert.deepEqual((await users.listIndexes().toArray()).map((i) => i.name), ['_id_']);
		await users.updateOne({ _id: 2500 }, { $set: { email: 'u2500' } });
		await users.createIndex({ email: 1 }, { unique: true });
		assert.equal(await users.createIndex({ email: 1 }, { unique: true }), 'email_1');
		await assert.rejects(users.createIndex({ email: 1 }), (e) => e.code == 85);
		await assert.rejects(users.createIndex({ email: 1 }, { unique: 'yes' }), TypeError);
		assert.deepEqual(await users.listIndexes().toArray(), [
			{ v: 2, key: { _id: 1 }, name: '_id_' },
			{ v: 2, key: { email: 1 }, name: 'email_1', unique: true },
		]);
		await assert.rejects(users.insertOne({ email: 'u5' }), duplicate({ email: 'u5' }));
		assert.equal((await users.findOne({ email: 'u2999' }))._id, 2999);
	});
});

test('unique index on several fields: only the whole tuple must be new', async () => {
	await withCollection(async (users) => {
		assert.equal(await users.createIndex({ org: 1, login: -1 }, { unique: true }), 'org_1_login_-1');
		await users.insertMany([{ org: 'a', login: 'x' }, { org: 'b', login: 'x' }, { org: 'a', login: 'y' }]);
		await assert.rejects(users.insertOne({ org: 'a', login: 'x' }), (e) => duplicate({ org: 'a', login: 'x' })(e) && JSON.stringify(e.keyPattern) == '{"org":1,"login":-1}');
		await users.insertOne({ org: 'a' });
		await assert.rejects(users.insertOne({ org: 'a', login: null }), duplicate({ org: 'a', login: null }));
	});
});

test('unique survives reopening', async () => {
	const dir = path.join(tempDir(), 'db');
	let client = await MostikClient.connect(dir);
	await client.db('app').collection('users').createIndex({ email: 1 }, { unique: true });
	await client.db('app').collection('users').insertOne({ email: 'a' });
	await client.close();
	client = await MostikClient.connect(dir);
	try {
		await assert.rejects(client.db('app').collection('users').insertOne({ email: 'a' }), duplicate({ email: 'a' }));
	} finally {
		await client.close();
	}
});

test('parallel concurrent inserts of one value: exactly one wins', async () => {
	await withCollection(async (users) => {
		await users.createIndex({ email: 1 }, { unique: true });
		const results = await Promise.allSettled(Array.from({ length: 50 }, (_, i) => users.insertOne({ _id: i, email: 'same' })));
		assert.equal(results.filter((r) => r.status == 'fulfilled').length, 1);
		assert.ok(results.filter((r) => r.status == 'rejected').every((r) => r.reason.code == 11000));
		assert.equal(await users.countDocuments({}), 1);
	});
});

// ---- compound indexes: filter and sort through them, checked against a reference ----

const rank = (v) => (v === null || v === undefined ? 1 : typeof v == 'number' ? 2 : typeof v == 'string' ? 3 : typeof v == 'boolean' ? 8 : 4);
const cmp = (a, b) => rank(a) - rank(b) || (a < b ? -1 : a > b ? 1 : 0);

test('compound index: equality on the first field, range and sort on the second, any directions', async () => {
	await withCollection(async (items) => {
		let seed = 11;
		const random = (n) => {
			seed = (seed * 16807) % 2147483647;
			return seed % n;
		};
		const docs = Array.from({ length: 4000 }, (_, i) => {
			const d = { _id: i, a: random(8), n: random(5) };
			if (random(10)) d.b = random(6) ? random(200) : ['s', 't', null, true][random(4)];
			return d;
		});
		await items.insertMany(docs);
		const cases = [
			[{ a: 3 }, { b: 1 }],
			[{ a: 3 }, { b: -1 }],
			[{ a: 5, b: { $gte: 50, $lt: 120 } }, { b: 1 }],
			[{ a: 5, b: { $gt: 150 } }, { b: -1 }],
			[{}, { a: 1, b: -1 }],
			[{}, { a: -1, b: 1 }],
			[{}, { a: -1, b: -1 }],
			[{ n: 2 }, { a: 1, b: 1 }],
			[{ a: { $in: [1, 2] } }, { a: -1, b: 1 }],
			[{ a: 7, n: { $ne: 1 } }, { b: -1 }],
			[{ b: { $lt: 20 } }, { a: 1 }],
		];
		const check = async (label) => {
			for (const [filter, sort] of cases) {
				const expected = docs
					.filter((d) => Object.entries(filter).every(([f, c]) => {
						const v = d[f];
						if (typeof c != 'object') return v === c;
						return Object.entries(c).every(([op, x]) => {
							const same = typeof v == 'number';
							return op == '$gte' ? same && v >= x : op == '$gt' ? same && v > x : op == '$lt' ? same && v < x : op == '$ne' ? v !== x : x.includes(v);
						});
					}))
					.sort((x, y) => Object.entries(sort).reduce((o, [f, dir]) => o || cmp(x[f], y[f]) * dir, 0) || x._id - y._id);
				const what = `${label}: ${JSON.stringify(filter)} ${JSON.stringify(sort)}`;
				for (const limit of [0, 7]) {
					const found = await items.find(filter).sort(sort).limit(limit).toArray();
					const want = limit ? expected.slice(0, limit) : expected;
					assert.equal(found.length, want.length, what);
					const key = (d) => Object.keys(sort).map((f) => d[f] ?? null);
					assert.deepEqual(found.map(key), want.map(key), `${what} limit ${limit}`);
				}
				assert.equal(await items.countDocuments(filter), expected.length, what);
			}
		};
		await check('scan');
		assert.equal(await items.createIndex({ a: 1, b: -1 }), 'a_1_b_-1');
		await check('index');
		// the index serves writes after it is built
		for (let i = 0; i < 300; i++) {
			const d = { _id: 4000 + i, a: random(8), n: random(5), b: random(200) };
			docs.push(d);
			await items.insertOne(d);
		}
		for (const d of docs.filter((d) => d.a == 3).slice(0, 40)) {
			d.b = 999;
			await items.updateOne({ _id: d._id }, { $set: { b: 999 } });
		}
		await items.deleteMany({ a: 6 });
		docs.splice(0, docs.length, ...docs.filter((d) => d.a != 6));
		await check('after writes');
	});
});

test('compound index: sort by index order reads few documents with a limit', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ user: 1, at: -1 });
		await items.insertMany(Array.from({ length: 50_000 }, (_, i) => ({ _id: i, user: i % 100, at: i })));
		const last = await items.find({ user: 42 }).sort({ at: -1 }).limit(3).toArray();
		assert.deepEqual(last.map((d) => d.at), [49942, 49842, 49742]);
		const first = await items.find({ user: 42 }).sort({ at: 1 }).limit(2).toArray();
		assert.deepEqual(first.map((d) => d.at), [42, 142]);
		const top = await items.find({}).sort({ user: -1, at: 1 }).limit(2).toArray();
		assert.deepEqual(top.map((d) => [d.user, d.at]), [[99, 99], [99, 199]]);
		assert.equal(await items.countDocuments({ user: 42, at: { $gte: 40_000 } }), 100);
	});
});

test('compound index: parallel arrays refused, list and drop by pattern', async () => {
	await withCollection(async (items) => {
		await items.createIndex({ a: 1, b: 1 });
		await items.insertOne({ a: [1, 2], b: 3 });
		await assert.rejects(items.insertOne({ a: [1, 2], b: [3, 4] }), (e) => e.code == 171);
		assert.deepEqual((await items.find({ a: 2, b: 3 }).toArray()).length, 1);
		assert.deepEqual((await items.listIndexes().toArray())[1], { v: 2, key: { a: 1, b: 1 }, name: 'a_1_b_1' });
		await items.dropIndex({ a: 1, b: 1 });
		assert.equal((await items.listIndexes().toArray()).length, 1);
		await items.insertOne({ a: [1, 2], b: [3, 4] });
		await assert.rejects(items.createIndex({ a: 1, b: 1 }), (e) => e.code == 171);
		assert.equal((await items.listIndexes().toArray()).length, 1);
	});
});

test('compound index with no equality on its first field: its values are skipped through, counts stay exact', async () => {
	await withCollection(async (items) => {
		let seed = 5;
		const random = (n) => {
			seed = (seed * 16807) % 2147483647;
			return seed % n;
		};
		const docs = Array.from({ length: 4000 }, (_, i) => {
			const d = { _id: i, n: random(6) };
			if (random(12)) d.g = random(3) ? random(30) : ['a', 'b', null, true][random(4)];
			if (random(10)) d.p = random(8);
			return d;
		});
		await items.insertMany(docs);
		const has = (d, f) => Object.hasOwn(d, f);
		const cases = [
			[{ g: { $ne: 7 }, p: { $in: [1, 2, 3] } }, (d) => d.g !== 7 && [1, 2, 3].includes(d.p)],
			[{ g: { $gte: 10 }, p: 5 }, (d) => typeof d.g == 'number' && d.g >= 10 && d.p === 5],
			[{ g: { $nin: [1, 2, 'a'] }, p: 0 }, (d) => ![1, 2, 'a'].includes(d.g) && d.p === 0],
			[{ g: { $exists: false }, p: 4 }, (d) => !has(d, 'g') && d.p === 4],
			[{ g: { $type: 'string' }, p: { $in: [6, 7] } }, (d) => typeof d.g == 'string' && [6, 7].includes(d.p)],
			[{ g: { $lt: 0 }, p: 1 }, () => false],
			[{ g: { $ne: null }, p: 2, n: 3 }, (d) => has(d, 'g') && d.g !== null && d.p === 2 && d.n === 3],
		];
		const check = async (label) => {
			for (const [filter, test] of cases) {
				const want = docs.filter(test).map((d) => d._id).sort((a, b) => a - b);
				assert.deepEqual((await items.find(filter).toArray()).map((d) => d._id).sort((a, b) => a - b), want, `${label} ${JSON.stringify(filter)}`);
				assert.equal(await items.countDocuments(filter), want.length, `${label} count ${JSON.stringify(filter)}`);
			}
		};
		await check('scan');
		await items.createIndex({ g: 1, p: -1 });
		await check('compound index');
	});
});
