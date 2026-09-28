import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { MostikClient, ObjectId, MongoServerError } from 'mostik';
import { tempDir } from './helpers.mjs';

const require = createRequire(import.meta.url);
const testDir = path.dirname(fileURLToPath(import.meta.url));

async function withUsers(fn) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		await fn(client.db('app').collection('users'), client);
	} finally {
		await client.close();
	}
}

/** Records how findOne reads: 'd' = scans the whole collection, 'i' = uses an index. */
function recordScans() {
	const { Storage } = require('../lib/storage.js');
	const { scan, lookup } = Storage.prototype;
	const seen = [];
	Storage.prototype.scan = function (start, ...rest) {
		seen.push(String.fromCharCode(start[0]));
		return scan.call(this, start, ...rest);
	};
	Storage.prototype.lookup = function (parts, ...rest) {
		if (rest.at(-1) == 0) seen.push(String.fromCharCode(parts[0][0]));
		return lookup.call(this, parts, ...rest);
	};
	return {
		seen,
		restore: () => Object.assign(Storage.prototype, { scan, lookup }),
	};
}

test('example: connect, collection, insertOne, findOne', async () => {
	const client = new MostikClient(path.join(tempDir(), 'db'));
	await client.connect();
	const users = client.db('app').collection('users');
	const doc = { name: 'Ann', email: 'ann@example.com' };
	const result = await users.insertOne(doc);
	assert.equal(result.acknowledged, true);
	assert.ok(result.insertedId instanceof ObjectId);
	assert.equal(doc._id, result.insertedId, 'the driver adds _id to the inserted object');
	assert.deepEqual(await users.findOne({ _id: result.insertedId }), { _id: result.insertedId, name: 'Ann', email: 'ann@example.com' });
	assert.deepEqual(Object.keys(await users.findOne({ name: 'Ann' })), ['_id', 'name', 'email']);
	assert.equal(await users.findOne({ name: 'Bob' }), null);
	await client.close();
});

test('require() works too', async () => {
	const { MostikClient: Client } = require('mostik');
	const client = await Client.connect(path.join(tempDir(), 'db'));
	await client.db().collection('c').insertOne({ _id: 1 });
	assert.deepEqual(await client.db('test').collection('c').findOne({}), { _id: 1 });
	await client.close();
});

test('custom _id values and duplicates', async () => {
	await withUsers(async (users) => {
		for (const _id of ['sku-1', 42, 0, -1.5, true, null, { region: 'eu', n: 1 }, new Date(5), new ObjectId()]) {
			await users.insertOne({ _id, tag: 'x' });
			assert.deepEqual(await users.findOne({ _id }), { _id, tag: 'x' }, `_id ${String(_id)}`);
			await assert.rejects(users.insertOne({ _id, tag: 'y' }), (error) => {
				assert.ok(error instanceof MongoServerError);
				assert.equal(error.code, 11000);
				assert.match(error.message, /^E11000 duplicate key error collection: app\.users index: _id_ dup key/);
				return true;
			});
		}
		assert.equal(await users.findOne({ _id: '42' }), null, 'string "42" is not number 42');
		assert.equal(await users.findOne({ _id: { n: 1, region: 'eu' } }), null, 'embedded documents compare in field order');
		await assert.rejects(users.insertOne({ _id: [1] }), /cannot be of type array/);
	});
});

test('duplicate _id inside one batch: exactly one insert wins', async () => {
	await withUsers(async (users) => {
		const results = await Promise.allSettled([users.insertOne({ _id: 'a', n: 1 }), users.insertOne({ _id: 'a', n: 2 })]);
		assert.deepEqual(results.map((r) => r.status), ['fulfilled', 'rejected']);
		assert.deepEqual(await users.findOne({ _id: 'a' }), { _id: 'a', n: 1 });
	});
});

test('findOne equality semantics', async () => {
	await withUsers(async (users) => {
		await users.insertOne({ _id: 1, name: 'Ann', tags: ['a', 'b'], address: { city: 'Riga', zip: '1000' }, orders: [{ sku: 'x' }, { sku: 'y' }] });
		await users.insertOne({ _id: 2, name: 'Bob', nickname: null, tags: [], born: new Date(0), avatar: Buffer.from('png') });
		const ids = async (filter) => (await users.findOne(filter))?._id ?? null;
		assert.equal(await ids({}), 1);
		assert.equal(await ids({ name: 'Bob' }), 2);
		assert.equal(await ids({ name: 'Bob', _id: 1 }), null);
		assert.equal(await ids({ tags: 'b' }), 1, 'array field matches an element');
		assert.equal(await ids({ tags: ['a', 'b'] }), 1, 'array field matches the whole array');
		assert.equal(await ids({ tags: ['b', 'a'] }), null);
		assert.equal(await ids({ tags: [] }), 2);
		assert.equal(await ids({ 'address.city': 'Riga' }), 1);
		assert.equal(await ids({ address: { city: 'Riga', zip: '1000' } }), 1);
		assert.equal(await ids({ address: { city: 'Riga' } }), null, 'embedded documents must match exactly');
		assert.equal(await ids({ 'orders.sku': 'y' }), 1, 'dot path through an array of documents');
		assert.equal(await ids({ 'tags.1': 'b' }), 1, 'numeric path picks an array position');
		assert.equal(await ids({ nickname: null, name: 'Ann' }), 1, 'null matches a missing field');
		assert.equal(await ids({ nickname: null, name: 'Bob' }), 2, 'null matches null');
		assert.equal(await ids({ born: new Date(0) }), 2);
		assert.equal(await ids({ avatar: Buffer.from('png') }), 2);
		assert.equal(await ids({ name: 'ann' }), null);
	});
});

test('unsupported queries are rejected, not answered wrongly', async () => {
	await withUsers(async (users) => {
		await assert.rejects(users.findOne({ age: { $mod: [2, 0] } }), /operator \$mod is not supported/);
		await assert.rejects(users.findOne({ $or: [] }), /\$or must be a non-empty array/);
		await assert.rejects(users.findOne({ $expr: { $eq: ['$a', 1] } }), /operator \$expr is not supported/);
		await assert.rejects(users.findOne({ a: undefined }), /undefined/);
		await assert.rejects(users.createIndex({ a: 2 }), /direction must be 1 or -1/);
	});
});

test('findOne uses _id, then an index, then a scan', async () => {
	await withUsers(async (users) => {
		for (let i = 0; i < 2000; i++) users.insertOne({ _id: i, email: `u${i}@x`, group: i % 10 });
		await users.insertOne({ _id: 'last' });
		const scans = recordScans();
		try {
			assert.equal((await users.findOne({ _id: 1500 })).email, 'u1500@x');
			assert.deepEqual(scans.seen, [], '_id is a direct lookup');
			assert.equal((await users.findOne({ email: 'u1500@x' }))._id, 1500);
			assert.deepEqual(scans.seen, ['d'], 'no index yet: collection scan');

			assert.equal(await users.createIndex({ email: 1 }), 'email_1');
			assert.equal(await users.createIndex('group'), 'group_1');
			scans.seen.length = 0;
			assert.equal((await users.findOne({ email: 'u1500@x' }))._id, 1500);
			assert.equal(await users.findOne({ email: 'nobody' }), null);
			assert.equal((await users.findOne({ group: 7, email: 'u1997@x' }))._id, 1997, 'other fields are checked after the index');
			// one lookup per query, and one per usable index for the query on two indexed fields
			assert.deepEqual(scans.seen, ['i', 'i', 'i', 'i']);
			scans.seen.length = 0;
			assert.equal((await users.findOne({ group: 3, email: 'u1997@x' })), null, 'u1997 is in group 7');
			assert.equal((await users.findOne({ group: 11, email: 'u5@x' })), null, 'no group 11 at all');
			assert.ok(!scans.seen.includes('d'), 'answered from the indexes');
		} finally {
			scans.restore();
		}
	});
});

test('indexes cover arrays, dot paths, missing fields and later inserts', async () => {
	await withUsers(async (users) => {
		await users.insertOne({ _id: 1, tags: ['red', 'blue'], address: { city: 'Riga' } });
		await users.insertOne({ _id: 2, tags: 'green' });
		await users.createIndex({ tags: 1 });
		await users.createIndex({ 'address.city': -1 });
		await users.insertOne({ _id: 3, tags: ['blue', 'blue'], address: { city: 'Oslo' } });
		const scans = recordScans();
		try {
			assert.equal((await users.findOne({ tags: 'red' }))._id, 1);
			assert.equal((await users.findOne({ tags: 'green' }))._id, 2);
			assert.equal((await users.findOne({ tags: 'blue', _id: 3 }))._id, 3);
			assert.equal((await users.findOne({ 'address.city': 'Oslo' }))._id, 3);
			assert.equal((await users.findOne({ 'address.city': null }))._id, 2, 'missing fields are indexed as null');
			assert.ok(scans.seen.every((space) => space == 'i'), scans.seen.join());
		} finally {
			scans.restore();
		}
		assert.equal(await users.createIndex({ tags: 1 }), 'tags_1', 'creating it again is a no-op');
	});
});

test('inserts made while an index is being built are indexed', async () => {
	await withUsers(async (users) => {
		for (let i = 0; i < 30_000; i++) users.insertOne({ _id: i, email: `u${i}` });
		const building = users.createIndex({ email: 1 });
		const during = [];
		for (let i = 30_000; i < 30_500; i++) during.push(users.insertOne({ _id: i, email: `u${i}` }));
		await Promise.all([building, ...during]);
		for (const i of [0, 12_345, 29_999, 30_000, 30_499]) assert.equal((await users.findOne({ email: `u${i}` }))?._id, i);
	});
});

test('collections and databases are separate', async () => {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	await client.db('a').collection('users').insertOne({ _id: 1, from: 'a.users' });
	await client.db('a').collection('users2').insertOne({ _id: 1, from: 'a.users2' });
	await client.db('b').collection('users').insertOne({ _id: 1, from: 'b.users' });
	assert.equal((await client.db('a').collection('users').findOne({})).from, 'a.users');
	assert.equal((await client.db('a').collection('users2').findOne({ _id: 1 })).from, 'a.users2');
	assert.equal((await client.db('b').collection('users').findOne({ from: 'b.users' }))._id, 1);
	assert.equal(await client.db('b').collection('users2').findOne({}), null);
	assert.throws(() => client.db('bad$name'), /Invalid database name/);
	assert.throws(() => client.db('a').collection(''), /Invalid collection name/);
	await client.close();
});

test('values round-trip', async () => {
	await withUsers(async (users) => {
		const doc = {
			_id: new ObjectId(),
			text: 'привет',
			number: 3.14,
			big: 2 ** 60,
			flag: false,
			none: null,
			date: new Date(1700000000000),
			bytes: Buffer.from([1, 2, 3]),
			nested: { list: [1, 'two', { three: 3 }], ref: new ObjectId() },
			large: 'x'.repeat(200_000),
		};
		await users.insertOne(doc);
		assert.deepEqual(await users.findOne({ _id: doc._id }), doc);
	});
});

test('compression option', async () => {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'), { compression: true });
	const docs = client.db('app').collection('docs');
	await docs.insertOne({ _id: 1, body: 'abc'.repeat(100_000) });
	assert.equal((await docs.findOne({ _id: 1 })).body.length, 300_000);
	await client.close();
});

test('two clients on one path share the database', async () => {
	const dir = path.join(tempDir(), 'db');
	const a = await MostikClient.connect(dir);
	const b = await MostikClient.connect(dir);
	await a.db('app').collection('users').insertOne({ _id: 1, email: 'x' });
	await b.db('app').collection('users').createIndex({ email: 1 });
	await a.db('app').collection('users').insertOne({ _id: 2, email: 'y' });
	assert.equal((await b.db('app').collection('users').findOne({ email: 'y' }))._id, 2);
	await a.close();
	assert.equal((await b.db('app').collection('users').findOne({ _id: 1 })).email, 'x');
	await b.close();
});

test('close waits for pending inserts; operations reconnect afterwards', async () => {
	const dir = path.join(tempDir(), 'db');
	const client = await MostikClient.connect(dir);
	const users = client.db('app').collection('users');
	const pending = users.insertOne({ _id: 1 });
	await client.close();
	await pending;
	assert.deepEqual(await users.findOne({ _id: 1 }), { _id: 1 });
	await client.close();
});

test('documents and indexes survive a process restart', () => {
	const dir = path.join(tempDir(), 'db');
	const run = (code) =>
		execFileSync(
			process.execPath,
			['--input-type=module', '-e', `import { MostikClient } from 'mostik'; const client = await MostikClient.connect(${JSON.stringify(dir)}); const users = client.db('app').collection('users'); ${code}; await client.close();`],
			{ cwd: testDir, encoding: 'utf8' },
		);
	run(`for (let i = 0; i < 3000; i++) users.insertOne({ _id: i, email: 'u' + i }); await users.createIndex({ email: 1 })`);
	assert.equal(run(`console.log((await users.findOne({ email: 'u2999' }))._id)`).trim(), '2999');
});

test('a killed process never leaves a document without its index entries', async () => {
	const dir = path.join(tempDir(), 'db');
	const writer = spawn(
		process.execPath,
		[
			'--input-type=module',
			'-e',
			`import { MostikClient } from 'mostik';
			const users = (await MostikClient.connect(${JSON.stringify(dir)})).db('app').collection('users');
			await users.createIndex({ email: 1 });
			for (let i = 0; ; i++) {
				const done = users.insertOne({ _id: i, email: 'u' + i });
				if (i % 200 == 199) await done;
				if (i == 1000) console.log('ready');
			}`,
		],
		{ cwd: testDir, stdio: ['ignore', 'pipe', 'inherit'] },
	);
	await new Promise((resolve) => writer.stdout.once('data', resolve));
	await new Promise((resolve) => setTimeout(resolve, 150));
	writer.kill('SIGKILL');
	await new Promise((resolve) => writer.once('exit', resolve));

	const client = await MostikClient.connect(dir);
	const users = client.db('app').collection('users');
	let count = 0;
	while (await users.findOne({ _id: count })) count++;
	assert.ok(count > 1000, `only ${count} documents`);
	assert.equal(await users.findOne({ _id: count + 1 }), null, 'batches commit in order');
	for (let i = 0; i < count; i++) assert.equal((await users.findOne({ email: 'u' + i }))?._id, i, `index entry for ${i}`);
	await client.close();
});

test('ObjectId', () => {
	const id = new ObjectId();
	assert.match(id.toHexString(), /^[0-9a-f]{24}$/);
	assert.ok(id.equals(new ObjectId(id.toHexString())));
	assert.ok(id.equals(id.toHexString()));
	assert.equal(JSON.stringify({ id }), `{"id":"${id}"}`);
	assert.ok(Math.abs(id.getTimestamp().getTime() - Date.now()) < 2000);
	assert.ok(ObjectId.isValid(id.toHexString()) && !ObjectId.isValid('nope') && !ObjectId.isValid(undefined));
	assert.throws(() => new ObjectId('xyz'), TypeError);
	const many = new Set(Array.from({ length: 10_000 }, () => new ObjectId().toHexString()));
	assert.equal(many.size, 10_000);
	const sorted = Array.from({ length: 100 }, () => new ObjectId().toHexString());
	assert.deepEqual([...sorted].sort(), sorted, 'ids created later sort later');
});

// Regressions found in review.

test('a failed insert does not shift the results of other writes in the same batch', async () => {
	await withUsers(async (users, client) => {
		await users.createIndex({ key: 1 });
		const other = client.db('app').collection('other');
		const results = await Promise.allSettled([
			users.insertOne({ _id: 1, key: 'k'.repeat(3000) }), // index key too large
			other.insertOne({ _id: 'x' }),
			users.insertOne({ _id: 2, tags: Array.from({ length: 60_000 }, (_, i) => i) }),
		]);
		assert.equal(results[0].status, 'rejected');
		assert.match(results[0].reason.message, /too large/);
		assert.equal(results[1].status, 'fulfilled');
		assert.equal(results[2].status, 'fulfilled');
		assert.equal(await users.findOne({ _id: 1 }), null, 'nothing of the failed insert was written');
		assert.deepEqual(await other.findOne({ _id: 'x' }), { _id: 'x' });
	});
});

test('large index builds and huge arrays do not overflow the stack', async () => {
	await withUsers(async (users) => {
		await users.insertOne({ _id: 1, tags: Array.from({ length: 100_000 }, (_, i) => i) });
		await users.createIndex({ tags: 1 });
		assert.equal((await users.findOne({ tags: 99_999 }))._id, 1);
	});
});

test('-0 and 0 are the same value everywhere', async () => {
	await withUsers(async (users) => {
		await users.createIndex({ a: 1 });
		await users.insertOne({ _id: 0 });
		await assert.rejects(users.insertOne({ _id: -0 }), (error) => error.code == 11000);
		await users.insertOne({ _id: 'n', a: -0, nested: { b: -0 } });
		assert.equal((await users.findOne({ a: 0 }))._id, 'n');
		assert.equal((await users.findOne({ a: -0 }))._id, 'n');
		assert.equal((await users.findOne({ nested: { b: 0 } }))._id, 'n');
		await users.insertOne({ _id: -0.0 === 0 ? 'z' : 'unreachable', a: 'zz' });
		assert.equal((await users.findOne({ a: 'zz' }))._id, 'z');
	});
});

test('different spellings of one path share the index registry', async () => {
	const fs = await import('node:fs');
	const base = tempDir();
	fs.mkdirSync(path.join(base, 'real'));
	fs.symlinkSync(path.join(base, 'real'), path.join(base, 'link'));
	const a = await MostikClient.connect(path.join(base, 'real'));
	const b = await MostikClient.connect(path.join(base, 'link', '..', 'link'));
	await b.db('app').collection('users').findOne({});
	await a.db('app').collection('users').createIndex({ email: 1 });
	await b.db('app').collection('users').insertOne({ _id: 1, email: 'x@y' });
	assert.equal((await a.db('app').collection('users').findOne({ email: 'x@y' }))?._id, 1);
	await Promise.all([a.close(), b.close()]);
});

test('index built before or after an insert gives the same answers', async () => {
	await withUsers(async (users, client) => {
		const late = client.db('app').collection('late');
		await users.createIndex({ v: 1 });
		const values = [new Set([1, 2]), new Map([['k', 1]]), { x: new Date(3) }, [1, [2, 3]], Buffer.from('b')];
		for (const [i, v] of values.entries()) {
			await users.insertOne({ _id: i, v });
			await late.insertOne({ _id: i, v });
		}
		await late.createIndex({ v: 1 });
		for (const [i] of values.entries()) {
			const stored = (await users.findOne({ _id: i })).v;
			assert.equal((await users.findOne({ v: stored }))?._id, i, `index before, value ${i}`);
			assert.equal((await late.findOne({ v: stored }))?._id, i, `index after, value ${i}`);
		}
	});
});

test('regular expressions match strings, not compared by equality', async () => {
	await withUsers(async (users) => {
		await users.insertOne({ name: 'Alice' });
		assert.equal((await users.findOne({ name: /^A/ }))?.name, 'Alice');
		assert.equal(await users.findOne({ name: /^B/ }), null);
	});
});

test('array filters match array elements; NaN equals NaN', async () => {
	await withUsers(async (users) => {
		await users.insertOne({ _id: 1, a: [[1, 2], 3], n: NaN });
		assert.equal((await users.findOne({ a: [1, 2] }))?._id, 1);
		assert.equal((await users.findOne({ n: NaN }))?._id, 1);
		await users.createIndex({ n: 1 });
		assert.equal((await users.findOne({ n: NaN }))?._id, 1);
	});
});

test('null matches array elements without the field, with and without an index', async () => {
	await withUsers(async (users) => {
		await users.insertOne({ _id: 1, a: [{ b: 1 }, { c: 2 }] });
		await users.insertOne({ _id: 2, a: [{ b: 1 }] });
		assert.equal((await users.findOne({ 'a.b': null }))?._id, 1);
		await users.createIndex({ 'a.b': 1 });
		assert.equal((await users.findOne({ 'a.b': null }))?._id, 1);
	});
});

test('inherited properties are not document fields', async () => {
	await withUsers(async (users) => {
		await users.insertOne({ _id: 1, x: 1, when: new Date(0) });
		assert.equal((await users.findOne({ constructor: null }))?._id, 1);
		assert.equal((await users.findOne({ 'x.toString': null }))?._id, 1);
		assert.equal((await users.findOne({ 'when.getTime': null }))?._id, 1);
	});
});

test('key elements for ObjectIds and documents do not leak memory', () => {
	const { keyElement } = require('../lib/query.js');
	const { Encoder } = require('msgpackr');
	const encoder = new Encoder();
	const encode = (value) => encoder.encode(value);
	const churn = () => {
		for (let i = 0; i < 100_000; i++) {
			keyElement(new ObjectId(), encode);
			keyElement({ k: i }, encode);
		}
	};
	churn();
	global.gc();
	const before = process.memoryUsage().heapUsed;
	churn();
	churn();
	global.gc();
	const grownMb = (process.memoryUsage().heapUsed - before) / 2 ** 20;
	assert.ok(grownMb < 3, `heap grew ${grownMb.toFixed(1)} MB for 400k key elements`);
});

test('close() waits for an index still being built; reopening sees it', async () => {
	const dir = path.join(tempDir(), 'db');
	const client = await MostikClient.connect(dir);
	const users = client.db('app').collection('users');
	for (let i = 0; i < 20_000; i++) users.insertOne({ _id: i, email: `u${i}` });
	const building = users.createIndex({ email: 1 });
	await client.close();
	await building;
	const again = await MostikClient.connect(dir);
	const reopened = again.db('app').collection('users');
	await reopened.insertOne({ _id: 'new', email: 'fresh' });
	assert.equal((await reopened.findOne({ email: 'fresh' }))?._id, 'new');
	assert.equal((await reopened.findOne({ email: 'u19999' }))?._id, 19_999);
	await again.close();
});

test('close() of one client waits for its own pending writes', async () => {
	const dir = path.join(tempDir(), 'db');
	const a = await MostikClient.connect(dir);
	const b = await MostikClient.connect(dir);
	let committed = false;
	a.db('app').collection('users').insertOne({ _id: 1 }).then(() => (committed = true));
	await a.close();
	assert.equal(committed, true);
	await b.close();
});

test('close() returns while another client keeps writing', async () => {
	const dir = path.join(tempDir(), 'db');
	const a = await MostikClient.connect(dir);
	const b = await MostikClient.connect(dir);
	const users = b.db('app').collection('users');
	let writing = true;
	const writer = (async () => {
		for (let i = 0; writing; i++) await users.insertOne({ _id: i });
	})();
	await new Promise((resolve) => setTimeout(resolve, 20));
	await a.close();
	writing = false;
	await writer;
	await b.close();
});

test('cacheSize option', async () => {
	assert.throws(() => new MostikClient(path.join(tempDir(), 'db'), { cacheSize: -1 }), /cacheSize/);
	assert.throws(() => new MostikClient(path.join(tempDir(), 'db'), { cacheSize: '64MB' }), /cacheSize/);
	// a tiny cache still answers correctly: reads just go to disk more often
	const client = await MostikClient.connect(path.join(tempDir(), 'db'), { cacheSize: 64 * 1024 });
	const users = client.db('app').collection('users');
	await users.createIndex({ email: 1 });
	for (let i = 0; i < 20_000; i++) users.insertOne({ _id: i, email: `u${i}`, pad: 'x'.repeat(100) });
	await users.insertOne({ _id: 'last' });
	for (const i of [0, 7_777, 19_999]) {
		assert.equal((await users.findOne({ _id: i })).email, `u${i}`);
		assert.equal((await users.findOne({ email: `u${i}` }))._id, i);
	}
	await client.close();
});

test('scans without an index skip non-matching documents before decoding, without losing matches', async () => {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'), { compression: { threshold: 100 } });
	const docs = client.db('app').collection('docs');
	const values = [0, -0, 7, -7, 2 ** 40, 1.5, -0.25, true, false, 'Товар 1', 'lorem ipsum 42', '', 'x'.repeat(300)];
	for (const [i, v] of values.entries()) {
		await docs.insertOne({ _id: `plain${i}`, v, pad: 'p'.repeat(i % 2 ? 500 : 0) }); // odd ones get compressed
		await docs.insertOne({ _id: `array${i}`, list: ['other', v] });
		await docs.insertOne({ _id: `nested${i}`, deep: { v } });
	}
	for (const [i, v] of values.entries()) {
		const index = values.findIndex((other) => other === v); // 0 and -0 are the same value
		assert.equal((await docs.findOne({ v }))?._id, `plain${index}`, `v = ${String(v)}`);
		assert.equal((await docs.findOne({ list: v }))?._id, `array${index}`, `list contains ${String(v)}`);
		assert.equal((await docs.findOne({ 'deep.v': v }))?._id, `nested${index}`, `deep.v = ${String(v)}`);
		assert.equal((await docs.findOne({ v, _id: `plain${i}` }))?._id, `plain${i}`);
	}
	assert.equal(await docs.findOne({ v: 'absent' }), null);
	assert.equal(await docs.findOne({ v: 8 }), null);
	await client.close();
});

test('pre-encoded key prefixes produce the same bytes as whole keys', () => {
	const { encodeKey } = require('../lib/storage.js');
	const { keyElement } = require('../lib/query.js');
	const { Encoder } = require('msgpackr');
	const encoder = new Encoder();
	const encode = (value) => encoder.encode(value);
	const prefix = encodeKey(['d', 'app', 'users']);
	for (const value of ['', 'x', 'lorem ipsum', '\u0000a', 0, -1.5, 2 ** 60, true, false, null, new ObjectId(), { a: [1] }, 'я'.repeat(100)]) {
		const element = keyElement(value, encode);
		assert.deepEqual(encodeKey([prefix, element]), encodeKey(['d', 'app', 'users', element]), String(value));
		assert.deepEqual(encodeKey([prefix, 'email', element, 1]), encodeKey(['d', 'app', 'users', 'email', element, 1]));
	}
});
