import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { open } from 'mostik';
import { tempDir } from './helpers.mjs';

test('example from the task (ESM)', async () => {
	const myDB = open(path.join(tempDir(), 'my-db'), { compression: true });
	await myDB.put('greeting', { someText: 'Hello, World!' });
	assert.equal(myDB.get('greeting').someText, 'Hello, World!');
	await myDB.close();
});

test('require() works too', async () => {
	const { open: openCjs } = createRequire(import.meta.url)('mostik');
	const db = openCjs(path.join(tempDir(), 'db'));
	assert.equal(await db.put('a', 1), true);
	assert.equal(db.get('a'), 1);
	await db.close();
});

test('get does not see a put until it is committed', async () => {
	const db = open(path.join(tempDir(), 'db'));
	const written = db.put('k', 'v');
	assert.equal(db.get('k'), undefined);
	assert.equal(await written, true);
	assert.equal(db.get('k'), 'v');
	await db.close();
});

test('writes in one event turn share a transaction and a promise', async () => {
	const db = open(path.join(tempDir(), 'db'));
	const a = db.put('k', 1);
	const b = db.remove('k');
	const c = db.put('k', 2);
	assert.equal(a, c);
	assert.deepEqual(await Promise.all([a, b, c]), [true, true, true]);
	assert.equal(db.get('k'), 2);
	await db.close();
});

test('remove', async () => {
	const db = open(path.join(tempDir(), 'db'));
	await db.put('k', 1);
	assert.equal(await db.remove('k'), true);
	assert.equal(db.get('k'), undefined);
	assert.equal(await db.remove('never-existed'), true);
	await db.close();
});

test('key types keep their identity', async () => {
	const db = open(path.join(tempDir(), 'db'));
	const keys = ['', 'text', 0, 1, -1.5, 2 ** 40, true, false, null, Symbol.for('s'), ['a', 1, true], Buffer.from([1, 2])];
	keys.forEach((k, i) => db.put(k, i));
	await db.put('last', 'x');
	keys.forEach((k, i) => assert.equal(db.get(k), i, `key ${String(k)}`));
	assert.equal(db.get('1'), undefined, 'string "1" is not number 1');
	await db.close();
});

test('value types round-trip through msgpack', async () => {
	const db = open(path.join(tempDir(), 'db'));
	const values = [
		{ nested: { list: [1, 'two', null, { three: 3 }] } },
		'string',
		42,
		3.14,
		null,
		undefined,
		true,
		[1, 2, 3],
		new Map([[1, 2]]),
		new Date(0),
		Buffer.from('bytes'),
		'x'.repeat(100_000),
	];
	values.forEach((v, i) => db.put(i, v));
	await db.put('done', true);
	values.forEach((v, i) => assert.deepEqual(db.get(i), v, `value #${i}`));
	// like lmdb-js (plain msgpack), a Set comes back as an array
	await db.put('set', new Set(['a']));
	assert.deepEqual(db.get('set'), ['a']);
	await db.close();
});

test('bad keys throw synchronously', () => {
	const db = open(path.join(tempDir(), 'db'));
	assert.throws(() => db.put(undefined, 1), /zero length key is not allowed in LMDB undefined/);
	assert.throws(() => db.put(Buffer.alloc(0), 1), /zero length key/);
	assert.throws(() => db.put('x'.repeat(1979), 1), /larger than the maximum key size \(1978\)/);
	assert.throws(() => db.remove('x'.repeat(5000)), /larger than the maximum key size \(1978\)/);
	assert.throws(() => db.put({}, 1), /Unable to serialize object as a key/);
	assert.throws(() => db.get(undefined), /A key is required for get, but is undefined/);
	assert.equal(db.get('x'.repeat(1979)), undefined);
	return db.close();
});

test('compression stores large values smaller', async () => {
	const dir = tempDir();
	const big = { text: 'abc'.repeat(2_000_000) };
	const plain = open(path.join(dir, 'plain'));
	const packed = open(path.join(dir, 'packed'), { compression: true });
	await Promise.all([plain.put('k', big), packed.put('k', big)]);
	assert.deepEqual(packed.get('k'), big);
	const size = (name) => fs.statSync(path.join(dir, name, 'data.mostik')).size;
	assert.ok(size('packed') < size('plain') / 4, `${size('packed')} vs ${size('plain')}`);
	// a database written with compression is readable without it
	await packed.close();
	const reopened = open(path.join(dir, 'packed'));
	assert.deepEqual(reopened.get('k'), big);
	await Promise.all([plain.close(), reopened.close()]);
});

test('path with an extension is a file', async () => {
	const dir = tempDir();
	const db = open(path.join(dir, 'nested', 'store.mostik'));
	await db.put('k', 1);
	assert.ok(fs.statSync(path.join(dir, 'nested', 'store.mostik')).isFile());
	await db.close();
});

test('no path gives a temporary database removed on close', async () => {
	const db = open();
	await db.put('k', 1);
	assert.equal(db.get('k'), 1);
	assert.ok(fs.existsSync(db.path));
	await db.close();
	assert.ok(!fs.existsSync(db.path));
});

test('close also flushes writes queued while it waits', async () => {
	const dir = path.join(tempDir(), 'db');
	const db = open(dir);
	const first = db.put('a', 1);
	const closing = db.close();
	// queued right after close() sees the first batch finish
	const late = first.then(() => db.put('b', 2));
	await closing;
	assert.equal(await late, true);
	const again = open(dir);
	assert.deepEqual([again.get('a'), again.get('b')], [1, 2]);
	await again.close();
});

test('a killed process leaves the last whole batch, never half of one', async () => {
	const dir = path.join(tempDir(), 'db');
	const writer = spawn(
		process.execPath,
		[
			'--input-type=module',
			'-e',
			`import { open } from 'mostik';
			const db = open(${JSON.stringify(dir)});
			for (let round = 1; ; round++) {
				for (let i = 0; i < 300; i++) db.put(i, { round, pad: 'x'.repeat(i * 10) });
				await db.put('round', round);
				if (round == 3) console.log('ready');
			}`,
		],
		{ cwd: path.dirname(fileURLToPath(import.meta.url)), stdio: ['ignore', 'pipe', 'inherit'] },
	);
	await new Promise((resolve) => writer.stdout.once('data', resolve));
	await new Promise((resolve) => setTimeout(resolve, 150));
	writer.kill('SIGKILL');
	await new Promise((resolve) => writer.once('exit', resolve));
	const db = open(dir);
	const round = db.get('round');
	assert.ok(round >= 3);
	for (let i = 0; i < 300; i++) assert.equal(db.get(i).round, round, `key ${i}`);
	await db.close();
});

test('close waits for pending writes, then rejects use', async () => {
	const dir = path.join(tempDir(), 'db');
	const db = open(dir);
	db.put('k', 'pending');
	await db.close();
	assert.throws(() => db.get('k'), /closed database/);
	assert.throws(() => db.put('k', 1), /Database is closed/);
	const again = open(dir);
	assert.equal(again.get('k'), 'pending');
	await again.close();
});

test('data survives a process restart', () => {
	const dir = path.join(tempDir(), 'db');
	const script = (code) =>
		execFileSync(process.execPath, ['--input-type=module', '-e', `import { open } from 'mostik'; const db = open(${JSON.stringify(dir)}); ${code}`], {
			cwd: path.dirname(fileURLToPath(import.meta.url)),
			encoding: 'utf8',
		});
	script(`for (let i = 0; i < 5000; i++) db.put(i, { i }); await db.put('done', true);`);
	assert.equal(script(`console.log(db.get(4999).i, db.get('done'))`).trim(), '4999 true');
});

test('a second process cannot open the same database', async () => {
	const dir = path.join(tempDir(), 'db');
	const db = open(dir);
	assert.throws(
		() =>
			execFileSync(process.execPath, ['--input-type=module', '-e', `import { open } from 'mostik'; open(${JSON.stringify(dir)})`], {
				cwd: path.dirname(fileURLToPath(import.meta.url)),
				stdio: 'pipe',
			}),
		/already open in another process/,
	);
	await db.close();
});

test('many writes across many batches', async () => {
	const db = open(path.join(tempDir(), 'db'), { compression: true });
	const expected = new Map();
	for (let round = 0; round < 50; round++) {
		for (let i = 0; i < 500; i++) {
			const key = (i * 7919 + round) % 3000;
			if (i % 5 == 0) {
				db.remove(key);
				expected.delete(key);
			} else {
				const value = { round, i, pad: 'p'.repeat((i * 31) % 3000) };
				db.put(key, value);
				expected.set(key, value);
			}
		}
		await new Promise((r) => setImmediate(r));
	}
	await db.put('flush', 1);
	for (let key = 0; key < 3000; key++) assert.deepEqual(db.get(key), expected.get(key));
	await db.close();
});
