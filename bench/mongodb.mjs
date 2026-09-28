// The benchmark on MongoDB: MONGOD=/path/to/mongod node mongodb.mjs  (N=1000000 for a quicker
// run; DIR: where the data goes). Starts its own mongod with default settings.
import { MongoClient } from 'mongodb';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { run } from './scenario.mjs';
import { recorder, du, peakMemory } from './common.mjs';

const N = Number(process.env.N ?? 10_000_000);
const PORT = Number(process.env.PORT ?? 27999);
const dir = process.env.DIR ?? fs.mkdtempSync(path.join(os.tmpdir(), 'mongodb-bench-'));
fs.rmSync(dir, { recursive: true, force: true });
fs.mkdirSync(dir, { recursive: true });

const mongod = spawn(process.env.MONGOD ?? 'mongod', ['--dbpath', dir, '--port', String(PORT), '--bind_ip', '127.0.0.1', '--quiet'], { stdio: 'ignore' });
const exited = new Promise((resolve) => mongod.once('exit', resolve));
let client;
for (let attempt = 0; ; attempt++) {
	try {
		client = await MongoClient.connect(`mongodb://127.0.0.1:${PORT}`, { serverSelectionTimeoutMS: 1000 });
		break;
	} catch (error) {
		if (attempt == 30) throw error;
	}
}
const record = recorder('mongodb');
try {
	await run(
		{
			db: client.db('bench'),
			size: async () => {
				await client.db('admin').command({ fsync: 1 });
				return du(dir);
			},
			// a database of its own, as mostik's is a file of its own
			durable: async () => client.db('durable').collection('durable', { writeConcern: { w: 1, j: true } }),
		},
		record,
		N,
	);
	record({ group: 'Memory', label: 'Peak memory of the process', bytes: peakMemory(mongod.pid) });
} finally {
	await client.close();
	mongod.kill();
	await exited;
	fs.rmSync(dir, { recursive: true, force: true });
}
