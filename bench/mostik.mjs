// The benchmark on mostik: node mostik.mjs  (N=1000000 for a quicker run; DIR: where the data goes)
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { MostikClient } from '../index.mjs';
import { run } from './scenario.mjs';
import { recorder, du, peakMemory } from './common.mjs';

const N = Number(process.env.N ?? 10_000_000);
const dir = process.env.DIR ?? fs.mkdtempSync(path.join(os.tmpdir(), 'mostik-bench-'));
const durableDir = `${dir}-durable`;
fs.rmSync(dir, { recursive: true, force: true });
fs.rmSync(durableDir, { recursive: true, force: true });

const client = await MostikClient.connect(dir);
let durableClient = null;
const record = recorder('mostik');
await run(
	{
		db: client.db('bench'),
		// closing writes everything to the files; the next operation opens them again
		size: async () => {
			await client.close();
			return du(dir);
		},
		durable: async () => {
			durableClient = await MostikClient.connect(durableDir, { durability: 'strict' });
			return durableClient.db('bench').collection('durable');
		},
	},
	record,
	N,
);
await durableClient.close();
await client.close();
record({ group: 'Memory', label: 'Peak memory of the process', bytes: peakMemory(process.pid) });
fs.rmSync(dir, { recursive: true, force: true });
fs.rmSync(durableDir, { recursive: true, force: true });
