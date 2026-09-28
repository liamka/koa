// Helpers shared by the runners: result files, sizes on disk, peak memory.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

export const RESULTS = path.join(import.meta.dirname, 'results');

/** Writes results to `results/<name>.jsonl` and prints them as they come. */
export function recorder(name) {
	fs.mkdirSync(RESULTS, { recursive: true });
	const file = path.join(RESULTS, `${name}.jsonl`);
	fs.writeFileSync(file, '');
	return (result) => {
		fs.appendFileSync(file, JSON.stringify(result) + '\n');
		const shown = result.bytes !== undefined ? `${Math.round(result.bytes / 2 ** 20)} MB` : `${result.ms.toFixed(3)} ms`;
		console.log(`${result.label.padEnd(80)} ${shown.padStart(14)}${result.result === undefined ? '' : `  → ${result.result}`}`);
	};
}

/** Bytes a directory takes on disk. */
export function du(dir) {
	let bytes = 0;
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		const file = path.join(dir, entry.name);
		bytes += entry.isDirectory() ? du(file) : fs.statSync(file).blocks * 512;
	}
	return bytes;
}

/**
 * The most memory process `pid` has held: its peak physical footprint on macOS (what the system
 * counts, without memory freed but not yet taken back), its peak resident size on Linux.
 */
export function peakMemory(pid) {
	if (process.platform == 'darwin') {
		const summary = execFileSync('vmmap', ['-summary', String(pid)], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
		const [, amount, unit] = /Physical footprint \(peak\):\s+([\d.]+)([KMG])/.exec(summary);
		return Number(amount) * { K: 2 ** 10, M: 2 ** 20, G: 2 ** 30 }[unit];
	}
	const [, kb] = /VmHWM:\s+(\d+) kB/.exec(fs.readFileSync(`/proc/${pid}/status`, 'utf8'));
	return Number(kb) * 1024;
}
