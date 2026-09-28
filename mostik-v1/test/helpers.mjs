import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export function tempDir() {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mostik-test-'));
	return dir;
}
