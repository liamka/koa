// node examples/compression/index.mjs
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { open } from 'mostik';

const dir = (name) => fileURLToPath(new URL(`./${name}`, import.meta.url));
const article = { title: 'Log', body: 'the same line again\n'.repeat(200_000) };

// values larger than `threshold` bytes are LZ4-compressed; `compression: true` means threshold 1000
const plain = open(dir('plain-db'));
const packed = open(dir('packed-db'), { compression: { threshold: 500 } });
await Promise.all([plain.put('article', article), packed.put('article', article)]);

console.log(packed.get('article').body.length === article.body.length); // true
for (const name of ['plain-db', 'packed-db']) {
	console.log(name, fs.statSync(`${dir(name)}/data.mostik`).size, 'bytes on disk');
}

await Promise.all([plain.close(), packed.close()]);
