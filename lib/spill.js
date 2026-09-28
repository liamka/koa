'use strict';
// Sorting and grouping in a bounded amount of memory: what does not fit goes to temporary files
// next to the database, in sorted runs, and comes back merged in order.

const fs = require('fs');
const { EMPTY_ARRAY_KEY } = require('./query');
const { Wait, breathe } = require('./pause');

// Memory sorts and groups may hold: each operation, and all of them in the process together.
// Past either, an operation writes what it holds to a file.
const limits = { own: 32 << 20, shared: 64 << 20, native: 32 << 20 };
let held = 0;

/**
 * Sets the limits (tests use small ones to see runs on disk), those of the native layer's sorts
 * and groups too (lib/.../query.rs, 32 MB unless set); returns the previous ones.
 */
function setLimits(next) {
	const previous = { ...limits };
	Object.assign(limits, next);
	const native = limits.native ?? 32 << 20;
	require('../mostik.node').setMemoryLimits(Math.min(native, limits.own), Math.min(native, limits.own));
	return previous;
}

/** Memory an operation holds, counted in the process's total until `release`. */
class Budget {
	#mine = 0;

	take(bytes) {
		this.#mine += bytes;
		held += bytes;
	}

	release() {
		held -= this.#mine;
		this.#mine = 0;
	}

	get full() {
		return this.#mine > limits.own || held > limits.shared;
	}

	get mine() {
		return this.#mine;
	}
}

/** Rough bytes of memory `value` takes as JavaScript values. */
function sizeOf(value, depth = 0) {
	if (value === null || value === undefined) return 8;
	switch (typeof value) {
		case 'number':
		case 'boolean':
			return 8;
		case 'string':
			return 24 + 2 * value.length;
		case 'bigint':
			return 24;
		case 'symbol':
			return 16;
	}
	if (value instanceof Uint8Array) return 96 + value.length;
	if (value instanceof Date) return 48;
	if (depth > 8) return 64;
	let size = 64;
	if (Array.isArray(value)) {
		for (const item of value) size += 8 + sizeOf(item, depth + 1);
		return size;
	}
	for (const key in value) size += 16 + 2 * key.length + sizeOf(value[key], depth + 1);
	return size;
}

// temporary files not removed yet, removed when the process exits whatever happens
const temporary = new Set();
let counter = 0;
process.once('exit', () => {
	for (const file of temporary) {
		try {
			fs.unlinkSync(file);
		} catch {}
	}
});

function removeFile(file) {
	temporary.delete(file);
	try {
		fs.unlinkSync(file);
	} catch {}
}

/**
 * A file of records written in order: each `[u32 length][bytes]` per part. `base`: the path
 * prefix of temporary files (next to the database).
 */
class RunWriter {
	constructor(base) {
		this.file = `${base}-spill-${process.pid}-${++counter}`;
		temporary.add(this.file);
		this.fd = fs.openSync(this.file, 'w');
		this.buffer = Buffer.allocUnsafe(1 << 20);
		this.at = 0;
	}

	/** One record of several parts (Buffers). */
	write(parts) {
		let size = 0;
		for (const part of parts) size += 4 + part.length;
		if (this.at + size > this.buffer.length) this.#flush();
		if (size > this.buffer.length) this.buffer = Buffer.allocUnsafe(size);
		for (const part of parts) {
			this.buffer.writeUInt32LE(part.length, this.at);
			part.copy(this.buffer, this.at + 4);
			this.at += 4 + part.length;
		}
	}

	#flush() {
		if (this.at > 0) fs.writeSync(this.fd, this.buffer, 0, this.at);
		this.at = 0;
	}

	close() {
		this.#flush();
		fs.closeSync(this.fd);
		this.buffer = null;
	}
}

/** Reads back the records of a RunWriter's file, `parts` Buffers each, a chunk at a time. */
class RunReader {
	constructor(file, parts) {
		this.file = file;
		this.parts = parts;
		this.fd = fs.openSync(file, 'r');
		this.buffer = Buffer.allocUnsafe(256 << 10);
		this.start = 0;
		this.end = 0;
		this.eof = false;
		this.done = false;
	}

	/** The next record's parts (copies), or null at the end. */
	next() {
		const record = [];
		let offset = 0;
		for (let i = 0; i < this.parts; i++) {
			if (!this.#have(offset + 4)) return this.#end();
			const length = this.buffer.readUInt32LE(this.start + offset);
			if (!this.#have(offset + 4 + length)) return this.#end();
			const at = this.start + offset + 4;
			record.push(Buffer.from(this.buffer.subarray(at, at + length)));
			offset += 4 + length;
		}
		this.start += offset;
		return record;
	}

	/** Whether the `n` bytes from `start` are in the buffer, reading on (and moving them) if not. */
	#have(n) {
		while (this.start + n > this.end) {
			if (this.eof) return false;
			// what is left of the buffer goes to its start (to a bigger one when it cannot hold n)
			if (n > this.buffer.length) {
				const bigger = Buffer.allocUnsafe(Math.max(n, 2 * this.buffer.length));
				this.buffer.copy(bigger, 0, this.start, this.end);
				this.buffer = bigger;
			} else {
				this.buffer.copyWithin(0, this.start, this.end);
			}
			this.end -= this.start;
			this.start = 0;
			const read = fs.readSync(this.fd, this.buffer, this.end, this.buffer.length - this.end, null);
			if (read == 0) this.eof = true;
			this.end += read;
		}
		return true;
	}

	#end() {
		this.close();
		return null;
	}

	close() {
		if (this.done) return;
		this.done = true;
		fs.closeSync(this.fd);
		removeFile(this.file);
	}
}

/** A binary heap of the heads of sorted runs, smallest first by `order`. */
class Heap {
	constructor(order) {
		this.order = order;
		this.items = [];
	}

	push(item) {
		const items = this.items;
		items.push(item);
		let i = items.length - 1;
		while (i > 0) {
			const parent = (i - 1) >> 1;
			if (this.order(items[i], items[parent]) >= 0) break;
			[items[i], items[parent]] = [items[parent], items[i]];
			i = parent;
		}
	}

	pop() {
		const items = this.items;
		const top = items[0];
		const last = items.pop();
		if (items.length > 0) {
			items[0] = last;
			let i = 0;
			for (;;) {
				const [l, r] = [2 * i + 1, 2 * i + 2];
				let smallest = i;
				if (l < items.length && this.order(items[l], items[smallest]) < 0) smallest = l;
				if (r < items.length && this.order(items[r], items[smallest]) < 0) smallest = r;
				if (smallest == i) break;
				[items[i], items[smallest]] = [items[smallest], items[i]];
				i = smallest;
			}
		}
		return top;
	}

	get size() {
		return this.items.length;
	}
}

/**
 * Sorts `records` ({ keys, payload | value, key? }: sort keys, the document as bytes or as a
 * value, and a database key to carry along) by `compare(keysA, keysB)`, ties in the order they
 * come; yields them in order, the first `wanted` only (0: all). Holds at most the memory limits:
 * runs past them go to temporary files at `base`, keys and values encoded with `codec` (records
 * read back have a payload, the value's encoding). `allowDisk: false`: `tooMuch()` is thrown
 * instead of writing a file.
 */
function* sortRecords(records, { compare, wanted = 0, base, codec, allowDisk = true, tooMuch }) {
	const budget = new Budget();
	const runs = [];
	let kept = [];
	let seq = 0;
	const order = (a, b) => compare(a.keys, b.keys) || a.seq - b.seq;
	const cut = () => {
		kept.sort(order);
		if (wanted > 0 && kept.length > wanted) kept.length = wanted;
		budget.release();
		for (const record of kept) budget.take(record.size);
	};
	try {
		for (const record of records) {
			if (record instanceof Wait) {
				yield record;
				continue;
			}
			if ((seq & 1023) == 1023) yield* breathe();
			record.seq = seq++;
			// the record, its Buffers (a few hundred bytes each besides their bytes) and its keys
			record.size = 320 + (record.payload ? record.payload.length : sizeOf(record.value)) + (record.key?.length ?? 0) + sizeOf(record.keys);
			kept.push(record);
			budget.take(record.size);
			// the first `wanted`: sort and cut now and then, so memory stays near them
			if (wanted > 0 && kept.length >= 2 * wanted + 1024) cut();
			if (budget.full) {
				if (wanted > 0) cut();
				if (!budget.full) continue;
				if (!allowDisk) throw tooMuch();
				kept.sort(order);
				const run = new RunWriter(base);
				const limit = wanted > 0 ? Math.min(wanted, kept.length) : kept.length;
				for (let i = 0; i < limit; i++) {
					const { keys, payload, value, key } = kept[i];
					run.write([codec.encode(keys.map(emptyOut)), payload ?? codec.encode(value), key ?? EMPTY]);
				}
				run.close();
				runs.push(run.file);
				kept = [];
				budget.release();
			}
		}
		kept.sort(order);
		if (runs.length == 0) {
			const limit = wanted > 0 ? Math.min(wanted, kept.length) : kept.length;
			for (let i = 0; i < limit; i++) {
				if ((i & 1023) == 1023) yield* breathe();
				yield kept[i];
			}
			return;
		}
		yield* merge(runs, kept, { compare, wanted, codec });
	} finally {
		budget.release();
		for (const file of runs) removeFile(file);
	}
}

const EMPTY = Buffer.alloc(0);
// the empty array's sort key, a symbol, goes to files as undefined (no key is undefined)
const emptyOut = (key) => (key === EMPTY_ARRAY_KEY ? undefined : key);
const emptyIn = (key) => (key === undefined ? EMPTY_ARRAY_KEY : key);

/** Merges runs on disk and the last run, in memory (`tail`), by keys; ties by run, then place. */
function* merge(files, tail, { compare, wanted, codec }) {
	const readers = files.map((file) => new RunReader(file, 3));
	try {
		const heap = new Heap((a, b) => compare(a.record.keys, b.record.keys) || a.run - b.run || a.place - b.place);
		const advance = (run, place) => {
			let record;
			if (run < readers.length) {
				const parts = readers[run].next();
				if (!parts) return;
				record = { keys: codec.decode(parts[0]).map(emptyIn), payload: parts[1], key: parts[2].length > 0 ? parts[2] : undefined };
			} else {
				if (place >= tail.length) return;
				record = tail[place];
			}
			heap.push({ record, run, place });
		};
		for (let run = 0; run <= readers.length; run++) advance(run, 0);
		let given = 0;
		while (heap.size > 0) {
			if ((given & 1023) == 1023) yield* breathe();
			const { record, run, place } = heap.pop();
			yield record;
			if (wanted > 0 && ++given >= wanted) return;
			advance(run, place + 1);
		}
	} finally {
		for (const reader of readers) reader.close();
	}
}

module.exports = { Budget, sizeOf, sortRecords, setLimits, RunWriter, RunReader, Heap, removeFile, limits };
