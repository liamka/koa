'use strict';
// One Storage per database file and process: every MostikClient on the same file shares
// its native handle, write queue and index registry.
const fs = require('fs');
const pathModule = require('path');
const { Encoder, addExtension } = require('msgpackr');
const { writeKey, readKey } = require('ordered-binary');
const { NativeEnv } = require('../mostik.node');
const { ObjectId } = require('./object-id');
const { breathe } = require('./pause');

const MAX_KEY_SIZE = 1978;
const DEFAULT_COMPRESSION_THRESHOLD = 1000;
const MAX_BATCH_BYTES = Number(process.env.MOSTIK_BATCH ?? 4 << 20);
const OP_PUT = 1;
const OP_REMOVE = 2;
const OP_REQUIRE_ABSENT = 3;
const OP_END_GROUP = 4;
const OP_REQUIRE_PRESENT = 5;
const OP_REQUIRE_EQUAL = 6;
const OP_ADD = 7;
const OP_REQUIRE_UNIQUE = 8;
// room for one record header plus the largest key ordered-binary may try to write
const KEY_ROOM = 2048 + 64;

// ordered-binary views its target through a DataView rounded up to 4 bytes, so every buffer
// it writes into must have a length that is a multiple of 4
function allocate(size) {
	return Buffer.allocUnsafeSlow(Math.ceil(size / 8) * 8);
}

addExtension({
	Class: ObjectId,
	type: 11,
	pack: (id) => id.id,
	unpack: (bytes) => new ObjectId(bytes),
});

// Shared scratch buffers: reads and scans copy into these instead of allocating per call.
const keyBytes = allocate(KEY_ROOM);
let valueBytes = Buffer.allocUnsafeSlow(0x10000);
let scanBytes = Buffer.allocUnsafeSlow(0x40000);

class KeyTooLargeError extends Error {}

const MISSING_DOC = Symbol('missing document');

/** Where the shared record structures (field names of objects, stored once) are kept. */
const STRUCTURES = ['s'];
/** The catalog: ['c', db, collection] -> the collection's id, which its keys start with. */
const CATALOG = 'c';
/** Record structures shared by all values; more distinct shapes are defined in each value. */
const MAX_SHARED_STRUCTURES = 32;

function u32(n) {
	const bytes = Buffer.allocUnsafe(4);
	bytes.writeUInt32LE(n, 0);
	return bytes;
}

/** Writes the ordered-binary key for `parts` at `target[at]`; returns its size. */
function writeKeyAt(parts, target, at) {
	let end;
	try {
		end = writeKey(parts, target, at);
	} catch (error) {
		if (error.name == 'RangeError') throw new KeyTooLargeError('key too large');
		throw error;
	}
	if (end - at > MAX_KEY_SIZE) throw new KeyTooLargeError('key too large');
	return end - at;
}

/**
 * The encoded key for `parts`, as its own buffer. Passed back as the first element of a key
 * (`[prefix, ...more]`), ordered-binary copies it as is: the result is byte for byte the key of
 * the whole array, so constant prefixes are encoded once instead of on every call.
 */
function encodeKey(parts) {
	return Buffer.from(keyBytes.subarray(0, writeKeyAt(parts, keyBytes, 0)));
}

/** The range holding every key that starts with the elements `parts`. */
function prefixRange(parts) {
	const size = writeKeyAt(parts, keyBytes, 0);
	const start = Buffer.allocUnsafe(size + 1);
	keyBytes.copy(start, 0, 0, size);
	start[size] = 0;
	const end = Buffer.from(start);
	end[size] = 1;
	return [start, end];
}

function compressionThreshold(compression) {
	if (!compression) return 0;
	if (typeof compression == 'object' && compression.threshold > 0) return compression.threshold;
	return DEFAULT_COMPRESSION_THRESHOLD;
}

/** Write records of one event-loop turn, laid out in a single growing buffer. */
class Batch {
	constructor() {
		this.buffer = Buffer.allocUnsafeSlow(0x10000);
		this.length = 0;
		this.groups = 0;
		this.results = null;
	}

	reserve(bytes) {
		if (this.length + bytes <= this.buffer.length) return;
		const grown = allocate(Math.max(this.buffer.length * 2, this.length + bytes));
		this.buffer.copy(grown, 0, 0, this.length);
		this.buffer = grown;
	}

	/** Appends an op with its key; returns the key size. */
	key(op, parts) {
		this.reserve(KEY_ROOM);
		const at = this.length;
		const size = writeKeyAt(parts, this.buffer, at + 5);
		this.buffer[at] = op;
		this.buffer.writeUInt32LE(size, at + 1);
		this.length = at + 5 + size;
		return size;
	}

	value(bytes) {
		this.reserve(4 + bytes.length);
		this.buffer.writeUInt32LE(bytes.length, this.length);
		bytes.copy(this.buffer, this.length + 4);
		this.length += 4 + bytes.length;
	}
}

/**
 * Writes applied atomically, only if all its conditions hold at commit time. Build it
 * synchronously, then `end()` or `commit()`; if building throws, call `abort()` and nothing
 * of it is written.
 */
class Group {
	constructor(storage, batch) {
		this.storage = storage;
		this.batch = batch;
		this.start = batch.length;
		this.ended = false;
	}

	/** Requires the key to be missing; returns the key size. */
	absent(parts) {
		return this.batch.key(OP_REQUIRE_ABSENT, parts);
	}

	present(parts) {
		this.batch.key(OP_REQUIRE_PRESENT, parts);
	}

	/**
	 * Requires that no other key than the one of `parts` (an index entry: prefix, then an _id
	 * of `idSize` bytes) shares its prefix: the entry of a unique index.
	 */
	unique(parts, idSize) {
		const size = this.batch.key(OP_REQUIRE_UNIQUE, parts);
		this.batch.reserve(4);
		this.batch.buffer.writeUInt32LE(size - 1 - idSize, this.batch.length);
		this.batch.length += 4;
	}

	/** Requires the stored value to still be these msgpack bytes. */
	expect(parts, bytes) {
		this.batch.key(OP_REQUIRE_EQUAL, parts);
		this.batch.value(bytes);
	}

	/** Puts msgpack `bytes`; they are copied right away. */
	putBytes(parts, bytes) {
		this.batch.key(OP_PUT, parts);
		this.batch.value(bytes);
	}

	put(parts, value) {
		this.putBytes(parts, this.storage.encoder.encode(value));
	}

	remove(parts) {
		this.batch.key(OP_REMOVE, parts);
	}

	/** Adds `delta` to the counter at `parts`. */
	add(parts, delta) {
		const batch = this.batch;
		batch.key(OP_ADD, parts);
		batch.reserve(8);
		batch.buffer.writeBigInt64LE(BigInt(delta), batch.length);
		batch.length += 8;
	}

	abort() {
		if (!this.ended) this.batch.length = this.start;
	}

	/**
	 * Closes the group; groups sharing a non-zero `chain` stop at the first rejected one.
	 * Returns a ticket for `Storage.applied`.
	 */
	end(chain = 0) {
		const batch = this.batch;
		this.ended = true;
		batch.reserve(5);
		batch.buffer[batch.length] = OP_END_GROUP;
		batch.buffer.writeUInt32LE(chain, batch.length + 1);
		batch.length += 5;
		return { batch, index: batch.groups++ };
	}

	/** Resolves to true when the group was applied, once it is durably committed. */
	commit() {
		return Storage.applied([this.end()]).then(([applied]) => applied);
	}
}

const open = new Map();

class Storage {
	static acquire(path, options) {
		// like lmdb-js: a path with an extension is the data file, otherwise a directory
		const directory = pathModule.extname(path) ? pathModule.dirname(path) : path;
		fs.mkdirSync(directory, { recursive: true });
		// keyed by the real path, so every spelling of it (symlinks, "..") shares one Storage
		const real = fs.realpathSync(directory);
		const dataPath = pathModule.extname(path) ? pathModule.join(real, pathModule.basename(path)) : pathModule.join(real, 'data.mostik');
		const lockPath = pathModule.extname(path) ? dataPath + '-lock' : pathModule.join(real, 'lock.mostik');
		let storage = open.get(dataPath);
		if (!storage) {
			storage = new Storage(dataPath, NativeEnv.open(dataPath, lockPath, options.cacheSize, options.durability == 'strict'), options);
			open.set(dataPath, storage);
		}
		storage.refs++;
		return storage;
	}

	constructor(dataPath, native, options) {
		this.dataPath = dataPath;
		this.native = native;
		this.refs = 0;
		// keys use plain msgpack: their bytes must never depend on the record structures
		this.plain = new Encoder({ copyBuffers: true, useRecords: false });
		const bytes = this.getBytes(STRUCTURES);
		/** field names of objects, stored once instead of in every value (msgpackr records) */
		this.structures = bytes === undefined ? [] : this.plain.decode(bytes);
		this.savedStructures = this.structures.length;
		this.structureTables = null;
		this.encoder = new Encoder({
			copyBuffers: true,
			// maps read back as plain objects, as MongoDB returns them
			mapsAsObjects: true,
			structures: this.structures,
			maxSharedStructures: MAX_SHARED_STRUCTURES,
			// new structures are written with the next batch (`#seal`), in the same commit as
			// the first values that use them
			saveStructures: () => true,
		});
		this.threshold = compressionThreshold(options.compression);
		this.batch = null;
		this.tail = Promise.resolve();
		/** collection id -> { all, ready, building, ... } indexed fields, loaded lazily */
		this.indexes = new Map();
		/** "db\0collection" -> id, for every collection there is */
		this.catalog = new Map();
		/** catalog entries not yet in a committed batch: "db\0collection" -> [db, collection, id] */
		this.unsavedCatalog = new Map();
		let last = 0;
		for (const { key, value } of this.scan(...prefixRange([encodeKey([CATALOG])]), { entries: true })) {
			const [, db, name] = readKey(key, 0, key.length);
			this.catalog.set(`${db}\0${name}`, value);
			last = Math.max(last, parseInt(value, 36));
		}
		this.nextCollection = last + 1;
		this.chains = 0;
		this.lookupMore = false;
	}

	/** The id of collection `name` of database `db` (its keys start with it), or undefined. */
	collectionId(db, name) {
		return this.catalog.get(`${db}\0${name}`);
	}

	/**
	 * Gives collection `name` of `db` an id. Its catalog entry is written with every batch until
	 * one is committed, so no key of the collection is ever on disk without it.
	 */
	createCollection(db, name) {
		const id = (this.nextCollection++).toString(36);
		this.catalog.set(`${db}\0${name}`, id);
		this.unsavedCatalog.set(`${db}\0${name}`, [db, name, id]);
		return id;
	}

	/** Takes collection `name` out of the catalog in memory; returns its catalog key, to remove. */
	uncatalog(db, name) {
		this.catalog.delete(`${db}\0${name}`);
		this.unsavedCatalog.delete(`${db}\0${name}`);
		return encodeKey([CATALOG, db, name]);
	}

	/** Puts into `group` the catalog entry of collection `name` with `id`. */
	catalogAs(group, db, name, id) {
		this.catalog.set(`${db}\0${name}`, id);
		this.unsavedCatalog.delete(`${db}\0${name}`);
		group.putBytes([CATALOG, db, name], this.plain.encode(id));
	}

	/** Names of the collections of `db`, sorted. */
	collectionNames(db) {
		const names = [];
		for (const key of this.catalog.keys()) {
			const at = key.indexOf('\0');
			if (key.slice(0, at) == db) names.push(key.slice(at + 1));
		}
		return names.sort();
	}

	/** msgpack bytes of `value`, valid until the next encode. */
	encode(value) {
		return this.encoder.encode(value);
	}

	/** Plain msgpack bytes of `value` (no record structures), for keys. */
	pack(value) {
		return this.plain.encode(value);
	}

	get #sharedStructures() {
		return this.structures.sharedLength ?? this.structures.length;
	}

	/** The shared record structures for native filters: `[count u32]([fields u32]([len u32][utf-8])*)*`. */
	structureTable() {
		const count = this.#sharedStructures;
		if (this.structureTables?.count !== count) {
			const chunks = [u32(count)];
			for (const names of this.structures.slice(0, count)) {
				chunks.push(u32(names.length));
				for (const name of names) {
					const bytes = Buffer.from(name, 'utf8');
					chunks.push(u32(bytes.length), bytes);
				}
			}
			this.structureTables = { count, bytes: Buffer.concat(chunks) };
		}
		return this.structureTables.bytes;
	}

	decode(bytes, end) {
		return this.encoder.decode(bytes, end);
	}

	/** Committed value at the key `parts`, decoded, or undefined. */
	get(parts) {
		let size;
		try {
			size = writeKeyAt(parts, keyBytes, 0);
		} catch (error) {
			if (error instanceof KeyTooLargeError) return undefined; // no such key can exist
			throw error;
		}
		return this.#read(() => this.native.getInto(keyBytes, size, valueBytes));
	}

	/** Committed value at the key `parts` as raw msgpack bytes (its own buffer), or undefined. */
	getBytes(parts) {
		let size;
		try {
			size = writeKeyAt(parts, keyBytes, 0);
		} catch (error) {
			if (error instanceof KeyTooLargeError) return undefined;
			throw error;
		}
		return this.#read(() => this.native.getInto(keyBytes, size, valueBytes), true);
	}

	/** A counter written with `Group.add`; 0 when it was never written. */
	counter(parts) {
		const bytes = this.getBytes(parts);
		return bytes === undefined ? 0 : Number(bytes.readBigInt64LE(0));
	}

	/**
	 * Index lookup in one native call: the document that the (skip+1)-th index entry under the
	 * prefix `parts` points to (documents live under `docs`). undefined when there is no such
	 * entry, null when it dangles. `this.lookupMore` then tells whether more entries follow.
	 */
	lookup(parts, docs, skip) {
		const size = writeKeyAt(parts, keyBytes, 0);
		let result = this.native.lookupInto(keyBytes, size, docs, skip, valueBytes);
		if (result >= 0 && result >> 1 > valueBytes.length) {
			valueBytes = Buffer.allocUnsafeSlow(Math.max(result >> 1, valueBytes.length * 2));
			return this.lookup(parts, docs, skip);
		}
		if (result == -1) return undefined;
		if (result == -2) {
			this.lookupMore = true; // unknown: let the caller look further
			return null;
		}
		this.lookupMore = (result & 1) == 1;
		return this.decode(valueBytes, result >> 1);
	}

	/**
	 * Number of entries in [start, end), up to `limit` (0 = all); with `docs`, index entries whose
	 * document exists; with `filter` (native filter bytes), only matching documents.
	 */
	countRange(start, end, docs, limit = 0, filter = undefined) {
		return this.native.countRange(start, end, docs, limit, filter);
	}

	/**
	 * Deletes every key of each range `[start, end, counter?]`, then the keys `remove`, in one
	 * commit off the JS thread; a range's counter loses the keys it deleted. Resolves to the
	 * keys deleted per range.
	 */
	deleteRanges(ranges, remove = []) {
		const flat = ranges.flatMap(([start, end, counter]) => [start, end, counter ?? Buffer.alloc(0)]);
		return this.native.deleteRanges(flat, remove);
	}

	/** Resolves to, per ticket from `Group.end`, whether that group was applied. */
	static async applied(tickets) {
		const results = new Map();
		for (const { batch } of tickets) if (!results.has(batch)) results.set(batch, batch.results);
		for (const [batch, pending] of results) results.set(batch, await pending);
		return tickets.map(({ batch, index }) => results.get(batch)[index] == 1);
	}

	/** A fresh chain id for `Group.end`. */
	nextChain() {
		this.chains = (this.chains % 0xffffffff) + 1;
		return this.chains;
	}

	/** True when the next group would start a new batch (the current one is full). */
	batchFull() {
		return this.batch !== null && this.batch.length >= MAX_BATCH_BYTES;
	}

	/** Runs a native read into `valueBytes`, growing it when the value does not fit. */
	#read(readInto, raw = false) {
		let length = readInto();
		if (length > valueBytes.length) {
			valueBytes = Buffer.allocUnsafeSlow(Math.max(length, valueBytes.length * 2));
			length = readInto();
			// the value grew between the two reads
			if (length > valueBytes.length) return this.#read(readInto, raw);
		}
		if (length == -1) return undefined;
		if (length == -2) return MISSING_DOC;
		return raw ? Buffer.from(valueBytes.subarray(0, length)) : this.decode(valueBytes, length);
	}

	/**
	 * Iterates committed values with keys in [start, end), decoded.
	 * `filter`: native filter bytes; documents that cannot match are skipped undecoded.
	 * `docs`: the range holds index entries; yield the documents (under `docs`) they point to.
	 * `entries`: yield `{ key, value, bytes }` (key and raw value bytes copied) instead of values.
	 * `skip`: entries at the start of the range to pass over.
	 * `wanted`: how many values the caller needs at most (0: all); reads start that small.
	 * `reverse`: descending key order.
	 * `project`: native projection bytes; documents come with only those fields.
	 */
	*scan(start, end, { filter, docs, entries = false, skip = 0, wanted = 0, reverse = false, project, pause = false } = {}) {
		yield* this.#chunks(this.native.cursor(start, end, filter, docs, skip, wanted, reverse, project), entries, pause);
	}

	/**
	 * The documents of a native sort (SortedDocs, lib/.../query.rs), in order, as `scan` gives
	 * them; Waits between chunks (lib/pause.js).
	 */
	*readSorted(sorted, entries = false) {
		yield* this.#chunks(sorted, entries, true);
	}

	/** Records read from `source` a chunk at a time (`read(buffer)`, `close()`), decoded. */
	*#chunks(source, entries, pause) {
		try {
			for (;;) {
				const used = source.read(scanBytes);
				if (used == 0) return;
				if (used < 0) {
					scanBytes = Buffer.allocUnsafeSlow(Math.max(-used, scanBytes.length * 2));
					continue;
				}
				// decode the whole chunk before yielding: the next read, by this or another
				// scan, overwrites the shared buffer
				const values = [];
				for (let at = 0; at < used; ) {
					const valueAt = at + 8 + scanBytes.readUInt32LE(at);
					const valueEnd = valueAt + scanBytes.readUInt32LE(valueAt - 4);
					const bytes = scanBytes.subarray(valueAt, valueEnd);
					if (entries) {
						const copy = Buffer.from(bytes);
						values.push({ key: Buffer.from(scanBytes.subarray(at + 4, valueAt - 4)), value: this.decode(copy), bytes: copy });
					} else {
						values.push(this.decode(bytes));
					}
					at = valueEnd;
				}
				yield* values;
				// a turn for the event loop now and then (lib/pause.js): Waits come out too
				if (pause) yield* breathe();
			}
		} finally {
			source.close();
		}
	}

	/** The first key in [start, end) (reverse: the last), or null: one key read, no more. */
	firstKey(start, end, reverse = false) {
		const cursor = this.native.cursor(start, end, undefined, undefined, 0, 1, reverse);
		try {
			for (;;) {
				const used = cursor.read(scanBytes);
				if (used == 0) return null;
				if (used < 0) {
					scanBytes = Buffer.allocUnsafeSlow(Math.max(-used, scanBytes.length * 2));
					continue;
				}
				return Buffer.from(scanBytes.subarray(4, 4 + scanBytes.readUInt32LE(0)));
			}
		} finally {
			cursor.close();
		}
	}

	/** Iterates the committed keys in [start, end) (descending: `reverse`), each its own buffer; values are not read. */
	*keys(start, end, reverse = false) {
		const cursor = this.native.cursor(start, end, undefined, undefined, 0, 0, reverse);
		try {
			for (;;) {
				const used = cursor.read(scanBytes);
				if (used == 0) return;
				if (used < 0) {
					scanBytes = Buffer.allocUnsafeSlow(Math.max(-used, scanBytes.length * 2));
					continue;
				}
				// copied before yielding: the next read overwrites the shared buffer
				const keys = [];
				for (let at = 0; at < used; ) {
					const keyEnd = at + 4 + scanBytes.readUInt32LE(at);
					keys.push(Buffer.from(scanBytes.subarray(at + 4, keyEnd)));
					at = keyEnd + 4 + scanBytes.readUInt32LE(keyEnd);
				}
				yield* keys;
			}
		} finally {
			cursor.close();
		}
	}

	/**
	 * A new write group. Writes of one event turn share one transaction; a batch that grows past
	 * MAX_BATCH_BYTES is committed right away so a burst of writes does not pile up in memory.
	 */
	group() {
		if (this.batch && this.batch.length >= MAX_BATCH_BYTES) this.#seal(this.batch);
		let batch = this.batch;
		if (!batch) {
			batch = this.batch = new Batch();
			batch.results = new Promise((resolve, reject) => {
				batch.settle = { resolve, reject };
			});
			setImmediate(() => this.#seal(batch));
		}
		return new Group(this, batch);
	}

	/** Starts committing the writes queued so far now, instead of at the end of this event turn. */
	commitSoon() {
		if (this.batch) this.#seal(this.batch);
	}

	#seal(batch) {
		if (batch.sealed) return;
		batch.sealed = true;
		if (this.batch === batch) this.batch = null;
		const shared = this.#sharedStructures;
		if (shared > this.savedStructures) {
			// structures new since the last save may be used by this batch's values
			const group = new Group(this, batch);
			group.putBytes(STRUCTURES, this.plain.encode(this.structures.slice(0, shared).map((names) => [...names])));
			group.end();
		}
		// likewise catalog entries of new collections: in the same commit as their first keys
		const cataloged = [...this.unsavedCatalog];
		if (cataloged.length > 0) {
			const group = new Group(this, batch);
			for (const [, [db, name, id]] of cataloged) group.putBytes([CATALOG, db, name], this.plain.encode(id));
			group.end();
		}
		const records = batch.buffer.subarray(0, batch.length);
		const { resolve, reject } = batch.settle;
		this.tail = this.tail
			.then(() => this.native.commit(records, this.threshold))
			.then((results) => {
				this.savedStructures = Math.max(this.savedStructures, shared);
				for (const [key, entry] of cataloged) if (this.unsavedCatalog.get(key) === entry) this.unsavedCatalog.delete(key);
				return results;
			})
			.then(resolve, reject);
	}

	/** Resolves once every write queued so far is committed (later writes are not awaited). */
	async flush() {
		const batch = this.batch;
		if (batch) await batch.results.catch(() => {});
		await this.tail;
	}

	/** Called by each client on close: waits for writes and index builds queued so far. */
	async release() {
		await this.flush();
		const building = [...this.indexes.values()].flatMap((indexes) => [...indexes.building.values()]);
		await Promise.allSettled(building);
		if (--this.refs > 0) return;
		// last client: nobody else can queue writes, so drain whatever is left
		while (this.batch || this.#pendingTail !== this.tail) {
			this.#pendingTail = this.tail;
			await this.flush();
		}
		// reopened while we waited: keep it open for the new client
		if (this.refs > 0) return;
		open.delete(this.dataPath);
		this.native.close();
	}

	#pendingTail = null;
}

module.exports = { Storage, encodeKey, prefixRange, KeyTooLargeError };
