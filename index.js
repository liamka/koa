'use strict';
const { ObjectId } = require('./lib/object-id');
const { readKey } = require('ordered-binary');
const { Storage, encodeKey, prefixRange, KeyTooLargeError } = require('./lib/storage');
const { keyElement, indexElements, matches, equal, isOperators, isEquality, isElementOperators, regexPrefix, filterPaths, checkFilter, nativeFilter, compareValues, sortKey, sortKeys, compareSortKeys, EMPTY_ARRAY_ELEMENT, rank, RANK, nativeProjection } = require('./lib/query');
const { parseUpdate, applyUpdate, upsertSeed } = require('./lib/update');
const { parsePipeline, run: runPipeline, fieldsUsed, sorted: sortStage } = require('./lib/aggregate');
const { sortRecords } = require('./lib/spill');
const { Wait, wait, breathe, step, finish } = require('./lib/pause');

// Key layout, one ordered key space per file:
// (prefixes are encoded once per Collection, see encodeKey)
//   ['d', db, collection, _id]                 -> document
//   ['i', db, collection, field, value, _id]   -> length of the _id suffix, u16 (secondary index
//                                                 entry; the native layer rebuilds the document key)
//   ['x', db, collection, field]               -> { field, name, multikey }   (index registry;
//                                                 multikey: some document has several entries)
//   ['n', db, collection]                      -> document count   (counter)

// Groups queued before their results are awaited: bounds memory on bulk operations.
const SETTLE_EVERY = 20_000;
// Bulk updates hand this many groups at a time to the native layer, which commits them while
// the next ones are prepared.
const PIPELINE_EVERY = 1024;

class MongoServerError extends Error {
	constructor(message, fields) {
		super(message);
		this.name = 'MongoServerError';
		Object.assign(this, fields);
	}
}

class MongoBulkWriteError extends MongoServerError {
	constructor(writeErrors, insertedCount, insertedIds) {
		super(writeErrors[0].errmsg, { code: writeErrors[0].code, writeErrors, insertedCount, insertedIds });
		this.name = 'MongoBulkWriteError';
		this.result = { insertedCount, insertedIds };
	}
}

const serverError = (message, fields) => new MongoServerError(message, fields);

function updateResult(matchedCount, modifiedCount) {
	return { acknowledged: true, matchedCount, modifiedCount, upsertedCount: 0, upsertedId: null };
}

/** Identity of an index key element: equal elements, equal identities. */
function elementId(element) {
	return typeof element == 'symbol' ? 's' + element.description : typeof element + ':' + String(element);
}

/** The fields and directions of an index, from its registry entry. */
function specOf(entry) {
	return { fields: entry.fields ?? [entry.field], directions: entry.directions ?? [entry.direction ?? 1], unique: entry.unique === true };
}

/**
 * The key element tuples a document gets in an index on `fields`: every combination of the
 * elements of each field, where one field at most has several (an array).
 */
function indexTuples(doc, fields, pack) {
	const lists = fields.map((field) => indexElements(doc, field, pack, ambiguousField));
	if (lists.length == 1) return lists[0].map((element) => [element]);
	const arrays = fields.filter((_, i) => lists[i].length > 1);
	if (arrays.length > 1)
		throw new MongoServerError(`cannot index parallel arrays [${arrays[1]}] [${arrays[0]}]`, { code: 171, codeName: 'CannotIndexParallelArrays' });
	let tuples = [[]];
	for (const list of lists) tuples = tuples.flatMap((tuple) => list.map((element) => [...tuple, element]));
	return tuples;
}

function parallelSort() {
	throw new MongoServerError('cannot sort with keys that are parallel arrays', { code: 2, codeName: 'BadValue' });
}

/** A position in an index path that array elements also have as a field name: which is meant? */
function ambiguousField(field, array) {
	throw new MongoServerError(
		`Ambiguous field name found in array (do not use numeric field names in embedded elements in an array), field: '${field}' for array: ${JSON.stringify(array)}`,
		{ code: 16746, codeName: 'Location16746' },
	);
}

/**
 * The first value of reader `iterator` (lib/pause.js), or null, the reader closed: found in
 * this turn when the reader need not wait, else a promise of it.
 */
function first(iterator) {
	const done = (next) => {
		iterator.return();
		return next.done ? null : next.value;
	};
	let next;
	try {
		next = step(iterator);
	} catch (error) {
		iterator.return();
		throw error;
	}
	return next instanceof Promise ? next.then(done, (error) => (iterator.return(), Promise.reject(error))) : done(next);
}

function tupleId(tuple) {
	return tuple.map(elementId).join('\0');
}

/** What a key element stands for (keyElement's inverse), for messages and sort groups. */
function elementValue(element, storage) {
	if (typeof element != 'symbol') return element;
	const description = element.description;
	if (description.startsWith('\u0001o')) return new ObjectId(description.slice(2));
	if (description.startsWith('\u0001m')) return storage.plain.decode(Buffer.from(description.slice(2), 'base64'));
	if (description == EMPTY_ARRAY_ELEMENT) return [];
	return description;
}

function isPlainObjectSpec(value) {
	return value !== null && typeof value == 'object' && !Array.isArray(value) && Object.keys(value).length > 0;
}

function namespaceNotFound(namespace, what) {
	const message = what ? `${what} ${namespace} does not exist` : `ns does not exist: ${namespace}`;
	return new MongoServerError(message, { code: 26, codeName: 'NamespaceNotFound' });
}

// Collection's rename, for Db.renameCollection
const RENAME = Symbol('rename');
// on a document being updated: its _id is its own
const UPDATING = Symbol('updating');

function showId(id) {
	return id instanceof ObjectId ? `ObjectId('${id.toHexString()}')` : JSON.stringify(id);
}

/** Explains which value made a key too large; other errors pass through. */
function tooLarge(error, fields) {
	if (!(error instanceof KeyTooLargeError)) return error;
	const which = fields.size > 0 ? `_id or an indexed field (${[...fields].join(', ')})` : '_id';
	return new Error(`mostik: the value of ${which} is too large to be used as a key`);
}

function duplicateMessage(namespace, id) {
	return `E11000 duplicate key error collection: ${namespace} index: _id_ dup key: { _id: ${showId(id)} }`;
}

function checkDocument(doc) {
	if (doc === null || typeof doc != 'object' || Array.isArray(doc))
		throw new TypeError('The document to insert must be an object');
}

function checkCount(name, value) {
	if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${name} must be a non-negative integer`);
	return value;
}

// value of an index entry: the length of its key's _id suffix; copied into each write
const suffixBytes = Buffer.alloc(2);

/**
 * Documents matching a query, read lazily in chunks. Like the MongoDB driver's FindCursor:
 * `skip`/`limit` before reading, then `next`, `hasNext`, `toArray` or `for await`.
 */
class FindCursor {
	#open;
	#skip = 0;
	#limit = 0;
	#sort = null;
	#allowDisk = true;
	#iterator = null;
	#peeked = null;
	#closed = false;

	constructor(open, { skip = 0, limit = 0, sort, allowDiskUse } = {}) {
		this.#open = open;
		this.skip(skip);
		this.limit(limit);
		if (sort !== undefined) this.sort(sort);
		if (allowDiskUse !== undefined) this.allowDiskUse(allowDiskUse);
	}

	#checkNotStarted() {
		if (this.#iterator || this.#closed) throw new Error('Cursor is already initialized');
	}

	skip(n) {
		this.#checkNotStarted();
		this.#skip = checkCount('skip', n);
		return this;
	}

	/**
	 * Orders the documents: `{ field: 1, other: -1 }`, `[['field', 1], ...]`, `['field', 1]` or
	 * `'field'`; directions 1/-1 or 'asc'/'desc'. Values compare as in MongoDB (types in its
	 * order; an array by its smallest element ascending, its largest descending; a missing field
	 * as null).
	 */
	sort(spec) {
		this.#checkNotStarted();
		this.#sort = sortSpec(spec);
		return this;
	}

	/**
	 * Whether a sort that does not fit in memory may use temporary files (the default, as in
	 * MongoDB 7); false: it fails with error 292 instead.
	 */
	allowDiskUse(allow = true) {
		this.#checkNotStarted();
		if (typeof allow != 'boolean') throw new TypeError('allowDiskUse must be true or false');
		this.#allowDisk = allow;
		return this;
	}

	/** 0 means no limit; a negative limit counts like its absolute value, as in MongoDB. */
	limit(n) {
		this.#checkNotStarted();
		this.#limit = checkCount('limit', Math.abs(n));
		return this;
	}

	*#take() {
		// what the source may do itself: pass over `skip` documents (lowering it), and read no
		// more than `wanted`
		const window = { skip: this.#skip, wanted: this.#limit > 0 ? this.#skip + this.#limit : 0, allowDisk: this.#allowDisk };
		let left = this.#limit || Infinity;
		for (const doc of this.#open(window, this.#sort)) {
			if (doc instanceof Wait) {
				yield doc;
				continue;
			}
			if (window.skip > 0) {
				window.skip--;
				continue;
			}
			yield doc;
			if (--left == 0) return;
		}
	}

	// reading gives the event loop turns, and waits for work off the main thread (lib/pause.js)
	async #pull() {
		if (this.#peeked) {
			const peeked = this.#peeked;
			this.#peeked = null;
			return peeked;
		}
		if (this.#closed) return { done: true };
		this.#iterator ??= this.#take();
		let next = step(this.#iterator);
		if (next instanceof Promise) next = await next;
		if (next.done) this.#closed = true;
		return next;
	}

	async next() {
		const next = await this.#pull();
		return next.done ? null : next.value;
	}

	async hasNext() {
		this.#peeked ??= await this.#pull();
		return !this.#peeked.done;
	}

	async toArray() {
		const docs = [];
		for (let next = await this.#pull(); !next.done; next = await this.#pull()) docs.push(next.value);
		return docs;
	}

	async *[Symbol.asyncIterator]() {
		try {
			for (let next = await this.#pull(); !next.done; next = await this.#pull()) yield next.value;
		} finally {
			await this.close();
		}
	}

	async close() {
		this.#closed = true;
		this.#peeked = null;
		this.#iterator?.return();
	}
}

/**
 * The results of an aggregation, read lazily. Like the MongoDB driver's AggregationCursor:
 * `next`, `hasNext`, `toArray`, `for await`; `sort`, `skip`, `limit` act on the results.
 */
class AggregationCursor extends FindCursor {}

function checkName(kind, name) {
	if (typeof name != 'string' || name.length == 0 || name.includes('\0') || name.includes('$'))
		throw new TypeError(`Invalid ${kind} name: ${String(name)}`);
}

class MostikClient {
	#path;
	#options;
	#storage = null;

	/** `path` is where the database lives: a directory, or a file if it has an extension. */
	constructor(path, options = {}) {
		if (typeof path != 'string' || path.length == 0) throw new TypeError('A database path is required');
		const { cacheSize, durability } = options;
		if (cacheSize !== undefined && !(Number.isSafeInteger(cacheSize) && cacheSize > 0))
			throw new TypeError('cacheSize must be a positive number of bytes');
		if (durability !== undefined && durability !== 'journal' && durability !== 'strict')
			throw new TypeError("durability must be 'journal' or 'strict'");
		this.#path = path;
		this.#options = options;
	}

	static async connect(path, options) {
		return new MostikClient(path, options).connect();
	}

	async connect() {
		this._storage();
		return this;
	}

	// Like the MongoDB driver, operations connect on first use.
	_storage() {
		if (!this.#storage) this.#storage = Storage.acquire(this.#path, this.#options);
		return this.#storage;
	}

	db(name = 'test') {
		checkName('database', name);
		return new Db(this, name);
	}

	/** Waits for pending writes, then releases the database file. */
	async close() {
		const storage = this.#storage;
		this.#storage = null;
		if (storage) await storage.release();
	}
}

class Db {
	#client;

	constructor(client, name) {
		this.#client = client;
		this.databaseName = name;
	}

	collection(name) {
		checkName('collection', name);
		return new Collection(this.#client, this.databaseName, name);
	}

	/**
	 * The collections of this database (those with documents or indexes), by name, as MongoDB
	 * describes them; `filter` applies to the descriptions (e.g. { name: 'users' }).
	 * `nameOnly`: only `{ name, type }`.
	 */
	listCollections(filter = {}, { nameOnly = false } = {}) {
		checkFilter(filter);
		const storage = this.#client._storage();
		const db = this.databaseName;
		return new FindCursor(function* () {
			for (const name of storage.collectionNames(db)) {
				const info = nameOnly
					? { name, type: 'collection' }
					: { name, type: 'collection', options: {}, info: { readOnly: false }, idIndex: { v: 2, key: { _id: 1 }, name: '_id_' } };
				if (matches(info, filter)) yield info;
			}
		});
	}

	/**
	 * Renames collection `from` to `to`, in one catalog change whatever its size; resolves to the
	 * renamed Collection. `dropTarget`: drop `to` first if it exists (else that is an error).
	 */
	async renameCollection(from, to, { dropTarget = false } = {}) {
		checkName('collection', from);
		checkName('collection', to);
		if (from == to) throw new MongoServerError("Can't rename a collection to itself", { code: 20, codeName: 'IllegalOperation' });
		const storage = this.#client._storage();
		if (storage.collectionId(this.databaseName, from) === undefined)
			throw new MongoServerError(`Source collection ${this.databaseName}.${from} does not exist`, { code: 26, codeName: 'NamespaceNotFound' });
		if (storage.collectionId(this.databaseName, to) !== undefined) {
			if (!dropTarget) throw new MongoServerError(`Target namespace exists: ${this.databaseName}.${to}`, { code: 48, codeName: 'NamespaceExists' });
			await this.collection(to).drop();
		}
		await this.collection(from)[RENAME](to);
		return this.collection(to);
	}

	/** Drops every collection of the database. Resolves to true. */
	async dropDatabase() {
		const storage = this.#client._storage();
		for (const name of storage.collectionNames(this.databaseName)) await this.collection(name).drop();
		return true;
	}
}

class Collection {
	#client;
	#db;
	#name;
	// the encoded key prefixes of the collection's id: documents, index entries, index registry;
	// its document counter key
	#prefixes = null;

	constructor(client, db, name) {
		this.#client = client;
		this.#db = db;
		this.#name = name;
	}

	/**
	 * The key prefixes of the collection as the catalog has it now (renames and drops change
	 * it); a collection that does not exist gets those of an id no collection has, holding nothing.
	 */
	#keys() {
		const id = this.#client._storage().collectionId(this.#db, this.#name) ?? '';
		if (this.#prefixes?.id !== id) {
			this.#prefixes = {
				id,
				docs: encodeKey(['d', id]),
				entries: encodeKey(['i', id]),
				registry: encodeKey(['x', id]),
				counter: encodeKey(['n', id]),
			};
		}
		return this.#prefixes;
	}

	get #docs() {
		return this.#keys().docs;
	}

	get #entries() {
		return this.#keys().entries;
	}

	get #registry() {
		return this.#keys().registry;
	}

	get #counter() {
		return this.#keys().counter;
	}

	/** Creates the collection (gives it an id) unless it exists. */
	#ensure(storage) {
		if (storage.collectionId(this.#db, this.#name) === undefined) storage.createCollection(this.#db, this.#name);
	}

	get collectionName() {
		return this.#name;
	}

	get dbName() {
		return this.#db;
	}

	get namespace() {
		return `${this.#db}.${this.#name}`;
	}

	/**
	 * Inserts `doc`, adding an ObjectId `_id` to it when it has none (as the MongoDB driver does).
	 * Resolves once the document and its index entries are durably committed.
	 */
	async insertOne(doc) {
		checkDocument(doc);
		if (doc._id === undefined) doc._id = new ObjectId();
		const storage = this.#client._storage();
		if (this.#indexes(storage).fence) await this.#unfenced(storage);
		const fields = this.#indexes(storage).all;
		const group = storage.group();
		try {
			this.#addDocument(group, storage, doc, fields);
		} catch (error) {
			group.abort();
			throw tooLarge(error, fields);
		}
		if (!(await group.commit())) throw this.#duplicate(storage, doc);
		return { acknowledged: true, insertedId: doc._id };
	}

	/**
	 * Inserts all `docs` (adding missing `_id`s) with one commit per few MB instead of one per
	 * document. `ordered` (default): stops at the first failure; otherwise inserts every document
	 * it can. Failures reject with a MongoBulkWriteError listing them.
	 */
	async insertMany(docs, { ordered = true } = {}) {
		if (!Array.isArray(docs)) throw new TypeError('docs must be an array');
		for (const doc of docs) checkDocument(doc);
		const storage = this.#client._storage();
		if (this.#indexes(storage).fence) await this.#unfenced(storage);
		const fields = this.#indexes(storage).all;
		const chain = ordered ? storage.nextChain() : 0;
		const insertedIds = {};
		const writeErrors = [];
		let insertedCount = 0;
		let failed = false;
		let tickets = [];
		let positions = [];
		const settle = async () => {
			const applied = await Storage.applied(tickets);
			for (let j = 0; j < applied.length; j++) {
				const i = positions[j];
				if (applied[j]) {
					insertedIds[i] = docs[i]._id;
					insertedCount++;
				} else if (!(ordered && failed)) {
					// in an ordered insert only the first rejection is a duplicate; the rest of the chain was skipped
					const { message, keyValue } = this.#duplicate(storage, docs[i]);
					writeErrors.push({ index: i, code: 11000, errmsg: message, keyValue });
					failed = true;
				}
			}
			tickets = [];
			positions = [];
		};
		for (let i = 0; i < docs.length && !(ordered && failed); i++) {
			// an ordered chain only holds within one commit: settle before the batch splits
			if ((ordered && storage.batchFull()) || tickets.length >= SETTLE_EVERY) {
				await settle();
				if (ordered && failed) break;
			}
			const doc = docs[i];
			if (doc._id === undefined) doc._id = new ObjectId();
			const group = storage.group();
			try {
				this.#addDocument(group, storage, doc, fields);
			} catch (error) {
				group.abort();
				writeErrors.push({ index: i, code: error.code ?? 2, errmsg: tooLarge(error, fields).message });
				failed = true;
				continue;
			}
			tickets.push(group.end(chain));
			positions.push(i);
		}
		await settle();
		if (writeErrors.length > 0) {
			writeErrors.sort((a, b) => a.index - b.index);
			throw new MongoBulkWriteError(writeErrors, insertedCount, insertedIds);
		}
		return { acknowledged: true, insertedCount, insertedIds };
	}

	/** Writes the document, its index entries and the count into `group`. */
	#addDocument(group, storage, doc, fields) {
		this.#ensure(storage);
		const encode = (value) => storage.encode(value);
		const pack = (value) => storage.pack(value);
		// keys come from values as they will be read back (msgpack normalizes some types),
		// so inserts, index builds and queries all see the same thing
		const id = storage.decode(encode(doc._id));
		if (Array.isArray(id)) throw new MongoServerError("The '_id' value cannot be of type array", { code: 2 });
		const idElement = keyElement(id, pack);
		const docKey = [this.#docs, idElement];
		// MongoDB stores _id as the first field
		const docBytes = encode(Object.keys(doc)[0] == '_id' ? doc : { _id: doc._id, ...doc });
		const stored = fields.size > 0 ? storage.decode(docBytes) : null;
		const keySize = group.absent(docKey);
		group.putBytes(docKey, docBytes);
		if (fields.size > 0) {
			const idSize = keySize - this.#docs.length - 1;
			suffixBytes.writeUInt16LE(idSize, 0);
			const { registry } = this.#indexes(storage);
			for (const index of fields) {
				const { fields: paths, unique } = specOf(registry.get(index));
				const tuples = indexTuples(stored, paths, pack);
				for (const tuple of tuples) {
					const entry = [this.#entries, index, ...tuple, idElement];
					if (unique) group.unique(entry, idSize);
					group.putBytes(entry, suffixBytes);
				}
				if (tuples.length > 1) this.#markMultikey(group, storage, index);
			}
		}
		group.add([this.#counter], 1);
	}

	/** Deletes a document read by a scan (`{ key, value, bytes }`), if it is still unchanged. */
	#removeDocument(group, storage, { key, value, bytes }) {
		const pack = (v) => storage.pack(v);
		group.expect([key], bytes);
		group.remove([key]);
		const idElement = keyElement(value._id, pack);
		const { all, registry } = this.#indexes(storage);
		for (const index of all) {
			for (const tuple of indexTuples(value, specOf(registry.get(index)).fields, pack)) {
				group.remove([this.#entries, index, ...tuple, idElement]);
			}
		}
		group.add([this.#counter], -1);
	}

	/**
	 * The first document equal to `filter` on every given field (dot paths allowed), or null.
	 * Uses `_id` directly, else indexes on the filter's fields, else scans the collection.
	 */
	async findOne(filter = {}) {
		checkFilter(filter);
		const storage = this.#client._storage();
		const pack = (value) => storage.pack(value);
		const accept = (doc) => doc !== undefined && matches(doc, filter);

		if ('_id' in filter && !Array.isArray(filter._id) && isEquality(filter._id)) {
			const doc = storage.get([this.#docs, keyElement(filter._id, pack)]);
			return accept(doc) ? doc : null;
		}
		const { ready } = this.#indexes(storage);
		const indexed = Object.keys(filter).filter((path) => ready.has(path) && !Array.isArray(filter[path]) && isEquality(filter[path]));
		if (indexed.length > 0) {
			let prefixes;
			try {
				prefixes = indexed.map((path) => [this.#entries, path, keyElement(filter[path], pack)]);
				// Usually an early candidate matches. Take candidates from every usable index in
				// turn, each fetched together with its document in one native call; an index with
				// no (more) entries for its value proves there is no match at all.
				for (let skip = 0; skip < 8; skip++) {
					for (const prefix of prefixes) {
						const doc = storage.lookup(prefix, this.#docs, skip);
						if (doc === undefined) return null;
						if (doc !== null && accept(doc)) return doc;
					}
				}
			} catch (error) {
				if (error instanceof KeyTooLargeError) return null; // no value that large was ever indexed
				throw error;
			}
		}
		return first(this.#matching(storage, filter, false, 1));
	}

	/** A cursor over every document matching `filter`, read lazily in chunks. */
	find(filter = {}, options = {}) {
		checkFilter(filter);
		return new FindCursor(
			(window, sort) =>
				sort ? this.#sorted(this.#client._storage(), filter, sort, window) : this.#matching(this.#client._storage(), filter, false, window.wanted),
			options,
		);
	}

	/** Number of documents matching `filter`, after `skip`, at most `limit`. */
	async countDocuments(filter = {}, { skip = 0, limit = 0 } = {}) {
		checkFilter(filter);
		checkCount('skip', skip);
		checkCount('limit', limit);
		return finish(this.#count(this.#client._storage(), filter, skip, limit));
	}

	/** countDocuments, as a generator (lib/pause.js) returning the count. */
	*#count(storage, filter, skip = 0, limit = 0) {
		const clamp = (n) => {
			n = Math.max(0, n - skip);
			return limit > 0 ? Math.min(n, limit) : n;
		};
		const paths = Object.keys(filter);
		// the whole collection: its counter, one read
		if (paths.length == 0) return clamp(storage.counter([this.#counter]));
		const plan = this.#plan(storage, filter);
		if (plan === EMPTY) return clamp(0);
		const wanted = limit > 0 ? skip + limit : 0;
		// every document once: sums of native counts over the ranges are right
		const countRanges = (docs, bytes) => {
			// many ranges of index keys: counted in one call
			if (!docs && !bytes && wanted == 0 && plan.ranges.length > 8) return clamp(storage.native.countRanges(plan.ranges.flat()));
			let n = 0;
			for (const [start, end] of plan.ranges) {
				n += storage.countRange(start, end, docs, wanted > 0 ? wanted - n : 0, bytes);
				if (wanted > 0 && n >= wanted) break;
			}
			return clamp(n);
		};
		// indexed fields whose ranges hold exactly the matches: count index entries,
		// documents are never read (index entries come and go with them). A few are counted
		// right away; many, off the JS thread.
		if (plan.index && plan.exact && !plan.dedupe && paths.every((path) => plan.covers?.includes(path))) {
			let n = 0;
			for (const [start, end] of plan.ranges) {
				n += storage.countRange(start, end, undefined, SYNC_COUNT - n);
				if (n >= SYNC_COUNT) break;
			}
			if (n < SYNC_COUNT) return clamp(n);
			if (wanted > 0 && n >= wanted) return clamp(n);
			return clamp(yield* wait(storage.native.countKeysTask(plan.ranges.flat())));
		}
		// $or of alternatives that each index answers exactly: the distinct documents their keys
		// find, counted without reading documents
		if (paths.length == 1 && paths[0] == '$or' && plan.union?.every((part) => exactAlone(part))) {
			const [indexRanges, docRanges, docKeys] = [[], [], []];
			for (const part of plan.union) {
				if (part.keys) docKeys.push(...part.keys);
				else (part.index ? indexRanges : docRanges).push(...part.ranges.flat());
			}
			return clamp(storage.native.countDistinct(this.#docs, indexRanges, docRanges, docKeys));
		}
		// the native filter decides alone: count without decoding a single document
		const compiled = nativeFilter(filter, (v) => storage.encode(v), storage.structureTable());
		if (compiled.exact && plan.ranges && !plan.dedupe) {
			// documents read and filtered off the JS thread
			if (compiled.bytes) return clamp(yield* wait(storage.native.countTask(plan.ranges.flat(), plan.index ? this.#docs : undefined, compiled.bytes, wanted)));
			return countRanges(plan.index ? this.#docs : undefined, compiled.bytes);
		}
		let n = 0;
		for (const doc of this.#matching(storage, filter, false, wanted)) {
			if (doc instanceof Wait) yield doc;
			else if (++n == wanted) break;
		}
		return clamp(n);
	}

	/** Deletes the first document matching `filter`. */
	async deleteOne(filter = {}) {
		checkFilter(filter);
		const storage = this.#client._storage();
		if (this.#indexes(storage).fence) await this.#unfenced(storage);
		// a document changed or deleted between reading and committing is skipped: look again
		for (;;) {
			let entry = first(this.#matching(storage, filter, true, 1));
			if (entry instanceof Promise) entry = await entry;
			if (!entry) return { acknowledged: true, deletedCount: 0 };
			const group = storage.group();
			try {
				this.#removeDocument(group, storage, entry);
			} catch (error) {
				group.abort();
				throw error;
			}
			if (await group.commit()) return { acknowledged: true, deletedCount: 1 };
		}
	}

	/**
	 * Deletes every document matching `filter`, streaming: documents are read in chunks and
	 * deleted in bulk commits. `{}` empties the collection entirely inside the native layer,
	 * without reading documents.
	 */
	async deleteMany(filter = {}) {
		checkFilter(filter);
		const storage = this.#client._storage();
		if (this.#indexes(storage).fence) await this.#unfenced(storage);
		if (Object.keys(filter).length == 0) {
			// documents and index entries go in one commit, whole subtrees at once; writes wait
			// meanwhile, so no entry of a new document goes with them
			return this.#exclusive(storage, async () => {
				await storage.flush();
				const [deletedCount] = await storage.deleteRanges([
					[...prefixRange([this.#docs]), this.#counter],
					prefixRange([this.#entries]),
				]);
				return { acknowledged: true, deletedCount };
			});
		}
		let deletedCount = 0;
		let tickets = [];
		const settle = async () => {
			for (const applied of await Storage.applied(tickets)) deletedCount += applied;
			tickets = [];
		};
		const matching = this.#matching(storage, filter, true);
		for (;;) {
			let next = step(matching);
			if (next instanceof Promise) next = await next;
			if (next.done) break;
			const entry = next.value;
			const group = storage.group();
			try {
				this.#removeDocument(group, storage, entry);
			} catch (error) {
				group.abort();
				throw error;
			}
			tickets.push(group.end());
			if (tickets.length >= SETTLE_EVERY) await settle();
		}
		await settle();
		return { acknowledged: true, deletedCount };
	}

	/**
	 * Runs an aggregation pipeline ($match, $group, $count, $sort, $skip, $limit, $project,
	 * $unwind). A leading $match finds its documents as find does (indexes, filtering on
	 * document bytes), a $sort right after it sorts as find does (by an index when one gives
	 * the order); $match then $count counts as countDocuments does, without reading documents.
	 */
	aggregate(pipeline = [], options = {}) {
		const stages = parsePipeline(pipeline, serverError);
		if (options.allowDiskUse !== undefined && typeof options.allowDiskUse != 'boolean') throw new TypeError('allowDiskUse must be true or false');
		const storage = () => this.#client._storage();
		return new AggregationCursor((window, sort) => {
			const io = this.#pipelineIo(storage(), options.allowDiskUse !== false && window.allowDisk !== false);
			const results = this.#aggregate(storage(), stages, io);
			// a sort of the results is one more $sort stage
			return sort ? sortStage(results, { sort, wanted: window.wanted }, io) : results;
		}, options);
	}

	/**
	 * What pipeline stages need from the database: key elements (`pack`), a codec and a place for
	 * the temporary files of sorts and groups that do not fit in memory, whether they may be
	 * written (`allowDisk`), and MongoDB's errors (`make`).
	 */
	#pipelineIo(storage, allowDisk) {
		return { pack: (value) => storage.pack(value), codec: storage.plain, base: storage.dataPath, allowDisk, make: serverError };
	}

	*#aggregate(storage, stages, io) {
		const filter = stages[0]?.op == '$match' ? stages[0].arg : {};
		const rest = stages[0]?.op == '$match' ? stages.slice(1) : stages;
		if (rest.length == 1 && rest[0].op == '$count') {
			const n = yield* this.#count(storage, filter);
			if (n > 0) yield { [rest[0].arg]: n };
			return;
		}
		if (rest[0]?.op == '$skip' && Object.keys(filter).length == 0) {
			// every document matches: the skipped ones are passed over without being read
			const fields = fieldsUsed(stages);
			const project = fields ? nativeProjection(fields, storage.structureTable()) : undefined;
			yield* runPipeline(storage.scan(...prefixRange([this.#docs]), { skip: rest[0].arg, project, pause: true }), rest.slice(1), io);
			return;
		}
		if (rest[0]?.op == '$group') {
			// grouped on document bytes off the JS thread when the native layer can alone
			const groups = yield* this.#groupNatively(storage, filter, rest[0].arg);
			if (groups) {
				yield* runPipeline(groups, rest.slice(1), io);
				return;
			}
		}
		if (rest[0]?.op == '$sort') {
			// sorted as find sorts, through temporary files when it does not fit in memory
			const { sort, wanted } = rest[0].arg;
			yield* runPipeline(this.#sorted(storage, filter, sort, { skip: 0, wanted, allowDisk: io.allowDisk }), rest.slice(1), io);
			return;
		}
		// the stages read a few fields: the native layer cuts documents down to those
		const fields = fieldsUsed(stages);
		const project = fields ? nativeProjection(fields, storage.structureTable()) : undefined;
		yield* runPipeline(this.#matching(storage, filter, false, 0, project), rest, io);
	}

	/**
	 * Applies `update` ($set, $unset, $inc) to the first document matching `filter`. `upsert`:
	 * when none matches, inserts the filter's equality fields with the update applied.
	 */
	async updateOne(filter = {}, update, { upsert = false } = {}) {
		checkFilter(filter);
		const changes = parseUpdate(update, serverError);
		const storage = this.#client._storage();
		if (this.#indexes(storage).fence) await this.#unfenced(storage);
		// a document changed or deleted between reading and committing is read again
		for (;;) {
			let entry = first(this.#matching(storage, filter, true, 1));
			if (entry instanceof Promise) entry = await entry;
			if (!entry) return upsert ? this.#upsert(storage, filter, changes) : updateResult(0, 0);
			const fields = this.#affectedIndexes(storage, changes);
			const doc = this.#updated(entry, changes, fields.length > 0);
			if (!doc) return updateResult(1, 0);
			const group = storage.group();
			try {
				this.#rewriteDocument(group, storage, entry, doc, fields);
			} catch (error) {
				group.abort();
				throw tooLarge(error, this.#indexes(storage).all);
			}
			if (await group.commit()) return updateResult(1, 1);
			// refused though the document is as read: a unique index holds a new value already
			const refused = this.#uniqueRefusal(storage, entry, doc);
			if (refused) throw refused;
		}
	}

	/** The E11000 error when a rewrite of `entry` into `doc` was refused for a unique index, else null (it changed meanwhile). */
	#uniqueRefusal(storage, entry, doc) {
		const now = storage.getBytes([entry.key]);
		if (now === undefined || !now.equals(entry.bytes)) return null;
		return this.#duplicate(storage, Object.assign(doc, { [UPDATING]: true }));
	}

	/**
	 * Applies `update` to every document matching `filter`, streaming in bulk commits like
	 * deleteMany. `upsert`: when none matches, inserts one document as updateOne does.
	 */
	async updateMany(filter = {}, update, { upsert = false } = {}) {
		checkFilter(filter);
		const changes = parseUpdate(update, serverError);
		const storage = this.#client._storage();
		if (this.#indexes(storage).fence) await this.#unfenced(storage);
		const fields = this.#affectedIndexes(storage, changes);
		let matchedCount = 0;
		let modifiedCount = 0;
		let tickets = [];
		let rewrites = [];
		let refused = null;
		const settle = async () => {
			const [pending, written] = [tickets, rewrites];
			[tickets, rewrites] = [[], []];
			const applied = await Storage.applied(pending);
			for (let i = 0; i < applied.length; i++) {
				if (applied[i]) modifiedCount++;
				else refused ??= this.#uniqueRefusal(storage, written[i].entry, written[i].doc);
			}
		};
		try {
			const matching = this.#matching(storage, filter, true);
			for (;;) {
				let next = step(matching);
				if (next instanceof Promise) next = await next;
				if (next.done) break;
				const entry = next.value;
				matchedCount++;
				const doc = this.#updated(entry, changes, fields.length > 0);
				if (!doc) continue;
				const group = storage.group();
				try {
					this.#rewriteDocument(group, storage, entry, doc, fields);
				} catch (error) {
					group.abort();
					throw tooLarge(error, this.#indexes(storage).all);
				}
				tickets.push(group.end());
				rewrites.push({ entry, doc });
				// the native layer commits this part while the next one is prepared
				if (tickets.length % PIPELINE_EVERY == 0) {
					storage.commitSoon();
					// let finished commits report back, so the next one starts
					await new Promise((resolve) => setImmediate(resolve));
				}
				if (tickets.length >= SETTLE_EVERY) await settle();
				if (refused) break;
			}
		} finally {
			// like MongoDB, documents updated before an error stay updated
			await settle();
		}
		if (refused) throw refused;
		if (matchedCount == 0 && upsert) return this.#upsert(storage, filter, changes);
		return updateResult(matchedCount, modifiedCount);
	}

	/** Indexed fields whose entries `changes` may alter: a change's path is on the field's path. */
	#affectedIndexes(storage, changes) {
		const { all, registry } = this.#indexes(storage);
		const touches = (field) =>
			changes.some(({ path, to }) => [path, to].some((p) => p !== undefined && (p == field || p.startsWith(field + '.') || field.startsWith(p + '.'))));
		return [...all].filter((index) => specOf(registry.get(index)).fields.some(touches));
	}

	/**
	 * The updated scanned document, or null when the update changes nothing. `keep`: update a
	 * copy, the scanned value is still needed (its index entries go away).
	 */
	#updated({ value, bytes }, changes, keep) {
		const doc = keep ? this.#client._storage().decode(bytes) : value;
		const id = doc._id;
		if (!applyUpdate(doc, changes, equal, serverError)) return null;
		if (!Object.hasOwn(doc, '_id') || !equal(doc._id, id) || typeof doc._id != typeof id)
			throw serverError("Performing an update on the path '_id' would modify the immutable field '_id'", { code: 66 });
		return doc;
	}

	/**
	 * Replaces a scanned document (`{ key, value, bytes }`), if it is still unchanged, and its
	 * entries in the indexes on `fields`.
	 */
	#rewriteDocument(group, storage, { key, value, bytes }, doc, fields) {
		group.expect([key], bytes);
		group.putBytes([key], storage.encode(doc));
		if (fields.length == 0) return;
		const pack = (v) => storage.pack(v);
		const idElement = keyElement(value._id, pack);
		const idSize = key.length - this.#docs.length - 1;
		suffixBytes.writeUInt16LE(idSize, 0);
		const { registry } = this.#indexes(storage);
		for (const index of fields) {
			const { fields: paths, unique } = specOf(registry.get(index));
			const before = new Map(indexTuples(value, paths, pack).map((tuple) => [tupleId(tuple), tuple]));
			const after = indexTuples(doc, paths, pack);
			for (const tuple of after) {
				if (before.delete(tupleId(tuple))) continue;
				const entry = [this.#entries, index, ...tuple, idElement];
				if (unique) group.unique(entry, idSize);
				group.putBytes(entry, suffixBytes);
			}
			if (after.length > 1) this.#markMultikey(group, storage, index);
			for (const tuple of before.values()) group.remove([this.#entries, index, ...tuple, idElement]);
		}
	}

	/** Inserts the document an upsert creates. */
	async #upsert(storage, filter, changes) {
		// a copy: the update must not change objects of the caller's filter
		let doc = storage.decode(storage.encode(upsertSeed(filter)));
		applyUpdate(doc, changes, equal, serverError, true);
		if (Object.hasOwn(filter, '_id') && isEquality(filter._id) && !equal(doc._id, filter._id))
			throw serverError("Performing an update on the path '_id' would modify the immutable field '_id'", { code: 66 });
		if (doc._id === undefined) doc = { _id: new ObjectId(), ...doc };
		const fields = this.#indexes(storage).all;
		const group = storage.group();
		try {
			this.#addDocument(group, storage, doc, fields);
		} catch (error) {
			group.abort();
			throw tooLarge(error, fields);
		}
		if (!(await group.commit())) {
			// the filter did not match a document with this _id, or a unique index refused it
			throw this.#duplicate(storage, doc);
		}
		return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 1, upsertedId: doc._id };
	}

	/**
	 * Documents matching `filter`, lazily (`entries`: as `{ key, value, bytes }`); `wanted`: how
	 * many the caller needs at most (0: all), so reads start small. The plan:
	 * `_id` → one read; indexed fields → walk the most selective index, the native layer
	 * fetching each document in the same pass; otherwise scan the collection. Either way the
	 * native layer evaluates the filter on document bytes and only matches get decoded.
	 */
	*#matching(storage, filter, entries, wanted = 0, project = undefined) {
		const plan = this.#plan(storage, filter);
		if (plan === EMPTY) return;
		if (plan.union) {
			if (this.#disjoint(storage, plan.union)) {
				// no document can be in two of them: no need to keep track
				for (const part of plan.union) yield* this.#run(storage, filter, part, entries, 0, project);
				return;
			}
			// each alternative's plan in turn; a document several of them find comes out once
			const seen = new Set();
			for (const part of plan.union) {
				for (const item of this.#run(storage, filter, part, true, 0, project)) {
					if (item instanceof Wait) {
						yield item;
						continue;
					}
					const id = item.key.toString('latin1');
					if (seen.has(id)) continue;
					seen.add(id);
					yield entries ? item : item.value;
				}
			}
			return;
		}
		yield* this.#run(storage, filter, plan, entries, wanted, project);
	}

	/**
	 * Whether the parts of a union find different documents for sure: keys of one field where a
	 * document has one value (not multikey; _id), in ranges that do not overlap.
	 */
	#disjoint(storage, parts) {
		const [{ path }] = parts;
		if (parts.some((part) => part.union || part.path !== path) || this.#indexes(storage).multikey.has(path)) return false;
		const ranges = parts
			.flatMap((part) => (part.keys ? part.keys.map((key) => [key, Buffer.concat([key, Buffer.from([0])])]) : part.ranges))
			.sort((a, b) => Buffer.compare(a[0], b[0]));
		for (let i = 1; i < ranges.length; i++) if (Buffer.compare(ranges[i][0], ranges[i - 1][1]) < 0) return false;
		return true;
	}

	/** The documents matching `filter` that `plan` finds (a union: as entries, maybe more than once). */
	*#run(storage, filter, plan, entries, wanted, project) {
		if (plan.union) {
			for (const part of plan.union) yield* this.#run(storage, filter, part, true, 0, project);
			return;
		}
		if (plan.keys) {
			for (const key of plan.keys) {
				const bytes = storage.getBytes([key]);
				if (bytes === undefined) continue;
				const value = storage.decode(bytes);
				if (matches(value, filter)) yield entries ? { key, value, bytes } : value;
			}
			return;
		}
		const nativeBytes = nativeFilter(filter, (value) => storage.encode(value), storage.structureTable()).bytes;
		// a document found twice (several of its index entries in range) comes out once
		const seen = plan.dedupe ? new Set() : null;
		const options = { filter: nativeBytes, entries: entries || seen !== null, skip: 0, docs: plan.index ? this.#docs : undefined, wanted, project, pause: true };
		if (plan.prefix && !options.entries) {
			// most index values match few documents: fetch the first ones with one native call
			// each, and only open a cursor when there are more
			for (; options.skip < FIRST_LOOKUPS; options.skip++) {
				const doc = storage.lookup(plan.prefix, this.#docs, options.skip);
				if (doc === undefined) return;
				const more = storage.lookupMore;
				if (doc !== null && matches(doc, filter)) yield doc;
				if (!more) return;
			}
		}
		for (const [start, end] of plan.ranges) {
			for (const item of storage.scan(start, end, options)) {
				if (item instanceof Wait) {
					yield item;
					continue;
				}
				if (seen) {
					const id = item.key.toString('latin1');
					if (seen.has(id)) continue;
					seen.add(id);
				}
				const value = options.entries ? item.value : item;
				if (matches(value, filter)) yield entries ? item : value;
			}
			options.skip = 0;
		}
	}

	/**
	 * The documents matching `filter` in the order of `sort` (`[[path, 1 | -1], ...]`). By one
	 * field with an index (or _id): walks its keys in order, unless the filter leaves few
	 * candidates, or matches rarely without a limit; otherwise sorts the matches in memory.
	 */
	*#sorted(storage, filter, sort, window) {
		const { wanted } = window;
		// sorts that do not fit in memory go to temporary files unless told not to
		const allowDisk = window.allowDisk !== false;
		const [[path, direction]] = sort;
		const indexes = this.#indexes(storage);
		// an index on several fields: equalities of the filter on its first fields, then the
		// sort's fields in its order
		const compound = this.#compoundFor(storage, filter, sort);
		if (compound && (compound.equalities > 0 || Object.keys(filter).length == 0 || wanted > 0)) {
			yield* this.#levels(storage, filter, compound.base, sort, 0, window);
			return;
		}
		let plan = null;
		if (sort.length == 1 && (path == '_id' || indexes.ready.has(path))) {
			plan = this.#plan(storage, filter);
			if (plan === EMPTY) return;
			// the filter's ranges on the sort field, walked in order: unless a document may have
			// several values there (it sorts by its smallest or largest, maybe out of range)
			const onField = plan.path == path && plan.ranges && !indexes.multikey.has(path);
			const scan = plan.scan === true;
			let walk = onField || (scan && (Object.keys(filter).length == 0 || wanted > 0));
			if (!walk && plan.ranges && !plan.keys && wanted > 0) {
				// candidates from another index and a limit: the sort order's own index finds the
				// first ones sooner when it holds them densely enough. Walking it passes about
				// wanted × documents / candidates entries; sorting reads the candidates.
				let candidates = 0;
				for (const [start, end] of plan.ranges) candidates += storage.countRange(start, end, undefined, WALK_OR_SORT);
				const documents = storage.counter([this.#counter]);
				walk = candidates >= SORT_IN_MEMORY && (wanted * documents) / candidates < Math.min(candidates, WALK_OR_SORT);
			}
			if (walk) {
				yield* this.#inOrder(storage, filter, path, direction < 0, onField ? plan : null, window);
				return;
			}
		}
		plan ??= this.#plan(storage, filter);
		if (plan === EMPTY) return;
		// the documents, filtered and sorted on their bytes off the JS thread
		const native = this.#natively(storage, filter, plan);
		const segments = sort.map(([path]) => path.split('.'));
		const descending = sort.map(([, direction]) => direction < 0);
		if (native && (wanted == 0 || wanted > TOP_BY_KEYS)) {
			// all of them, or many: sorted through temporary files past its memory
			let sorted;
			try {
				sorted = yield* wait(storage.native.sortDocs(native.ranges, native.docs, native.conditions, segments, descending, wanted, storage.dataPath, allowDisk));
			} catch (error) {
				if (/sort memory/.test(error.message)) throw sortMemoryError();
				throw error;
			}
			if (sorted) {
				yield* storage.readSorted(sorted);
				return;
			}
		}
		if (wanted > 0 && wanted <= TOP_BY_KEYS) {
			if (native) {
				// the first ones picked there
				const found = yield* wait(storage.native.topKeys(native.ranges, native.docs, native.conditions, segments, descending, wanted, TOP_BY_KEYS));
				if (found) {
					yield* this.#placed(storage, filter, sort, wanted, allowDisk, found, window);
					return;
				}
			}
			// the first few: sorted on the fields the sort and the filter read, then read whole
			const fields = new Set([...sort.map(([path]) => path), ...filterPaths(filter)].map((path) => path.split('.')[0]));
			const project = nativeProjection([...fields], storage.structureTable());
			for (const entry of sortInMemory(storage, this.#matching(storage, filter, true, 0, project), sort, wanted, allowDisk, true)) {
				if (entry instanceof Wait) {
					yield entry;
					continue;
				}
				const bytes = storage.getBytes([entry.key]);
				if (bytes === undefined) continue;
				const doc = storage.decode(bytes);
				// changed since: still a match?
				if (matches(doc, filter)) yield doc;
			}
			return;
		}
		yield* sortInMemory(storage, this.#matching(storage, filter, true, 0), sort, wanted, allowDisk);
	}

	/**
	 * $group `spec` of the documents matching `filter`, done by the native layer
	 * (lib/.../query.rs) when it can: _id a field path or null, accumulators $sum (a field path
	 * or a number), $avg, $min, $max, $first, $last (field paths) and $count, the filter one it
	 * answers exactly. Returns the groups (an iterator), or null for the JS side to group.
	 */
	*#groupNatively(storage, filter, spec) {
		const path = (expr) => (typeof expr == 'string' && expr.startsWith('$') && !expr.startsWith('$$') && expr.length > 1 ? expr.slice(1).split('.') : null);
		const id = spec._id === null ? null : path(spec._id);
		if (spec._id !== null && !id) return null;
		const OPS = { $sum: 1, $avg: 2, $min: 3, $max: 4, $count: 5, $first: 6, $last: 7 };
		const [fields, ops, paths, constants] = [[], [], [], []];
		for (const field in spec) {
			if (field == '_id') continue;
			const [op] = Object.keys(spec[field]);
			const arg = spec[field][op];
			const at = op == '$count' ? [] : path(arg);
			if (op == '$sum' && !at && typeof arg == 'number' && Number.isFinite(arg)) {
				paths.push([]);
				constants.push(arg);
			} else if (!(op in OPS) || (op != '$count' && !at)) {
				return null;
			} else {
				paths.push(at);
				constants.push(0);
			}
			fields.push(field);
			ops.push(OPS[op]);
		}
		const plan = this.#plan(storage, filter);
		if (plan === EMPTY) return [].values();
		const native = this.#natively(storage, filter, plan);
		if (!native) return null;
		const groups = yield* wait(storage.native.groupDocs(native.ranges, native.docs, native.conditions, id, ops, paths, constants));
		if (!groups) return null;
		return (function* () {
			for (const bytes of groups) {
				const [value, ...results] = storage.plain.decode(bytes);
				const out = { _id: value };
				fields.forEach((field, i) => (out[field] = results[i]));
				yield out;
			}
		})();
	}

	/**
	 * How the native layer reads the documents `plan` finds and tells those `filter` matches,
	 * when it can alone: `{ ranges, docs, conditions }` for its tasks (lib/.../query.rs): the
	 * collection's documents, or an index's entries each document has one of at most, and a
	 * filter it answers exactly. null otherwise.
	 */
	#natively(storage, filter, plan) {
		if (!plan.ranges || !(plan.scan || (plan.index && !plan.dedupe))) return null;
		const compiled = nativeFilter(filter, (value) => storage.encode(value), storage.structureTable());
		if (!compiled.exact) return null;
		// no condition: the shared structures alone
		const conditions = compiled.bytes ?? Buffer.concat([Buffer.alloc(4), storage.structureTable()]);
		return { ranges: plan.ranges.flat(), docs: plan.index ? this.#docs : undefined, conditions };
	}

	/**
	 * The documents of `found` (topKeys: the first ones' keys in order, and those it could not
	 * place) in the order of `sort`, the first `wanted` of them. Those `window.skip` passes over
	 * are not read when their places are known.
	 */
	*#placed(storage, filter, sort, wanted, allowDisk, { top, unsure }, window) {
		const read = (key) => {
			const bytes = storage.getBytes([key]);
			if (bytes === undefined) return null;
			const value = storage.decode(bytes);
			// changed since: still a match?
			return matches(value, filter) ? { key, value, bytes } : null;
		};
		if (unsure.length == 0) {
			const skip = Math.min(window.skip, top.length);
			window.skip -= skip;
			for (const key of top.slice(skip)) {
				const entry = read(key);
				if (entry) yield entry.value;
			}
			return;
		}
		// placed here with the others, in key order so that ties keep it
		const entries = [...top, ...unsure].sort(Buffer.compare).map(read).filter(Boolean);
		yield* sortInMemory(storage, entries, sort, wanted, allowDisk);
	}

	/**
	 * Documents matching `filter` in the order of the keys of `path` (_id: the documents
	 * themselves; else its index), ascending or `descending`. `plan`: the filter's own ranges on
	 * that field, walked in the order of their values; else the whole index, one type after
	 * another in MongoDB's order of types (keys order them otherwise), values that keys do not
	 * order (documents, dates, binary) sorted in memory. `wanted`: documents needed (0: all).
	 */
	*#inOrder(storage, filter, path, descending, plan, window, under = null) {
		const byId = path == '_id' && !under;
		// `under`: a prefix of a compound index's keys, whose next field is `path`
		const base = under ?? (byId ? [this.#docs] : [this.#entries, path]);
		const pack = (value) => storage.pack(value);
		// a document with several values here comes up at each: keep its first, the smallest
		// ascending, the largest descending
		const seen = !byId && this.#indexes(storage).multikey.has(base[1]) ? new Set() : null;
		const options = {
			filter: nativeFilter(filter, (value) => storage.encode(value), storage.structureTable()).bytes,
			docs: byId ? undefined : this.#docs,
			entries: true,
			reverse: descending,
			wanted: window.wanted,
			pause: true,
		};
		// with every document matching and none twice, skipped documents need not be read:
		// whole ranges go by their count of keys, the rest by the cursor
		const skipsKeys = Object.keys(filter).length == 0 && !seen;
		const fresh = (item) => {
			if (!seen) return true;
			const id = item.key.toString('latin1');
			if (seen.has(id)) return false;
			seen.add(id);
			return true;
		};
		const walk = function* (range) {
			let skip = 0;
			if (skipsKeys && window.skip > 0) {
				const keys = storage.countRange(...range, undefined, window.skip + 1);
				if (keys <= window.skip) {
					window.skip -= keys;
					if (options.wanted > 0) options.wanted = Math.max(1, options.wanted - keys);
					return;
				}
				[skip, window.skip] = [window.skip, 0];
			}
			const wanted = options.wanted > 0 ? Math.max(1, options.wanted - skip) : 0;
			for (const item of storage.scan(...range, { ...options, skip, wanted })) {
				if (item instanceof Wait) yield item;
				else if (matches(item.value, filter) && fresh(item)) yield item.value;
			}
		};
		if (plan) {
			const order = plan.ranges.map((range, i) => [range, plan.values?.[i]]);
			if (plan.values) order.sort((a, b) => compareValues(a[1], b[1]));
			if (descending) order.reverse();
			for (const [range] of order) yield* walk(range);
			return;
		}
		// the documents with values keys do not order, sorted here and let out where their type goes
		let unordered = null;
		const collect = function* () {
			if (unordered) return;
			const found = [];
			for (const item of storage.scan(...keyRangeOfOthers(base), options)) {
				if (item instanceof Wait) yield item;
				else if (matches(item.value, filter)) found.push(item);
			}
			const keyed = found.map((item) => ({ item, key: sortKey(item.value, path, descending) }));
			keyed.sort((a, b) => (descending ? -1 : 1) * compareSortKeys(a.key, b.key));
			unordered = keyed;
		};
		// documents, arrays, binary sort before ObjectIds; dates, regular expressions after booleans;
		// a document sorting by a value of another type comes out in that type's walk
		const letOut = function* (low) {
			yield* collect();
			for (const { item, key } of unordered) {
				const r = rank(key);
				const here = low ? r > RANK.string && r < RANK.objectId : r > RANK.boolean;
				if (here && fresh(item)) yield item.value;
			}
		};
		const segments = [
			[keyRange([...base, Symbol(EMPTY_ARRAY_ELEMENT)], byId), walk],
			[keyRange([...base, null], byId), walk],
			[keyRange([...base, NaN], byId), walk],
			[TYPE_BOUNDS.number(base, byId), walk],
			[TYPE_BOUNDS.string(base, byId), walk],
			[keyRangeOfOthers(base), () => letOut(true)],
			[TYPE_BOUNDS.objectId(base, byId, pack), walk],
			[[keyRange([...base, false], byId)[0], keyRange([...base, true], byId)[1]], walk],
			[keyRangeOfOthers(base), () => letOut(false)],
		];
		yield* inTypeOrder(storage, base, segments, descending);
	}

	/**
	 * A compound index that gives the order of `sort` for `filter`: its first fields fixed by
	 * equalities of the filter (`equalities` of them), the next ones the sort's fields in order.
	 * `base`: the key prefix of those equalities. None for an index with arrays (multikey):
	 * a document sorts by its smallest or largest element there, not by its keys.
	 */
	#compoundFor(storage, filter, sort) {
		const pack = (value) => storage.pack(value);
		const indexes = this.#indexes(storage);
		const equal = new Map();
		const collect = (part) => {
			for (const path in part) {
				if (path == '$and') part[path].forEach(collect);
				else if (!path.startsWith('$') && !equal.has(path) && isEquality(part[path]) && !Array.isArray(part[path])) equal.set(path, part[path]);
			}
		};
		collect(filter);
		for (const index of indexes.ready) {
			const { fields } = specOf(indexes.registry.get(index));
			if (fields.length < 2 || indexes.multikey.has(index)) continue;
			let j = 0;
			while (j < fields.length && equal.has(fields[j]) && !sort.some(([path]) => path == fields[j])) j++;
			if (j + sort.length > fields.length || !sort.every(([path], i) => fields[j + i] == path)) continue;
			try {
				return { base: [this.#entries, index, ...fields.slice(0, j).map((field) => keyElement(equal.get(field), pack))], equalities: j };
			} catch (error) {
				if (!(error instanceof KeyTooLargeError)) throw error;
			}
		}
		return null;
	}

	/** The documents under key prefix `base` in the order of `sort` from its field `level` on. */
	*#levels(storage, filter, base, sort, level, window) {
		const [path, direction] = sort[level];
		if (level == sort.length - 1) {
			yield* this.#inOrder(storage, filter, path, direction < 0, null, window, base);
			return;
		}
		for (const element of this.#groups(storage, base, direction < 0)) yield* this.#levels(storage, filter, [...base, element], sort, level + 1, window);
	}

	/**
	 * The distinct key elements that follow the index key prefix `base`, in MongoDB's order of
	 * their values (descending: reversed): from one group of keys to the next, without reading
	 * documents; values keys do not order (documents, dates, binary) sorted here.
	 */
	*#groups(storage, base, descending) {
		// the key's parts: 'i', the collection's id, the index, the rest of `base`, then this one
		const position = base.length + 1;
		const seek = function* ([start, end], reverse) {
			for (;;) {
				const found = storage.firstKey(start, end, reverse);
				if (!found) return;
				const element = readKey(found, 0, found.length)[position];
				yield element;
				const [at, after] = prefixRange([...base, element]);
				if (reverse) end = at;
				else start = after;
			}
		};
		let others = null;
		const letOut = function* (low) {
			others ??= [...seek(keyRangeOfOthers(base), false)]
				.map((element) => ({ element, value: elementValue(element, storage) }))
				.sort((a, b) => (descending ? -1 : 1) * compareValues(a.value, b.value));
			for (const { element, value } of others) {
				const r = rank(value);
				if (low ? r > RANK.string && r < RANK.objectId : r > RANK.boolean) yield element;
			}
		};
		const pack = (value) => storage.pack(value);
		const bySeek = (range) => seek(range, descending);
		const segments = [
			[keyRange([...base, Symbol(EMPTY_ARRAY_ELEMENT)], false), bySeek],
			[keyRange([...base, null], false), bySeek],
			[keyRange([...base, NaN], false), bySeek],
			[TYPE_BOUNDS.number(base, false), bySeek],
			[TYPE_BOUNDS.string(base, false), bySeek],
			[keyRangeOfOthers(base), () => letOut(true)],
			[TYPE_BOUNDS.objectId(base, false, pack), bySeek],
			[[keyRange([...base, false], false)[0], keyRange([...base, true], false)[1]], bySeek],
			[keyRangeOfOthers(base), () => letOut(false)],
		];
		yield* inTypeOrder(storage, base, segments, descending);
	}

	/**
	 * How to find the documents `filter` may match, EMPTY when none can: `keys` of documents
	 * (by _id), else `ranges` of document keys or (`index`) of index entries, walked in turn.
	 * `dedupe`: a document may come up more than once; `exact`: the ranges hold exactly the
	 * matches of the one field they come from; `prefix`: the one index value of an equality.
	 * The most selective of the filter's _id and indexed fields wins; none: a full scan.
	 */
	#plan(storage, filter) {
		const pack = (value) => storage.pack(value);
		const indexes = this.#indexes(storage);
		const candidates = [];
		// conditions that all hold (those of $and parts too), and lists of alternatives ($or)
		const conjuncts = [];
		const alternatives = [];
		// conditions an $elemMatch implies: on the array's elements (`tight`: one element must
		// meet all its bounds), or on the fields of its documents; never all a match needs
		const implied = [];
		const tight = new Set();
		const collect = (part) => {
			for (const path in part) {
				if (path == '$and') part[path].forEach(collect);
				else if (path == '$or') alternatives.push(part[path]);
				// $nor: no index finds what does not match
				else if (path == '$nor') continue;
				else conjuncts.push([path, part[path]]);
				const spec = isOperators(part[path]) ? part[path].$elemMatch : undefined;
				if (spec === undefined) continue;
				if (isElementOperators(spec)) {
					implied.push([path, spec]);
					tight.add(path);
				} else {
					for (const field in spec) if (!field.startsWith('$')) implied.push([`${path}.${field}`, spec[field]]);
				}
			}
		};
		collect(filter);
		const keyTooLarge = (error) => {
			if (error instanceof KeyTooLargeError) return EMPTY; // no value that large was ever stored
			throw error;
		};
		for (const [path, condition] of conjuncts) {
			if (path != '_id') continue;
			let source;
			try {
				source = this.#source([this.#docs], true, false, condition, pack);
			} catch (error) {
				return keyTooLarge(error);
			}
			if (source === EMPTY) return EMPTY;
			if (source === null) continue;
			// document keys: read them, or walk them
			if (source.exact && !source.range) candidates.push({ keys: source.prefixes.map((prefix) => encodeKey(prefix)), path, exact: true });
			else candidates.push({ ranges: source.ranges, values: source.values, path, exact: source.exact });
		}
		// indexes: equalities on their first fields make a prefix, the next field's condition
		// narrows it further
		// a condition holding the $elemMatch leaves it to the implied ones
		const plain = (condition) => (isOperators(condition) && '$elemMatch' in condition ? withoutKey(condition, '$elemMatch') : condition);
		const conditions = new Map();
		const inexact = new Set();
		for (const [path, condition] of conjuncts) {
			if (conditions.has(path)) continue;
			if (plain(condition)) conditions.set(path, plain(condition));
			if (plain(condition) !== condition) inexact.add(path);
		}
		for (const [path, condition] of implied) {
			if (conditions.has(path)) continue;
			conditions.set(path, condition);
			inexact.add(path);
		}
		for (const index of indexes.ready) {
			const { fields } = specOf(indexes.registry.get(index));
			const multikey = indexes.multikey.has(index);
			const base = [this.#entries, index];
			let source = null;
			try {
				let j = 0;
				while (j < fields.length && conditions.has(fields[j])) {
					const condition = conditions.get(fields[j]);
					if (!isEquality(condition) || Array.isArray(condition)) break;
					base.push(keyElement(condition, pack));
					j++;
				}
				if (j < fields.length && conditions.has(fields[j])) {
					const next = fields[j];
					source = this.#source(base, false, multikey && !(tight.has(next) && inexact.has(next)), conditions.get(next), pack);
				}
				if (source && source !== EMPTY) source.covers = fields.slice(0, j + 1);
				if (source === null && j > 0) source = { ranges: [prefixRange(base)], values: fields.length == 1 ? [conditions.get(fields[0])] : undefined, prefixes: [base], prefix: base, exact: true, covers: fields.slice(0, j) };
				// no equality on the first field, one on the second: the first's values, each
				// with the second's, narrow down both (a range on the first alone does one)
				if ((source === null || source?.range) && j == 0 && !multikey && fields.length > 1 && conditions.has(fields[0]) && conditions.has(fields[1])) {
					const skipped = this.#skipScan(storage, index, conditions.get(fields[0]), conditions.get(fields[1]), pack);
					if (skipped) source = skipped === EMPTY ? EMPTY : { ...skipped, covers: fields.slice(0, 2) };
				}
			} catch (error) {
				return keyTooLarge(error);
			}
			if (source === EMPTY) return EMPTY;
			if (source === null) continue;
			source.index = true;
			// a null equality on the way: the null key holds documents it does not match; a path
			// with a position: keys hold the elements of an array it ends at, matched as a whole
			if (source.covers?.some((field) => inexact.has(field) || conditions.get(field) === null || /(^|\.)\d+(\.|$)/.test(field))) source.exact = false;
			if (fields.length == 1) {
				source.path = fields[0];
				source.dedupe = (source.range || source.ranges.length > 1) && multikey;
			} else {
				// several fields: ordered walks are for one field; exact counts hold for the
				// fields the keys narrow down
				source.values = undefined;
				source.dedupe = multikey;
				if (source.range || source.ranges.length > 1) source.prefix = undefined;
			}
			candidates.push(source);
		}
		if (candidates.length == 0) {
			// $or whose every alternative an index narrows down: the union of their plans
			for (const list of alternatives) {
				const plans = [];
				for (const alternative of list) {
					const plan = this.#plan(storage, alternative);
					if (plan === EMPTY) continue;
					plans.push({ ...plan, alternative });
				}
				if (plans.length == 0) return EMPTY;
				if (plans.every((plan) => !plan.scan)) {
					candidates.push({ union: plans });
					break;
				}
			}
		}
		if (candidates.length == 0) return { ranges: [prefixRange([this.#docs])], scan: true };
		// an index narrowing down fewer fields than another is not worth counting
		const narrower = (a, b) => a.covers && b.covers && a.covers.length < b.covers.length && a.covers.every((field) => b.covers.includes(field));
		if (candidates.length > 1) {
			const kept = candidates.filter((a) => !candidates.some((b) => narrower(a, b)));
			candidates.splice(0, candidates.length, ...kept);
		}
		if (candidates.length == 1) return candidates[0];
		// the fewest keys wins; past PLAN_COUNT they count as many, and the index narrowing down
		// more of the filter's fields wins
		let best = null;
		let fewest = Infinity;
		const narrows = (candidate) => candidate.covers?.length ?? 1;
		for (const candidate of candidates) {
			let count = 0;
			if (candidate.keys) count = candidate.keys.length;
			else {
				for (const [start, end] of candidate.ranges) {
					count += storage.countRange(start, end, undefined, PLAN_COUNT - count);
					if (count >= PLAN_COUNT) break;
				}
			}
			if (count == 0) return EMPTY;
			count = Math.min(count, PLAN_COUNT);
			if (count < fewest || (count == fewest && count == PLAN_COUNT && narrows(candidate) > narrows(best))) [best, fewest] = [candidate, count];
		}
		return best;
	}

	/**
	 * Ranges of an index on several fields whose first field's condition is no equality (a
	 * range, $ne, $nin...) and whose second's is equalities: its distinct first values that pass,
	 * each with the second's values, as MongoDB's index bounds are for such filters. null when
	 * the second condition is no equality or the first field has too many values.
	 */
	#skipScan(storage, index, first, second, pack) {
		const plainValue = (value) => isEquality(value) && !Array.isArray(value) && value !== null;
		let values;
		if (plainValue(second)) values = [second];
		else if (isOperators(second) && Object.keys(second).length == 1 && Array.isArray(second.$in) && second.$in.every(plainValue)) values = second.$in;
		else return null;
		// indexes whose first field was found to have too many values are not tried again
		const indexes = this.#indexes(storage);
		let wide = WIDE_FIRST_FIELDS.get(indexes);
		if (!wide) WIDE_FIRST_FIELDS.set(indexes, (wide = new Set()));
		if (wide.has(index)) return null;
		const base = [this.#entries, index];
		let [start, end] = prefixRange(base);
		const kept = [];
		let exact = true;
		for (let seen = 0; ; seen++) {
			const found = storage.firstKey(start, end);
			if (!found) break;
			if (seen == SKIP_SCAN_VALUES) {
				wide.add(index);
				return null;
			}
			// the key's parts: 'i', the collection's id, the index, then this value
			const element = readKey(found, 0, found.length)[3];
			start = prefixRange([...base, element])[1];
			const value = elementValue(element, storage);
			// the null key also holds documents without the field; documents and arrays are
			// told apart by their documents, not their keys
			const test = (v) => matches({ v }, { v: first });
			let passes = test(value);
			if (value === null || typeof value == 'object') {
				const missing = matches({}, { v: first });
				if (passes != missing || typeof value == 'object') exact = false;
				passes ||= missing;
			}
			if (passes) kept.push(element);
		}
		if (kept.length == 0) return EMPTY;
		const ranges = [];
		for (const element of kept) for (const value of values) ranges.push(prefixRange([...base, element, keyElement(value, pack)]));
		ranges.sort((a, b) => Buffer.compare(a[0], b[0]));
		return { ranges, range: true, exact };
	}

	/**
	 * The key ranges under `base` that hold the values `condition` accepts: one per value of an
	 * equality or $in; one for comparisons, within their type (numbers, strings, ObjectIds;
	 * others are not ordered in keys). `terminal`: the value ends the key (documents), else an
	 * _id follows it (index entries). `multikey`: a document may have several values there, and
	 * each comparison may hold for another one, so only the bounds of one side narrow the range.
	 * null when keys cannot narrow it down ($ne, arrays, other types), EMPTY when nothing
	 * matches.
	 */
	#source(base, terminal, multikey, condition, pack) {
		if (isOperators(condition) && '$eq' in condition) {
			// equality to the value as it is: an object with $ keys is a value too
			const operand = condition.$eq;
			if (operand instanceof RegExp) return EMPTY;
			if (Array.isArray(operand)) return null;
			const prefix = [...base, keyElement(operand, pack)];
			return { ranges: [keyRange(prefix, terminal)], values: [operand], prefixes: [prefix], prefix, exact: operand !== null && Object.keys(condition).length == 1 };
		}
		if (condition instanceof RegExp || (isOperators(condition) && '$regex' in condition)) {
			const range = regexRange(base, condition);
			if (!range) return null;
			const alone = condition instanceof RegExp || Object.keys(condition).every((op) => op == '$regex' || op == '$options');
			return { ranges: [range.range], range: true, exact: range.whole && alone && !multikey };
		}
		if (!isOperators(condition)) {
			if (Array.isArray(condition)) return null;
			const prefix = [...base, keyElement(condition, pack)];
			// null: the null key is also that of paths through arrays that match no null
			return { ranges: [keyRange(prefix, terminal)], values: [condition], prefixes: [prefix], prefix, exact: condition !== null };
		}
		const ops = Object.keys(condition);
		if ('$all' in condition) {
			// every value is there: the documents with one of them hold the matches
			const item = condition.$all.find((value) => isEquality(value) && !Array.isArray(value));
			if (item === undefined) return condition.$all.length == 0 ? EMPTY : null;
			const source = this.#source(base, terminal, multikey, item, pack);
			if (source === null || source === EMPTY) return source;
			// one value alone: the same as equality to it
			return { ...source, exact: source.exact && ops.length == 1 && condition.$all.length == 1 };
		}
		if ('$in' in condition) {
			const values = condition.$in;
			if (values.some((value) => Array.isArray(value))) return null;
			if (values.length == 0) return EMPTY;
			if (values.some((value) => value instanceof RegExp)) {
				// strings that start with a regular expression's prefix, and the values: merged
				// where they overlap, so no key comes up twice
				const ranges = [];
				for (const value of values) {
					if (!(value instanceof RegExp)) ranges.push(keyRange([...base, keyElement(value, pack)], terminal));
					else {
						const range = regexRange(base, value);
						if (!range) return null;
						ranges.push(range.range);
					}
				}
				ranges.sort((a, b) => Buffer.compare(a[0], b[0]));
				const merged = [ranges[0]];
				for (const [start, end] of ranges.slice(1)) {
					const last = merged[merged.length - 1];
					if (Buffer.compare(start, last[1]) <= 0) {
						if (Buffer.compare(end, last[1]) > 0) last[1] = end;
					} else merged.push([start, end]);
				}
				return { ranges: merged, range: true, exact: false };
			}
			const distinct = new Map(values.map((value) => [elementId(keyElement(value, pack)), value]));
			const prefixes = [...distinct.values()].map((value) => [...base, keyElement(value, pack)]);
			// keys in order: the walk goes forward through the index
			const order = prefixes.map((prefix, i) => [keyRange(prefix, terminal), [...distinct.values()][i]]).sort((a, b) => Buffer.compare(a[0][0], b[0][0]));
			return { ranges: order.map(([range]) => range), values: order.map(([, value]) => value), prefixes, exact: ops.length == 1 && !values.includes(null) };
		}
		let comparisons = ops.filter((op) => COMPARISON_OPS.has(op));
		if (comparisons.length == 0) {
			// a missing field has the null key (so does null: a superset)
			if ('$exists' in condition && !condition.$exists) return { ranges: [keyRange([...base, null], terminal)], exact: false };
			return null;
		}
		if (multikey) {
			// bounds of one side hold for one value together: the tightest does for all
			const lower = comparisons.filter((op) => op == '$gt' || op == '$gte');
			comparisons = lower.length > 0 ? lower : comparisons;
		}
		const operands = comparisons.map((op) => condition[op]);
		if (operands.some((operand) => operand === null || Number.isNaN(operand))) return null;
		const typeOf = (operand) => (operand instanceof ObjectId ? 'objectId' : typeof operand);
		// one value has one type: bounds of two types exclude each other, unless each bound
		// may hold for another value; then one bound narrows the range
		if (multikey) comparisons = comparisons.filter((op) => typeOf(condition[op]) == typeOf(operands[0]));
		const kinds = new Set(comparisons.map((op) => typeOf(condition[op])));
		if (kinds.size > 1) return EMPTY;
		const [kind] = kinds;
		const bracket = TYPE_BOUNDS[kind];
		if (!bracket) return null;
		let [low, high] = bracket(base, terminal, pack);
		for (const op of comparisons) {
			const [at, after] = keyRange([...base, keyElement(condition[op], pack)], terminal);
			if (op == '$gt' && Buffer.compare(after, low) > 0) low = after;
			if (op == '$gte' && Buffer.compare(at, low) > 0) low = at;
			if (op == '$lt' && Buffer.compare(at, high) < 0) high = at;
			if (op == '$lte' && Buffer.compare(after, high) < 0) high = after;
		}
		if (Buffer.compare(low, high) >= 0) return EMPTY;
		return { ranges: [[low, high]], range: true, exact: !multikey && comparisons.length == ops.length };
	}

	/**
	 * Creates an index on one field or several ({ a: 1, b: -1 }, dot paths allowed), unique
	 * with `{ unique: true }`, and fills it from the documents already stored. Resolves to the
	 * index name, e.g. "email_1".
	 */
	async createIndex(keys, options = {}) {
		const spec = typeof keys == 'string' ? { [keys]: 1 } : keys;
		const fields = spec !== null && typeof spec == 'object' && !Array.isArray(spec) ? Object.keys(spec) : [];
		if (fields.length == 0) throw new TypeError('createIndex needs the fields to index, e.g. { email: 1 }');
		const directions = fields.map((field) => spec[field]);
		if (directions.some((d) => d !== 1 && d !== -1)) throw new Error('mostik: index direction must be 1 or -1');
		if (options.unique !== undefined && typeof options.unique != 'boolean') throw new TypeError('unique must be true or false');
		const unique = options.unique === true;
		const name = options.name ?? fields.map((field, i) => `${field}_${directions[i]}`).join('_');
		// the _id index exists already, unique
		if (fields.length == 1 && fields[0] == '_id') return '_id_';

		const storage = this.#client._storage();
		// an index makes the collection exist, as in MongoDB
		this.#ensure(storage);
		const index = fields.join('\0');
		const indexes = this.#indexes(storage);
		const existing = indexes.registry.get(index);
		if (existing) {
			const { directions: had, unique: wasUnique } = specOf(existing);
			if (had.join() == directions.join() && wasUnique == unique) return existing.name;
			throw new MongoServerError(`Index already exists with a different name or options: ${existing.name}`, { code: 85, codeName: 'IndexOptionsConflict' });
		}
		if (indexes.building.has(index)) {
			await indexes.building.get(index);
			return name;
		}
		// Writes to this collection wait until the index is built (reads do not): the index is
		// filled from a snapshot of the documents that nothing changes meanwhile.
		const build = this.#exclusive(storage, async (indexes) => {
			const entry = await this.#buildIndex(storage, index, { fields, directions, name, unique });
			indexes.all.add(index);
			indexes.ready.add(index);
			indexes.names.set(index, name);
			indexes.registry.set(index, entry);
			if (entry.multikey) {
				indexes.multikey.add(index);
				indexes.savedMultikey.add(index);
			}
		});
		indexes.building.set(index, build);
		try {
			await build;
		} finally {
			indexes.building.delete(index);
		}
		return name;
	}

	/**
	 * Drops the index named `index` (e.g. "email_1"), or with the key pattern `index`
	 * ({ email: 1 }). Resolves to `{ nIndexesWas, ok: 1 }`.
	 */
	async dropIndex(index) {
		const storage = this.#client._storage();
		if (index === '_id_' || (isPlainObjectSpec(index) && Object.keys(index).join() == '_id'))
			throw new MongoServerError('cannot drop _id index', { code: 72, codeName: 'InvalidOptions' });
		if (typeof index != 'string' && !isPlainObjectSpec(index)) throw new TypeError('dropIndex takes an index name or key pattern');
		if (!this.#exists(storage)) throw namespaceNotFound(this.namespace);
		const found = () => {
			const entries = [...this.#indexes(storage).registry.values()];
			if (typeof index == 'string') return entries.find((entry) => entry.name == index);
			const keys = Object.keys(index);
			return entries.find((entry) => {
				const { fields, directions } = specOf(entry);
				return fields.join('\0') == keys.join('\0') && directions.every((d, i) => d == index[keys[i]]);
			});
		};
		const building = typeof index == 'string' ? undefined : this.#indexes(storage).building.get(Object.keys(index).join('\0'));
		if (building) await building.catch(() => {});
		const entry = found();
		if (!entry) {
			const shown = typeof index == 'string' ? index : JSON.stringify(index);
			throw new MongoServerError(`index not found with name [${shown}]`, { code: 27, codeName: 'IndexNotFound' });
		}
		return this.#exclusive(storage, async (indexes) => {
			const nIndexesWas = indexes.registry.size + 1;
			// writes to come leave the index alone; those queued so far are committed first
			for (const set of [indexes.all, indexes.ready, indexes.multikey, indexes.savedMultikey]) set.delete(entry.field);
			indexes.names.delete(entry.field);
			indexes.registry.delete(entry.field);
			await storage.flush();
			// entries and registry entry in one commit, the entries a whole subtree at a time
			await storage.deleteRanges([prefixRange([this.#entries, entry.field])], [encodeKey([this.#registry, entry.field])]);
			return { nIndexesWas, ok: 1 };
		});
	}

	/** Descriptions of the indexes (`{ v, key, name }`), _id first, as MongoDB lists them. */
	listIndexes() {
		const storage = this.#client._storage();
		const namespace = this.namespace;
		return new FindCursor(() => {
			if (!this.#exists(storage)) throw namespaceNotFound(namespace);
			const indexes = [{ v: 2, key: { _id: 1 }, name: '_id_' }];
			// in the order they were created, as MongoDB lists them
			const entries = [...this.#indexes(storage).registry.values()].sort((a, b) => (a.created ?? 0) - (b.created ?? 0));
			for (const entry of entries) {
				const { fields, directions, unique } = specOf(entry);
				indexes.push({ v: 2, key: Object.fromEntries(fields.map((f, i) => [f, directions[i]])), name: entry.name, ...(unique ? { unique } : {}) });
			}
			return indexes.values();
		});
	}

	/**
	 * Removes the collection: its documents, indexes and count. Resolves to true, or false when
	 * it did not exist (as the MongoDB driver does).
	 */
	async drop() {
		const storage = this.#client._storage();
		// an index being built finishes first; it would hold the collection meanwhile anyway
		await Promise.allSettled([...this.#indexes(storage).building.values()]);
		return this.#exclusive(storage, async () => {
			if (!this.#exists(storage)) return false;
			await storage.flush();
			// everything in one commit, whole subtrees at a time
			const id = this.#indexesId;
			const ranges = [prefixRange([this.#registry]), prefixRange([this.#docs]), prefixRange([this.#entries])];
			await storage.deleteRanges(ranges, [this.#counter, storage.uncatalog(this.#db, this.#name)]);
			storage.indexes.delete(id);
			return true;
		});
	}

	/**
	 * The E11000 error for writing `doc`: its _id taken, or a value a unique index holds for
	 * another document (the first found).
	 */
	#duplicate(storage, doc) {
		const pack = (value) => storage.pack(value);
		const id = storage.decode(storage.encode(doc._id));
		const idElement = keyElement(id, pack);
		const own = encodeKey([this.#docs, idElement]);
		const taken = storage.getBytes([own]);
		// the _id is another document's (an update keeps its own _id: that one is fine)
		if (taken !== undefined && !doc[UPDATING]) {
			return new MongoServerError(duplicateMessage(this.namespace, doc._id), { code: 11000, keyPattern: { _id: 1 }, keyValue: { _id: doc._id } });
		}
		const { registry, all } = this.#indexes(storage);
		const stored = storage.decode(storage.encode(doc));
		for (const index of all) {
			const { fields, unique } = specOf(registry.get(index));
			if (!unique) continue;
			for (const tuple of indexTuples(stored, fields, pack)) {
				const mine = encodeKey([this.#entries, index, ...tuple, idElement]);
				for (const key of storage.keys(...prefixRange([this.#entries, index, ...tuple]))) {
					if (!key.equals(mine)) return this.#duplicateError(storage, registry.get(index).name, fields, key);
				}
			}
		}
		return new MongoServerError(duplicateMessage(this.namespace, doc._id), { code: 11000, keyPattern: { _id: 1 }, keyValue: { _id: doc._id } });
	}

	/** The E11000 error for the entry `key` of the unique index `name` on `fields`. */
	#duplicateError(storage, name, fields, key) {
		const parts = readKey(key, 0, key.length);
		// 'i', the collection's id, the index, then the values
		const keyValue = Object.fromEntries(fields.map((field, i) => [field, elementValue(parts[3 + i], storage)]));
		const shown = fields.map((field) => `${field}: ${showId(keyValue[field])}`).join(', ');
		return new MongoServerError(`E11000 duplicate key error collection: ${this.namespace} index: ${name} dup key: { ${shown} }`, {
			code: 11000,
			keyPattern: Object.fromEntries(fields.map((field, i) => [field, specOf(this.#indexes(storage).registry.get(fields.join('\0')) ?? { fields }).directions[i] ?? 1])),
			keyValue,
		});
	}

	/** Whether the collection exists: it got a document or an index, and was not dropped since. */
	#exists(storage) {
		return storage.collectionId(this.#db, this.#name) !== undefined;
	}

	/**
	 * Renames the collection to `to` in its database: one catalog change, whatever its size.
	 * The target must not exist (the caller drops it first when asked to).
	 */
	async [RENAME](to) {
		const storage = this.#client._storage();
		await Promise.allSettled([...this.#indexes(storage).building.values()]);
		return this.#exclusive(storage, async () => {
			const id = storage.collectionId(this.#db, this.#name);
			if (id === undefined) throw namespaceNotFound(`${this.#db}.${this.#name}`, 'Source collection');
			if (storage.collectionId(this.#db, to) !== undefined) {
				throw new MongoServerError(`Target namespace exists: ${this.#db}.${to}`, { code: 48, codeName: 'NamespaceExists' });
			}
			await storage.flush();
			const group = storage.group();
			group.remove([storage.uncatalog(this.#db, this.#name)]);
			storage.catalogAs(group, this.#db, to, id);
			await group.commit();
		});
	}

	/** Key of the collection's entry in `storage.indexes`: its id (a collection not yet created has none). */
	get #indexesId() {
		const id = this.#keys().id;
		return id === '' ? `\0${this.#db}\0${this.#name}` : id;
	}

	/**
	 * Runs `fn(indexes)` while writes to this collection wait (reads do not); one such operation
	 * at a time.
	 */
	async #exclusive(storage, fn) {
		let indexes = this.#indexes(storage);
		while (indexes.fence) {
			await indexes.fence;
			// the collection may have been dropped meanwhile: look again
			indexes = this.#indexes(storage);
		}
		let unfence;
		indexes.fence = new Promise((resolve) => (unfence = resolve));
		try {
			return await fn(indexes);
		} finally {
			indexes.fence = null;
			unfence();
		}
	}

	/** Waits while an index build holds writes to this collection. */
	async #unfenced(storage) {
		const indexes = this.#indexes(storage);
		while (indexes.fence) await indexes.fence;
	}

	/**
	 * Fills the index on `field` from the stored documents: entries are sorted in the native
	 * layer (spilling to temporary files when large) and written in key order, each index page
	 * written once, full. They are not journaled: the index only counts once a checkpoint has
	 * written them and the registry names it, so a crash midway leaves no half-built index.
	 */
	async #buildIndex(storage, field, { fields, directions, name, unique }) {
		// writes queued before the fence went up
		await storage.flush();
		const range = prefixRange([this.#entries, field]);
		// start from an empty range: a build cut short by a crash may have left entries behind
		await storage.deleteRanges([range]);
		const pack = (value) => storage.pack(value);
		let multikey = false;
		const loader = storage.native.sortedLoader(`${storage.dataPath}-sort-${process.pid}-${Date.now()}-`, unique);
		let chunk = Buffer.allocUnsafe(4 << 20);
		let at = 0;
		const push = (key, value) => {
			const size = 8 + key.length + value.length;
			if (at + size > chunk.length) {
				if (at > 0) loader.add(chunk.subarray(0, at));
				at = 0;
				if (size > chunk.length) chunk = Buffer.allocUnsafe(size);
			}
			chunk.writeUInt32LE(key.length, at);
			key.copy(chunk, at + 4);
			chunk.writeUInt32LE(value.length, at + 4 + key.length);
			value.copy(chunk, at + 8 + key.length);
			at += size;
		};
		try {
			const documents = storage.scan(...prefixRange([this.#docs]), { entries: true, pause: true });
			for (;;) {
				let next = step(documents);
				if (next instanceof Promise) next = await next;
				if (next.done) break;
				const { key, value } = next.value;
				const idElement = keyElement(value._id, pack);
				suffixBytes.writeUInt16LE(key.length - this.#docs.length - 1, 0);
				const tuples = indexTuples(value, fields, pack);
				for (const tuple of tuples) {
					push(encodeKey([this.#entries, field, ...tuple, idElement]), suffixBytes);
				}
				if (tuples.length > 1) multikey = true;
			}
			if (at > 0) loader.add(chunk.subarray(0, at));
			await loader.finish();
			// the entries are unjournaled: on disk before the registry says the index exists
			await storage.native.checkpoint();
		} catch (error) {
			await storage.deleteRanges([range]).catch(() => {});
			const duplicate = /^mostik: duplicate key ([0-9a-f]+)$/.exec(error.message);
			if (duplicate) throw this.#duplicateError(storage, name, fields, Buffer.from(duplicate[1], 'hex'));
			throw tooLarge(error, new Set(fields));
		}
		const group = storage.group();
		const entry = { field, fields, directions, name, multikey, created: Date.now(), ...(unique ? { unique } : {}) };
		group.put([this.#registry, field], entry);
		await group.commit();
		return entry;
	}

	/**
	 * Records, with the write in `group`, that a document has several entries in the index on
	 * `field`. Every such write carries the record until the registry is known to hold it: a
	 * group can be rejected (the writes of one commit to the same key cost one).
	 */
	#markMultikey(group, storage, field) {
		const indexes = this.#indexes(storage);
		if (indexes.savedMultikey.has(field) || !indexes.registry.has(field)) return;
		indexes.multikey.add(field);
		group.put([this.#registry, field], { ...indexes.registry.get(field), multikey: true });
	}

	/** Indexed fields of this collection: `all` get entries on writes, `ready` answer queries. */
	#indexes(storage) {
		const id = this.#indexesId;
		let indexes = storage.indexes.get(id);
		if (!indexes) {
			const registered = [...storage.scan(...prefixRange([this.#registry]))];
			const fields = registered.map((entry) => entry.field);
			indexes = {
				all: new Set(fields),
				ready: new Set(fields),
				names: new Map(registered.map((entry) => [entry.field, entry.name])),
				// field -> its registry entry
				registry: new Map(registered.map((entry) => [entry.field, entry])),
				// fields with documents that have several entries in the index (array values)
				multikey: new Set(registered.filter((entry) => entry.multikey).map((entry) => entry.field)),
				// ... of which the registry is known to say so
				savedMultikey: new Set(registered.filter((entry) => entry.multikey).map((entry) => entry.field)),
				building: new Map(),
				fence: null,
			};
			storage.indexes.set(id, indexes);
		}
		return indexes;
	}
}

// index candidates find() fetches one by one before opening a cursor
const FIRST_LOOKUPS = 4;

// #plan: some filter value proves there is no match
const EMPTY = Symbol('empty');

/** Whether a plan part of a $or finds exactly the documents of its alternative: its one field's keys. */
function exactAlone(part) {
	const paths = Object.keys(part.alternative);
	return !part.union && part.exact && paths.length == 1 && paths[0] == part.path && !part.dedupe && (part.keys || part.ranges);
}

const COMPARISON_OPS = new Set(['$gt', '$gte', '$lt', '$lte']);

// #count: index keys counted on the JS thread up to this many, more off it
const SYNC_COUNT = 65536;
// #plan: candidates' keys are counted up to this many
const PLAN_COUNT = 1000;
// #skipScan: an index's first field with more distinct values than this is walked, not skipped
const SKIP_SCAN_VALUES = 4096;
// #skipScan: per collection's indexes, those whose first field has more values than that
const WIDE_FIRST_FIELDS = new WeakMap();
// #sorted: at most this many candidates are sorted in memory rather than read in index order
const SORT_IN_MEMORY = 1000;
// a sort keeping at most this many documents reads the others' sort fields only
const TOP_BY_KEYS = 10_000;
// #sorted: candidates counted up to this many when choosing between walking the sort's index
// and sorting them
const WALK_OR_SORT = 100_000;

/** `[[path, 1 | -1], ...]` for a sort specification, or null for an empty one. */
function sortSpec(spec) {
	let pairs;
	if (typeof spec == 'string') pairs = [[spec, 1]];
	else if (spec instanceof Map) pairs = [...spec];
	else if (Array.isArray(spec)) pairs = spec.length == 2 && typeof spec[0] == 'string' && !Array.isArray(spec[1]) ? [spec] : spec.map((p) => (typeof p == 'string' ? [p, 1] : p));
	else if (spec !== null && typeof spec == 'object') pairs = Object.entries(spec);
	else throw new TypeError('Invalid sort specification');
	const directions = { 1: 1, '-1': -1, asc: 1, ascending: 1, desc: -1, descending: -1 };
	const out = pairs.map((pair) => {
		if (!Array.isArray(pair) || typeof pair[0] != 'string' || pair[0].length == 0) throw new TypeError('Invalid sort specification');
		const direction = directions[typeof pair[1] == 'string' ? pair[1].toLowerCase() : pair[1]];
		if (direction === undefined) throw new MongoServerError(`Invalid sort direction ${JSON.stringify(pair[1])}`, { code: 2, codeName: 'BadValue' });
		return [pair[0], direction];
	});
	return out.length == 0 ? null : out;
}

/** A sort over the memory limit, not allowed to use temporary files (MongoDB's error 292). */
function sortMemoryError() {
	return new MongoServerError('Sort exceeded memory limit of 104857600 bytes, but did not opt in to external sorting.', {
		code: 292,
		codeName: 'QueryExceededMemoryLimitNoDiskUseAllowed',
	});
}

/**
 * Sorts matching entries (`{ key, value, bytes }`) by `sort`, keeping the first `wanted` only
 * (0: all). What does not fit the memory limits goes to temporary files (lib/spill.js), as
 * MongoDB does by default; `allowDisk: false` refuses to, with error 292. Only the documents'
 * bytes are held, read again as they come out. Yields their values, or the entries themselves
 * with their bytes (`asEntries`).
 */
function* sortInMemory(storage, entries, sort, wanted, allowDisk = true, asEntries = false) {
	const records = (function* () {
		for (const item of entries) yield item instanceof Wait ? item : { keys: sortKeys(item.value, sort, parallelSort), payload: item.bytes, key: item.key };
	})();
	const compare = (a, b) => {
		for (let i = 0; i < sort.length; i++) {
			const o = compareSortKeys(a[i], b[i]) * sort[i][1];
			if (o) return o;
		}
		return 0;
	};
	for (const record of sortRecords(records, { compare, wanted, base: storage.dataPath, codec: storage.plain, allowDisk, tooMuch: sortMemoryError })) {
		if (record instanceof Wait) yield record;
		else yield asEntries ? { key: record.key, bytes: record.payload } : storage.decode(record.payload);
	}
}

/**
 * Runs `segments` ([key range, walk of it]) in MongoDB's type order, reversed descending,
 * leaving out those outside the first and last keys under `base`: two seeks instead of walks
 * of types that are not there.
 */
function* inTypeOrder(storage, base, segments, descending) {
	const range = prefixRange(base);
	const first = storage.firstKey(...range, false);
	if (!first) return;
	const last = storage.firstKey(...range, true);
	if (descending) segments.reverse();
	for (const [range, walk] of segments) {
		if (Buffer.compare(range[1], first) > 0 && Buffer.compare(range[0], last) <= 0) yield* walk(range);
	}
}

/** Keys under `base` whose value is neither null, a number, a string, a boolean nor an ObjectId. */
function keyRangeOfOthers(base) {
	return [encodeKey([...base, Symbol('\u0001m')]), encodeKey([...base, Symbol('\u0001n')])];
}

/**
 * The keys under `base` of the strings a regular expression condition may match: those
 * starting with its literal prefix (`whole`: every one of them matches). null without a prefix.
 */
function regexRange(base, condition) {
	const found = regexPrefix(condition);
	if (!found) return null;
	const start = encodeKey([...base, found.prefix]);
	// no byte of UTF-8 is 0xff: every string continuing the prefix sorts below
	return { range: [start, Buffer.concat([start, Buffer.from([0xff])])], whole: found.whole };
}

function withoutKey(object, key) {
	const { [key]: _, ...rest } = object;
	return Object.keys(rest).length > 0 ? rest : null;
}

/**
 * The keys whose element after `parts[..-1]` is the last of `parts`: the key itself when it
 * ends there (`terminal`), else every key continuing it past a separator.
 */
function keyRange(parts, terminal) {
	if (!terminal) return prefixRange(parts);
	const key = encodeKey(parts);
	return [key, Buffer.concat([key, Buffer.from([0])])];
}

/**
 * Per type ordered in keys, the key range under `base` its values take (ordered-binary puts
 * numbers, then strings after them; ObjectIds are symbols holding their hex).
 */
const TYPE_BOUNDS = {
	number: (base, terminal) => [keyRange([...base, -Infinity], terminal)[0], keyRange([...base, Infinity], terminal)[1]],
	string: (base, terminal) => [keyRange([...base, ''], terminal)[0], Buffer.concat([encodeKey(base), Buffer.from([0, 0xff])])],
	objectId: (base, terminal, pack) => [
		keyRange([...base, keyElement(new ObjectId('0'.repeat(24)), pack)], terminal)[0],
		keyRange([...base, keyElement(new ObjectId('f'.repeat(24)), pack)], terminal)[1],
	],
};

module.exports = { MostikClient, Db, Collection, FindCursor, AggregationCursor, ObjectId, MongoServerError, MongoBulkWriteError };
