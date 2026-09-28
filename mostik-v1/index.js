'use strict';
const fs = require('fs');
const os = require('os');
const pathModule = require('path');
const { Encoder } = require('msgpackr');
const { writeKey } = require('ordered-binary');
const { NativeEnv } = require('./mostik.node');

const MAX_KEY_SIZE = 1978;
const DEFAULT_COMPRESSION_THRESHOLD = 1000;
const OP_PUT = 1;
const OP_REMOVE = 2;

// same buffer sizes as lmdb-js, so oversized keys fail the same way
const readKeyBytes = Buffer.allocUnsafeSlow(4096);
const writeKeyBytes = Buffer.allocUnsafeSlow(8192);
// get() copies values here instead of allocating a buffer per read; grows for large values
let valueBytes = Buffer.allocUnsafeSlow(0x10000);

function tooLarge() {
	return new Error('Key size is larger than the maximum key size (' + MAX_KEY_SIZE + ')');
}

function encodeWriteKey(key) {
	let size;
	try {
		size = writeKey(key, writeKeyBytes, 0);
	} catch (error) {
		throw error.name == 'RangeError' ? tooLarge() : error;
	}
	if (!(size > 0)) throw new Error('Invalid key or zero length key is not allowed in LMDB ' + key);
	if (size > MAX_KEY_SIZE) throw tooLarge();
	return Buffer.from(writeKeyBytes.subarray(0, size));
}

function compressionThreshold(compression) {
	if (!compression) return 0;
	if (typeof compression == 'object' && compression.threshold > 0) return compression.threshold;
	return DEFAULT_COMPRESSION_THRESHOLD;
}

class Database {
	constructor(native, options) {
		this.native = native;
		this.path = options.path;
		this.compression = options.compression;
		this.encoder = new Encoder({ copyBuffers: true });
		this._threshold = compressionThreshold(options.compression);
		this._deleteOnClose = options.deleteOnClose;
		this._batch = null;
		this._tail = Promise.resolve();
		this._closed = false;
	}

	get(key) {
		if (this._closed) throw new Error('Can not renew a transaction from a closed database');
		const size = writeKey(key, readKeyBytes, 0);
		if (!(size > 0))
			throw new Error(
				key === undefined ? 'A key is required for get, but is undefined' : 'Zero length key is not allowed in LMDB',
			);
		if (size > MAX_KEY_SIZE) return undefined;
		let length = this.native.getInto(readKeyBytes, size, valueBytes);
		if (length > valueBytes.length) {
			valueBytes = Buffer.allocUnsafeSlow(Math.max(length, valueBytes.length * 2));
			length = this.native.getInto(readKeyBytes, size, valueBytes);
		}
		if (length < 0) return undefined;
		// the value could have grown between the two reads
		if (length > valueBytes.length) return this.get(key);
		return this.encoder.decode(valueBytes, length);
	}

	put(key, value) {
		if (this._closed) throw new Error('Database is closed');
		const keyBytes = encodeWriteKey(key);
		const valueBytes = this.encoder.encode(value);
		const header = Buffer.allocUnsafe(9);
		header[0] = OP_PUT;
		header.writeUInt32LE(keyBytes.length, 1);
		const valueHeader = Buffer.allocUnsafe(4);
		valueHeader.writeUInt32LE(valueBytes.length, 0);
		return this._enqueue(header.subarray(0, 5), keyBytes, valueHeader, Buffer.from(valueBytes));
	}

	remove(key) {
		if (this._closed) throw new Error('Database is closed');
		const keyBytes = encodeWriteKey(key);
		const header = Buffer.allocUnsafe(5);
		header[0] = OP_REMOVE;
		header.writeUInt32LE(keyBytes.length, 1);
		return this._enqueue(header, keyBytes);
	}

	// Writes from the same event turn share one transaction and one promise, like lmdb-js.
	_enqueue(...chunks) {
		let batch = this._batch;
		if (!batch) {
			batch = this._batch = { chunks: [] };
			batch.promise = new Promise((resolve, reject) => {
				setImmediate(() => {
					this._batch = null;
					const buffer = Buffer.concat(batch.chunks);
					this._tail = this._tail
						.then(() => this.native.commit(buffer, this._threshold))
						.then(() => resolve(true), reject);
				});
			});
		}
		batch.chunks.push(...chunks);
		return batch.promise;
	}

	async close() {
		if (this._closed) return;
		// writes queued while we wait are flushed too
		let tail;
		do {
			if (this._batch) await this._batch.promise.catch(() => {});
			tail = this._tail;
			await tail;
		} while (this._batch || tail !== this._tail);
		this._closed = true;
		this.native.close();
		if (this._deleteOnClose) {
			fs.rmSync(this.path, { force: true });
			fs.rmSync(this.path + '-lock', { force: true });
		}
	}
}

function open(path, options) {
	if (typeof path == 'object' && path !== null && !options) {
		options = path;
		path = options.path;
	}
	options = Object.assign({}, options);
	if (path == null) {
		path = os.tmpdir() + '/' + Math.floor(Math.random() * 2821109907455).toString(36) + '.mostik';
		options.deleteOnClose = true;
	}
	if (options.encoding && options.encoding != 'msgpack')
		throw new Error('mostik: only the msgpack encoding is supported');
	if (options.keyEncoding && options.keyEncoding != 'ordered-binary')
		throw new Error('mostik: only the ordered-binary key encoding is supported');
	options.path = path;
	// like lmdb-js: a path with an extension is the data file, otherwise a directory
	let dataPath, lockPath;
	if (pathModule.extname(path)) {
		fs.mkdirSync(pathModule.dirname(path), { recursive: true });
		dataPath = path;
		lockPath = path + '-lock';
	} else {
		fs.mkdirSync(path, { recursive: true });
		dataPath = pathModule.join(path, 'data.mostik');
		lockPath = pathModule.join(path, 'lock.mostik');
	}
	return new Database(NativeEnv.open(dataPath, lockPath), options);
}

module.exports = { open, Database };
