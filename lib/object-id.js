'use strict';
const crypto = require('crypto');
const { inspect } = require('util');

// 12 bytes, as in MongoDB: 4-byte timestamp (seconds), 5 random bytes per process, 3-byte counter.
const processUnique = crypto.randomBytes(5);
let counter = crypto.randomBytes(3).readUIntBE(0, 3);

const HEX = /^[0-9a-fA-F]{24}$/;

class ObjectId {
	constructor(id) {
		if (id === undefined || id === null) {
			const bytes = Buffer.allocUnsafe(12);
			bytes.writeUInt32BE(Math.floor(Date.now() / 1000), 0);
			processUnique.copy(bytes, 4);
			counter = (counter + 1) & 0xffffff;
			bytes.writeUIntBE(counter, 9, 3);
			this.id = bytes;
		} else if (id instanceof ObjectId) {
			this.id = Buffer.from(id.id);
		} else if (typeof id == 'string' && HEX.test(id)) {
			this.id = Buffer.from(id, 'hex');
		} else if (id instanceof Uint8Array && id.length == 12) {
			this.id = Buffer.from(id);
		} else {
			throw new TypeError('Argument passed in must be a string of 24 hex characters or a 12 byte Uint8Array');
		}
	}

	static isValid(id) {
		try {
			new ObjectId(id);
			return id !== undefined && id !== null;
		} catch {
			return false;
		}
	}

	static createFromHexString(hex) {
		if (typeof hex != 'string' || !HEX.test(hex)) throw new TypeError('Argument passed in must be a string of 24 hex characters');
		return new ObjectId(hex);
	}

	get _bsontype() {
		return 'ObjectId';
	}

	toHexString() {
		return this.id.toString('hex');
	}

	toString() {
		return this.toHexString();
	}

	toJSON() {
		return this.toHexString();
	}

	getTimestamp() {
		return new Date(this.id.readUInt32BE(0) * 1000);
	}

	equals(other) {
		if (other instanceof ObjectId) return this.id.equals(other.id);
		return typeof other == 'string' && HEX.test(other) && other.toLowerCase() == this.toHexString();
	}

	[inspect.custom]() {
		return `new ObjectId('${this.toHexString()}')`;
	}
}

module.exports = { ObjectId };
