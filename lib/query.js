'use strict';
// Equality queries with MongoDB semantics: dot paths, arrays match any element, `null`
// matches a missing field, embedded documents compare field by field in order.
const { ObjectId } = require('./object-id');

const hasOwn = Object.hasOwn;

function isPrimitive(value) {
	return value === null || typeof value == 'string' || typeof value == 'number' || typeof value == 'boolean';
}

function isPlainObject(value) {
	if (value === null || typeof value != 'object' || Array.isArray(value)) return false;
	const proto = Object.getPrototypeOf(value);
	return proto === Object.prototype || proto === null;
}

/** Same structure with -0 turned into 0, so equal values get equal bytes. */
function withoutNegativeZero(value) {
	if (Object.is(value, -0)) return 0;
	if (Array.isArray(value)) return value.map(withoutNegativeZero);
	if (isPlainObject(value)) {
		const copy = {};
		for (const key in value) if (hasOwn(value, key)) copy[key] = withoutNegativeZero(value[key]);
		return copy;
	}
	return value;
}

/**
 * The key element for a value; values equal under `equal` get the same element. Primitives
 * encode as themselves (ordered-binary tells strings, numbers, booleans and null apart by their
 * first byte). Anything else becomes a symbol, its own key type, holding a canonical form: hex
 * for ObjectId, so ids sort by creation time, msgpack bytes otherwise. The symbols are not
 * registered (`Symbol.for` would keep every one of them alive forever).
 */
function keyElement(value, encode) {
	if (Object.is(value, -0)) return 0;
	if (isPrimitive(value)) return value;
	if (value instanceof ObjectId) return Symbol('\u0001o' + value.toHexString());
	return Symbol('\u0001m' + Buffer.from(encode(withoutNegativeZero(value))).toString('base64'));
}

/**
 * What a dot path reaches: `values` found, and `missing` when some branch of the document has
 * no such field (arrays met on the way are walked element by element, as MongoDB does). An
 * array element that is not a document is no branch: [1, 2] has no missing `k`, while a
 * value that is not a document (5) or a document without the field ({ j: 1 }) is missing it.
 * A number on an array is that position (an element too), and also a field of that name in
 * its documents. `whole`: arrays the path ends at by a position, which match as a whole value
 * only. `ambiguous(field, array)`: called for an array with both a position `field` and
 * documents with a field of that name.
 */
function lookup(doc, path, ambiguous) {
	const fields = segmentsOf(path);
	// no array on the way (most paths): the value, or nothing
	let value = doc;
	let i = 0;
	for (; i < fields.length && !Array.isArray(value); i++) {
		if (!isPlainObject(value) || !hasOwn(value, fields[i])) return NOTHING;
		value = value[fields[i]];
	}
	if (i == fields.length) return value === undefined ? NOTHING : { values: [value], missing: false, whole: NO_ARRAYS };
	return lookupThroughArrays(doc, fields, ambiguous);
}

// what a path reaches in a document without it
const NO_ARRAYS = new Set();
const NOTHING = Object.freeze({ values: Object.freeze([]), missing: true, whole: NO_ARRAYS });

// paths split once
const pathSegments = new Map();
function segmentsOf(path) {
	let segments = pathSegments.get(path);
	if (!segments) {
		segments = path.split('.');
		if (pathSegments.size < 4096) pathSegments.set(path, segments);
	}
	return segments;
}

/** lookup() for a path that meets an array. */
function lookupThroughArrays(doc, fields, ambiguous) {
	// the values reached so far, and whether each is an array's element
	let current = [doc];
	let elements = [false];
	let missing = false;
	const whole = new Set();
	for (let i = 0; i < fields.length; i++) {
		const field = fields[i];
		const [next, nextElements] = [[], []];
		const position = /^\d+$/.test(field) ? Number(field) : -1;
		for (let j = 0; j < current.length; j++) {
			const value = current[j];
			if (Array.isArray(value)) {
				// a number: that position, and the field of that name in its documents
				if (position >= 0 && position < value.length) {
					if (ambiguous && value.some((item) => isPlainObject(item) && hasOwn(item, field))) ambiguous(field, value);
					const item = value[position];
					next.push(item);
					nextElements.push(true);
					if (i == fields.length - 1 && Array.isArray(item)) whole.add(item);
				}
				for (const item of value) {
					if (!isPlainObject(item)) continue;
					if (hasOwn(item, field)) {
						next.push(item[field]);
						nextElements.push(false);
					} else {
						missing = true;
					}
				}
			} else if (isPlainObject(value) && hasOwn(value, field)) {
				next.push(value[field]);
				nextElements.push(false);
			} else if (isPlainObject(value) || !elements[j]) {
				missing = true;
			}
		}
		[current, elements] = [next, nextElements];
	}
	const values = current.filter((value) => value !== undefined);
	return { values, missing: missing || values.length < current.length, whole };
}

/** The key element of an empty array: it sorts below null, as in MongoDB. */
const EMPTY_ARRAY_ELEMENT = '\u0001e';

/**
 * Key elements a document gets in the index on `path`: one per array element (an empty array
 * gets one of its own), null when missing. `ambiguous`: as for lookup.
 */
function indexElements(doc, path, encode, ambiguous) {
	const { values, missing } = lookup(doc, path, ambiguous);
	const primitives = new Set();
	const symbols = new Map();
	if (missing || values.length == 0) primitives.add(null);
	for (const value of values) {
		if (Array.isArray(value) && value.length == 0) symbols.set(EMPTY_ARRAY_ELEMENT, Symbol(EMPTY_ARRAY_ELEMENT));
		for (const item of Array.isArray(value) ? value : [value]) {
			const element = keyElement(item, encode);
			if (typeof element == 'symbol') symbols.set(element.description, element);
			else primitives.add(element);
		}
	}
	return [...primitives, ...symbols.values()];
}

/** MongoDB-style equality: numeric (-0 == 0, NaN == NaN), typed, field order significant. */
function equal(a, b) {
	if (a === b) return true;
	if (typeof a == 'number' && typeof b == 'number') return a !== a && b !== b;
	if (a === null || b === null || typeof a != 'object' || typeof b != 'object') return false;
	if (a instanceof ObjectId || b instanceof ObjectId) return a instanceof ObjectId && a.equals(b);
	if (a instanceof Date || b instanceof Date) return a instanceof Date && b instanceof Date && Object.is(a.getTime(), b.getTime());
	if (a instanceof Uint8Array || b instanceof Uint8Array)
		return a instanceof Uint8Array && b instanceof Uint8Array && Buffer.compare(a, b) == 0;
	if (Array.isArray(a) || Array.isArray(b)) {
		if (!Array.isArray(a) || !Array.isArray(b) || a.length != b.length) return false;
		for (let i = 0; i < a.length; i++) if (!equal(a[i], b[i])) return false;
		return true;
	}
	if (!isPlainObject(a) || !isPlainObject(b)) return false;
	const keysA = Object.keys(a);
	const keysB = Object.keys(b);
	if (keysA.length != keysB.length) return false;
	for (let i = 0; i < keysA.length; i++) {
		if (keysA[i] !== keysB[i] || !equal(a[keysA[i]], b[keysB[i]])) return false;
	}
	return true;
}

function fieldMatches(doc, path, expected) {
	if (isOperators(expected)) {
		for (const op in expected) if (!operatorMatches(doc, path, op, expected[op], expected)) return false;
		return true;
	}
	if (expected instanceof RegExp) return anyValue(doc, path, (value) => regexMatches(value, expected));
	return equalMatches(doc, path, expected);
}

/** Equality to `expected` taken as a value (an object with $ keys too, as $eq does). */
function equalMatches(doc, path, expected) {
	const { values, missing, whole } = lookup(doc, path);
	if (expected === null && missing) return true;
	return values.some((value) => equal(value, expected) || (Array.isArray(value) && !whole.has(value) && value.some((item) => equal(item, expected))));
}

/** Whether some value at `path` (an array's elements one by one) passes `test`. */
function anyValue(doc, path, test) {
	const { values, whole } = lookup(doc, path);
	return values.some((value) => test(value) || (Array.isArray(value) && !whole.has(value) && value.some(test)));
}

// the RegExp of each regular expression condition, made once
const regexes = new WeakMap();

/**
 * The RegExp a condition stands for: a regular expression, or `{ $regex, $options }` (a
 * string or a regular expression, with its flags or `$options`). MongoDB's options i, m, s
 * are JavaScript's flags; g and y, which make a RegExp keep state between tests, are left out.
 */
function regexOf(condition) {
	let re = regexes.get(condition);
	if (re) return re;
	let source, flags;
	if (condition instanceof RegExp) [source, flags] = [condition.source, condition.flags];
	else if (condition.$regex instanceof RegExp) [source, flags] = [condition.$regex.source, condition.$options ?? condition.$regex.flags];
	else [source, flags] = [condition.$regex, condition.$options ?? ''];
	if ([...flags].some((flag) => !'imsguy'.includes(flag))) {
		const option = [...flags].find((flag) => !'imsguy'.includes(flag));
		throw new Error(option == 'x' ? 'mostik: regular expression option x is not supported' : `invalid flag in regex options: ${option}`);
	}
	try {
		re = new RegExp(source, [...new Set(flags)].filter((flag) => 'imsu'.includes(flag)).join(''));
	} catch (error) {
		throw new Error(`Regular expression is invalid: ${error.message}`);
	}
	regexes.set(condition, re);
	return re;
}

/** A regular expression matches strings only. */
function regexMatches(value, condition) {
	return typeof value == 'string' && regexOf(condition).test(value);
}

/**
 * The literal start every string a regular expression matches has, with `whole` when the
 * expression is that start alone (`^abc`): its matches are the strings that start with it.
 * null when there is none: no ^ anchor, case-insensitive, ^ at line starts (m), alternatives.
 */
function regexPrefix(condition) {
	const re = regexOf(condition);
	if (re.flags.includes('i') || re.flags.includes('m')) return null;
	const source = re.source;
	if (!source.startsWith('^') || source.includes('|')) return null;
	let prefix = '';
	let i = 1;
	while (i < source.length) {
		const c = source[i];
		let literal;
		let size = 1;
		if (c == '\\') {
			const next = source[i + 1];
			// an escaped punctuation character is itself; \d, \w, \b... are classes or anchors
			if (next === undefined || /[0-9A-Za-z]/.test(next)) break;
			[literal, size] = [next, 2];
		} else if ('.^$*+?()[]{}|'.includes(c)) {
			break;
		} else {
			const point = source.codePointAt(i);
			literal = String.fromCodePoint(point);
			size = literal.length;
		}
		// a quantifier after it: the character may be missing or repeated
		const after = source[i + size];
		if (after == '*' || after == '?' || after == '{') break;
		prefix += literal;
		i += size;
		if (after == '+') {
			// one at least: kept, and what follows is not literal
			i = -1;
			break;
		}
	}
	// the key range of a prefix holds its strings when the prefix starts at a printable character
	if (prefix.length == 0 || prefix.codePointAt(0) < 0x20) return null;
	return { prefix, whole: i == source.length };
}

/** Whether `element` (one value, arrays not walked) passes the operator `op` of `spec`. */
function elementMatches(element, op, operand, spec) {
	switch (op) {
		case '$regex':
			return regexMatches(element, spec);
		case '$options':
			return true;
		case '$in':
			return operand.some((value) => (value instanceof RegExp ? regexMatches(element, value) : equal(element, value)));
		case '$nin':
			return !operand.some((value) => (value instanceof RegExp ? regexMatches(element, value) : equal(element, value)));
		case '$ne':
			return !equal(element, operand);
		case '$eq':
			return !(operand instanceof RegExp) && equal(element, operand);
		case '$exists':
			return Boolean(operand);
		case '$elemMatch':
			return elementsMatch(element, operand);
		case '$all':
			return operand.length > 0 && operand.every((value) => (value instanceof RegExp ? regexMatches(element, value) : equal(element, value) || (Array.isArray(element) && element.some((item) => equal(item, value)))));
		case '$size':
			return Array.isArray(element) && element.length === operand;
		case '$type':
			return typeCodes(operand).has(typeCode(element));
		case '$not':
			if (operand instanceof RegExp) return !regexMatches(element, operand);
			for (const inner in operand) if (!elementMatches(element, inner, operand[inner], operand)) return true;
			return false;
	}
	if (operand === null) return (op == '$gte' || op == '$lte') && element === null;
	return compared(element, op, operand);
}

/** Whether `value` is an array with an element that `spec` ($elemMatch's operand) accepts. */
function elementsMatch(value, spec) {
	if (!Array.isArray(value)) return false;
	if (isElementOperators(spec)) {
		return value.some((element) => {
			for (const op in spec) if (!elementMatches(element, op, spec[op], spec)) return false;
			return true;
		});
	}
	// on fields: elements that are documents only
	return value.some((element) => isPlainObject(element) && matches(element, spec));
}

/** $elemMatch on values ({ $gt: 1 }) rather than on the fields of documents ({ a: 1 }). */
function isElementOperators(spec) {
	for (const key in spec) return key.startsWith('$') && !LOGICAL.has(key);
	return false;
}

/** Whether the comparison `op` with `operand` holds for `value` (of the operand's bracket). */
function compared(value, op, operand) {
	const order = compare(value, operand);
	if (order === undefined) return false;
	switch (op) {
		case '$gt':
			return order > 0;
		case '$gte':
			return order >= 0;
		case '$lt':
			return order < 0;
		default:
			return order <= 0;
	}
}

// $type: MongoDB's BSON type numbers, and their names
const TYPES = {
	double: 1, string: 2, object: 3, array: 4, binData: 5, undefined: 6, objectId: 7, bool: 8, date: 9, null: 10, regex: 11,
	dbPointer: 12, javascript: 13, symbol: 14, javascriptWithScope: 15, int: 16, timestamp: 17, long: 18, decimal: 19, minKey: -1, maxKey: 127,
};
const TYPE_NUMBERS = new Set(Object.values(TYPES));

/**
 * The $type code of a value as stored: integers of 32 bits are int, other numbers double,
 * BigInt long (64-bit integers read back as BigInt); undefined for values of no BSON type.
 */
function typeCode(value) {
	switch (typeof value) {
		case 'number':
			return Number.isInteger(value) && value >= -0x80000000 && value <= 0x7fffffff ? TYPES.int : TYPES.double;
		case 'bigint':
			return TYPES.long;
		case 'string':
			return TYPES.string;
		case 'boolean':
			return TYPES.bool;
	}
	if (value === null) return TYPES.null;
	if (Array.isArray(value)) return TYPES.array;
	if (value instanceof ObjectId) return TYPES.objectId;
	if (value instanceof Date) return TYPES.date;
	if (value instanceof Uint8Array) return TYPES.binData;
	if (value instanceof RegExp) return TYPES.regex;
	if (isPlainObject(value)) return TYPES.object;
	return undefined;
}

// the codes of each $type operand, worked out once
const typeSets = new WeakMap();

/** The codes a $type operand names: codes or names, one or a list; "number" is all four numeric types. */
function typeCodes(operand) {
	const list = Array.isArray(operand) ? operand : [operand];
	let codes = typeSets.get(list);
	if (codes) return codes;
	codes = new Set();
	for (const type of list) {
		if (type === 'number') [TYPES.double, TYPES.int, TYPES.long, TYPES.decimal].forEach((code) => codes.add(code));
		else codes.add(typeof type == 'string' ? TYPES[type] : type);
	}
	typeSets.set(list, codes);
	return codes;
}

const COMPARISONS = new Set(['$gt', '$gte', '$lt', '$lte']);
const OPERATORS = new Set([...COMPARISONS, '$eq', '$in', '$ne', '$nin', '$exists', '$regex', '$options', '$elemMatch', '$all', '$size', '$type', '$not']);
/** Operators over whole filters, at the top level of a filter: each takes a list of filters. */
const LOGICAL = new Set(['$and', '$or', '$nor']);

/** An operator expression: a plain object whose first key is an operator ({ $gt: 5 }). */
function isOperators(value) {
	if (!isPlainObject(value)) return false;
	for (const key in value) return key.startsWith('$');
	return false;
}

/** A condition that is a value to be equal to (not operators, not a regular expression). */
function isEquality(value) {
	return !isOperators(value) && !(value instanceof RegExp);
}

function operatorMatches(doc, path, op, operand, spec) {
	switch (op) {
		case '$regex':
			return anyValue(doc, path, (value) => regexMatches(value, spec));
		case '$options':
			return true;
		case '$eq':
			// a regular expression here is a value to be equal to, which no stored value is
			return !(operand instanceof RegExp) && equalMatches(doc, path, operand);
		case '$elemMatch':
			return lookup(doc, path).values.some((value) => elementsMatch(value, operand));
		case '$all':
			// an empty list matches nothing
			return operand.length > 0 && operand.every((value) => fieldMatches(doc, path, value));
		case '$size':
			return lookup(doc, path).values.some((value) => Array.isArray(value) && value.length === operand);
		case '$type': {
			const types = typeCodes(operand);
			return anyValue(doc, path, (value) => types.has(typeCode(value)));
		}
		case '$not':
			return !fieldMatches(doc, path, operand);
		case '$in':
			return operand.some((value) => fieldMatches(doc, path, value));
		case '$ne':
			return !fieldMatches(doc, path, operand);
		case '$nin':
			return !operand.some((value) => fieldMatches(doc, path, value));
		case '$exists':
			// some value is there, null included
			return lookup(doc, path).values.length > 0 == Boolean(operand);
	}
	// null is only equal to itself (and to a missing field): $gte/$lte match those, $gt/$lt nothing
	if (operand === null) return (op == '$gte' || op == '$lte') && fieldMatches(doc, path, null);
	return anyValue(doc, path, (value) => compared(value, op, operand));
}

/** Kinds of values that compare with each other (MongoDB's type brackets); undefined: none. */
function bracket(value) {
	switch (typeof value) {
		case 'number':
			return 'number';
		case 'string':
			return 'string';
		case 'boolean':
			return 'boolean';
	}
	if (value instanceof ObjectId) return 'objectId';
	if (value instanceof Date) return 'date';
	return undefined;
}

/**
 * Order of two values of the same bracket as MongoDB sees it (strings by code point, as UTF-8
 * bytes compare); undefined when they do not compare (other brackets; NaN against a number).
 */
function compare(a, b) {
	const kind = bracket(b);
	if (kind === undefined || bracket(a) !== kind) return undefined;
	switch (kind) {
		case 'number':
			if (a !== a || b !== b) return a !== a && b !== b ? 0 : undefined;
			return a < b ? -1 : a > b ? 1 : 0;
		case 'string':
			return compareStrings(a, b);
		case 'boolean':
			return Number(a) - Number(b);
		case 'objectId':
			return Buffer.compare(Buffer.from(a.id), Buffer.from(b.id));
		default: {
			const [x, y] = [a.getTime(), b.getTime()];
			if (x !== x || y !== y) return x !== x && y !== y ? 0 : undefined;
			return x < y ? -1 : x > y ? 1 : 0;
		}
	}
}

/** Code point order (UTF-16 code units differ from it only around surrogates). */
function compareStrings(a, b) {
	if (a === b) return 0;
	const n = Math.min(a.length, b.length);
	let i = 0;
	while (i < n && a.charCodeAt(i) === b.charCodeAt(i)) i++;
	if (i == n) return a.length < b.length ? -1 : 1;
	// back up to the start of a surrogate pair split at the difference
	if (i > 0 && (a.charCodeAt(i - 1) & 0xfc00) === 0xd800) i--;
	return a.codePointAt(i) < b.codePointAt(i) ? -1 : 1;
}

// MongoDB's order of types in comparisons and sorts; an empty array sorts below null
const RANK = { emptyArray: 0, null: 1, number: 2, string: 3, object: 4, array: 5, binary: 6, objectId: 7, boolean: 8, date: 9, regex: 10 };

function rank(value) {
	if (value === null || value === undefined) return RANK.null;
	switch (typeof value) {
		case 'number':
			return RANK.number;
		case 'string':
			return RANK.string;
		case 'boolean':
			return RANK.boolean;
	}
	if (Array.isArray(value)) return RANK.array;
	if (value instanceof ObjectId) return RANK.objectId;
	if (value instanceof Date) return RANK.date;
	if (value instanceof Uint8Array) return RANK.binary;
	if (value instanceof RegExp) return RANK.regex;
	return RANK.object;
}

/** Total order of any two values, as MongoDB sorts them (BSON comparison, simple collation). */
function compareValues(a, b) {
	const [ra, rb] = [rank(a), rank(b)];
	if (ra != rb) return ra - rb;
	switch (ra) {
		case RANK.null:
			return 0;
		case RANK.number:
			// NaN below every other number
			if (a !== a || b !== b) return (a === a) - (b === b);
			return a < b ? -1 : a > b ? 1 : 0;
		case RANK.string:
			return compareStrings(a, b);
		case RANK.boolean:
			return Number(a) - Number(b);
		case RANK.objectId:
			return Buffer.compare(Buffer.from(a.id), Buffer.from(b.id));
		case RANK.date: {
			const [x, y] = [a.getTime(), b.getTime()];
			if (x !== x || y !== y) return (x === x) - (y === y);
			return x < y ? -1 : x > y ? 1 : 0;
		}
		case RANK.binary:
			return a.length - b.length || Buffer.compare(Buffer.from(a), Buffer.from(b));
		case RANK.regex:
			return compareStrings(a.source, b.source) || compareStrings(a.flags, b.flags);
		case RANK.array: {
			for (let i = 0; i < Math.min(a.length, b.length); i++) {
				const order = compareValues(a[i], b[i]);
				if (order) return order;
			}
			return a.length - b.length;
		}
		default: {
			// documents: field by field, each by its value's type, then name, then value
			const [ka, kb] = [Object.keys(a), Object.keys(b)];
			for (let i = 0; i < Math.min(ka.length, kb.length); i++) {
				const order = rank(a[ka[i]]) - rank(b[kb[i]]) || compareStrings(ka[i], kb[i]) || compareValues(a[ka[i]], b[kb[i]]);
				if (order) return order;
			}
			return ka.length - kb.length;
		}
	}
}

const EMPTY_ARRAY_KEY = Symbol('empty array');

/**
 * The value a document sorts by on `path`: of the values there (array elements taken one by
 * one), the smallest when ascending, the largest when descending; null when missing.
 */
function sortKey(doc, path, descending) {
	const { values } = lookup(doc, path);
	let best;
	let found = false;
	const consider = (value) => {
		if (!found || (descending ? compareSortKeys(value, best) > 0 : compareSortKeys(value, best) < 0)) best = value;
		found = true;
	};
	for (const value of values) {
		if (!Array.isArray(value)) consider(value);
		else if (value.length == 0) consider(EMPTY_ARRAY_KEY);
		else for (const item of value) consider(item);
	}
	return found ? best : null;
}

/**
 * The keys `doc` sorts by on `sort` ([[path, direction]]); `parallel()` is called when two of
 * the paths go through different arrays (MongoDB cannot tell which elements go together).
 */
function sortKeys(doc, sort, parallel) {
	if (sort.length > 1) {
		let arrayAt = null;
		for (const [path] of sort) {
			const at = firstArray(doc, path);
			if (at === null) continue;
			if (arrayAt !== null && at !== arrayAt) parallel();
			arrayAt = at;
		}
	}
	return sort.map(([path, direction]) => sortKey(doc, path, direction < 0));
}

/** The prefix of `path` whose value in `doc` is the first array on the way to it, or null. */
function firstArray(doc, path) {
	let value = doc;
	let prefix = '';
	for (const segment of path.split('.')) {
		if (!isPlainObject(value) || !hasOwn(value, segment)) return null;
		value = value[segment];
		prefix = prefix ? `${prefix}.${segment}` : segment;
		if (Array.isArray(value)) return prefix;
	}
	return null;
}

function compareSortKeys(a, b) {
	if (a === EMPTY_ARRAY_KEY || b === EMPTY_ARRAY_KEY) return (a !== EMPTY_ARRAY_KEY) - (b !== EMPTY_ARRAY_KEY);
	return compareValues(a, b);
}

function matches(doc, filter) {
	for (const path in filter) {
		const condition = filter[path];
		if (path == '$and') {
			if (!condition.every((part) => matches(doc, part))) return false;
		} else if (path == '$or') {
			if (!condition.some((part) => matches(doc, part))) return false;
		} else if (path == '$nor') {
			if (condition.some((part) => matches(doc, part))) return false;
		} else if (!fieldMatches(doc, path, condition)) {
			return false;
		}
	}
	return true;
}

/** Every field path a filter looks at, those of its $and / $or parts included. */
function filterPaths(filter) {
	const paths = [];
	for (const path in filter) {
		if (LOGICAL.has(path)) for (const part of filter[path]) paths.push(...filterPaths(part));
		else paths.push(path);
	}
	return paths;
}

// operation codes of the native filter
const NATIVE_OPS = { $eq: 0, $gt: 1, $gte: 2, $lt: 3, $lte: 4, $in: 5, $ne: 6, $exists: 8 };
// a list of alternatives, each a list of conditions
const NATIVE_OR = 7;
// an array with an element that meets a list of conditions: on the element itself (form 0),
// or on its fields (form 1)
const NATIVE_ELEM_MATCH = 9;
// the list of conditions after it does not hold
const NATIVE_NOT = 10;
const NATIVE_SIZE = 11;
const NATIVE_TYPE = 12;
// the $type codes the native layer tells apart (lib/query.js typeCode on stored values)
const NATIVE_TYPES = new Set([1, 2, 3, 4, 5, 7, 8, 9, 10, 16, 18]);

/** Values the native layer compares by their bytes: they encode one way. */
function exactValue(value) {
	return typeof value == 'string' || typeof value == 'boolean' || (typeof value == 'number' && Number.isFinite(value));
}

/**
 * The filter for the native layer (crates/node/src/filter.rs), which evaluates it on document
 * bytes so documents that cannot match are never decoded. It covers equality, $in, $ne, $nin
 * with values that encode one way (strings, booleans, finite numbers), comparisons with a
 * number or string, $exists, $elemMatch of those, and $and / $or of those; `exact` tells
 * whether it covers every condition, in which case its answer is final. Conditions it does not
 * cover are left out: it may let documents through, never drop one. undefined when it covers
 * none. `structures`: the shared record structures documents refer to
 * (`Storage.structureTable`).
 */
function nativeFilter(filter, encode, structures) {
	const u32 = (n) => {
		const b = Buffer.allocUnsafe(4);
		b.writeUInt32LE(n, 0);
		return b;
	};
	let exact = true;
	const list = () => ({ count: 0, chunks: [] });
	const pathOf = (out, segments) => {
		out.chunks.push(u32(segments.length));
		for (const segment of segments) {
			const bytes = Buffer.from(segment, 'utf8');
			out.chunks.push(u32(bytes.length), bytes);
		}
	};
	const add = (out, segments, op, values) => {
		out.count++;
		out.chunks.push(Buffer.from([typeof op == 'number' ? op : NATIVE_OPS[op]]));
		pathOf(out, segments);
		out.chunks.push(u32(values.length));
		for (const value of values) {
			const bytes = Buffer.from(encode(value));
			out.chunks.push(u32(bytes.length), bytes);
		}
	};
	const nested = (out, inner) => out.chunks.push(u32(inner.count), ...inner.chunks);
	const elemMatch = (out, segments, spec) => {
		out.count++;
		out.chunks.push(Buffer.from([NATIVE_ELEM_MATCH]));
		pathOf(out, segments);
		const inner = list();
		if (isElementOperators(spec)) {
			out.chunks.push(Buffer.from([0]));
			operators(inner, [], spec);
		} else {
			out.chunks.push(Buffer.from([1]));
			conditions(inner, spec);
		}
		nested(out, inner);
	};
	// a list that does not hold: sent only when it covers every one of its conditions (one left
	// out would let the negation drop documents that match)
	const not = (out, build) => {
		const outer = exact;
		exact = true;
		const inner = list();
		build(inner);
		const whole = exact && inner.count > 0;
		exact = outer && whole;
		if (!whole) return;
		out.count++;
		out.chunks.push(Buffer.from([NATIVE_NOT]));
		nested(out, inner);
	};
	// the operators of one condition, on the value at `segments` (none: the element itself)
	const operators = (out, segments, value) => {
		for (const op in value) {
			const operand = value[op];
			if (op == '$exists') add(out, segments, op, [Boolean(operand)]);
			else if (op == '$in' && operand.every(exactValue)) add(out, segments, op, operand);
			else if (op == '$nin' && operand.every(exactValue)) add(out, segments, '$ne', operand);
			else if (op == '$ne' && exactValue(operand)) add(out, segments, op, [operand]);
			else if (op == '$eq' && exactValue(operand)) add(out, segments, '$eq', [operand]);
			else if (COMPARISONS.has(op) && exactValue(operand) && typeof operand != 'boolean') add(out, segments, op, [operand]);
			else if (op == '$elemMatch') elemMatch(out, segments, operand);
			else if (op == '$all' && operand.length > 0 && operand.every(exactValue)) for (const item of operand) add(out, segments, '$eq', [item]);
			else if (op == '$size') add(out, segments, NATIVE_SIZE, [operand]);
			// the other types (regex, decimal...) are no stored value's
			else if (op == '$type') add(out, segments, NATIVE_TYPE, [...typeCodes(operand)].filter((code) => NATIVE_TYPES.has(code)));
			else if (op == '$not' && isOperators(operand)) not(out, (inner) => operators(inner, segments, operand));
			else exact = false;
		}
	};
	// the conditions of `filter` as native records: `[count u32]` then each condition
	const conditions = (out, filter) => {
		for (const path in filter) {
			const value = filter[path];
			if (path == '$and') {
				// all of them: their conditions join these
				for (const part of value) conditions(out, part);
				continue;
			}
			if (path == '$nor') {
				not(out, (inner) => conditions(inner, { $or: value }));
				continue;
			}
			if (path == '$or') {
				out.count++;
				out.chunks.push(Buffer.from([NATIVE_OR]), u32(value.length));
				for (const part of value) {
					const inner = list();
					conditions(inner, part);
					nested(out, inner);
				}
				continue;
			}
			if (!isOperators(value)) {
				if (exactValue(value)) add(out, path.split('.'), '$eq', [value]);
				else exact = false;
				continue;
			}
			operators(out, path.split('.'), value);
		}
	};
	const top = list();
	conditions(top, filter);
	if (top.count == 0) return { bytes: undefined, exact };
	return { bytes: Buffer.concat([u32(top.count), ...top.chunks, structures ?? u32(0)]), exact };
}

/**
 * The projection for the native layer (crates/node/src/filter.rs): documents come with only
 * the top-level `fields`.
 */
function nativeProjection(fields, structures) {
	const u32 = (n) => {
		const b = Buffer.allocUnsafe(4);
		b.writeUInt32LE(n, 0);
		return b;
	};
	const chunks = [u32(fields.length)];
	for (const field of fields) {
		const bytes = Buffer.from(field, 'utf8');
		chunks.push(u32(bytes.length), bytes);
	}
	return Buffer.concat([...chunks, structures]);
}

/** Rejects what this engine cannot answer, instead of silently mis-answering. */
function checkFilter(filter) {
	if (!isPlainObject(filter)) throw new TypeError('The filter must be an object');
	for (const path in filter) {
		const value = filter[path];
		if (LOGICAL.has(path)) {
			if (!Array.isArray(value) || value.length == 0) throw new TypeError(`${path} must be a non-empty array`);
			for (const part of value) {
				if (!isPlainObject(part)) throw new TypeError('$or/$and/$nor entries need to be full objects');
				checkFilter(part);
			}
			continue;
		}
		if (path.startsWith('$')) throw new Error(`mostik: query operator ${path} is not supported`);
		checkCondition(path, value);
	}
}

/** Checks the condition on one field: a value, a regular expression, or operators. */
function checkCondition(path, value) {
	if (value === undefined) throw new Error(`mostik: filter value for "${path}" is undefined`);
	if (value instanceof RegExp) return void regexOf(value);
	if (!isOperators(value)) return;
	for (const op in value) {
		if (!op.startsWith('$')) throw new Error(`mostik: unknown operator: ${op} (in "${path}")`);
		if (!OPERATORS.has(op)) throw new Error(`mostik: query operator ${op} is not supported`);
		const operand = value[op];
		if (operand === undefined) throw new Error(`mostik: operand of ${op} for "${path}" is undefined`);
		if (op == '$in' || op == '$nin') {
			if (!Array.isArray(operand)) throw new TypeError(`${op} needs an array ("${path}")`);
			if (operand.some((item) => isOperators(item))) throw new Error(`mostik: ${op} accepts values only ("${path}")`);
			for (const item of operand) if (item instanceof RegExp) regexOf(item);
		} else if (op == '$exists' || op == '$eq') {
			// any value: its truth says which; the value to be equal to, as it is
		} else if (op == '$regex') {
			if (typeof operand != 'string' && !(operand instanceof RegExp)) throw new TypeError(`$regex has to be a string ("${path}")`);
			if (operand instanceof RegExp && operand.flags.replace(/[gy]/g, '') && '$options' in value)
				throw new Error(`options set in both $regex and $options ("${path}")`);
			regexOf(value);
		} else if (op == '$options') {
			if (!('$regex' in value)) throw new Error(`$options needs a $regex ("${path}")`);
			if (typeof operand != 'string') throw new TypeError(`$options has to be a string ("${path}")`);
		} else if (op == '$elemMatch') {
			if (!isPlainObject(operand)) throw new TypeError(`$elemMatch needs an Object ("${path}")`);
			if (isElementOperators(operand)) checkCondition(path, operand);
			else checkFilter(operand);
		} else if (op == '$all') {
			if (!Array.isArray(operand)) throw new TypeError(`$all needs an array ("${path}")`);
			const elemMatches = operand.filter((item) => isOperators(item));
			if (elemMatches.some((item) => Object.keys(item)[0] != '$elemMatch')) throw new Error(`no $ expressions in $all ("${path}")`);
			if (elemMatches.length > 0 && elemMatches.length < operand.length) throw new Error(`$all/$elemMatch has to be consistent ("${path}")`);
			for (const item of operand) {
				if (item instanceof RegExp) regexOf(item);
				else if (isOperators(item)) checkCondition(path, item);
			}
		} else if (op == '$size') {
			if (typeof operand != 'number') throw new TypeError(`$size needs a number ("${path}")`);
			if (!Number.isInteger(operand)) throw new Error(`$size must be a whole number ("${path}")`);
			if (operand < 0) throw new Error(`$size may not be negative ("${path}")`);
		} else if (op == '$type') {
			const list = Array.isArray(operand) ? operand : [operand];
			if (list.length == 0) throw new Error(`$type must match at least one type ("${path}")`);
			for (const type of list) {
				if (typeof type == 'string') {
					if (type != 'number' && !Object.hasOwn(TYPES, type)) throw new Error(`Unknown type name alias: ${type}`);
				} else if (typeof type != 'number' || !TYPE_NUMBERS.has(type)) {
					throw new Error(`Invalid numerical type code: ${type}`);
				}
			}
		} else if (op == '$not') {
			if (operand instanceof RegExp) regexOf(operand);
			else if (!isPlainObject(operand)) throw new TypeError(`$not needs a regex or a document ("${path}")`);
			else if (Object.keys(operand).length == 0) throw new Error(`$not cannot be empty ("${path}")`);
			else if (!isOperators(operand)) throw new Error(`mostik: unknown operator: ${Object.keys(operand)[0]} (in "${path}")`);
			else checkCondition(path, operand);
		} else if (operand instanceof RegExp || isOperators(operand)) {
			throw new Error(`mostik: ${op} accepts a value only ("${path}")`);
		} else if (COMPARISONS.has(op) && operand !== null && bracket(operand) === undefined) {
			throw new Error(`mostik: ${op} compares numbers, strings, booleans, dates and ObjectIds ("${path}")`);
		}
	}
}

module.exports = { keyElement, indexElements, matches, equal, compare, isOperators, isEquality, isElementOperators, regexPrefix, checkFilter, nativeFilter, isPrimitive, compareValues, sortKey, sortKeys, compareSortKeys, isPlainObject, EMPTY_ARRAY_ELEMENT, EMPTY_ARRAY_KEY, rank, RANK, nativeProjection, filterPaths, LOGICAL };
