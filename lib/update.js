'use strict';
// Update operators with MongoDB semantics on dot paths: $set, $unset, $inc, $min, $max,
// $rename, $setOnInsert, and on arrays $push, $addToSet, $pull.

const { compareValues, matches, isOperators: isQueryOperators, sortKey, compareSortKeys } = require('./query');

const hasOwn = Object.hasOwn;

const OPERATORS = new Set(['$set', '$unset', '$inc', '$min', '$max', '$rename', '$setOnInsert', '$push', '$addToSet', '$pull']);
// operators that create the fields they write (and the documents on the way)
const CREATING = new Set(['$set', '$inc', '$min', '$max', '$setOnInsert', '$push', '$addToSet']);
const PUSH_MODIFIERS = new Set(['$each', '$position', '$slice', '$sort']);

/** An error MongoDB would report with this code; `make` builds it (MongoServerError). */
function failure(make, message, code) {
	return make(message, { code });
}

function isPlainObject(value) {
	if (value === null || typeof value != 'object' || Array.isArray(value)) return false;
	const proto = Object.getPrototypeOf(value);
	return proto === Object.prototype || proto === null;
}

function show(value) {
	try {
		return JSON.stringify(value) ?? String(value);
	} catch {
		return String(value);
	}
}

function typeName(value) {
	if (value === null) return 'null';
	if (Array.isArray(value)) return 'array';
	if (isPlainObject(value)) return 'object';
	return typeof value == 'number' ? (Number.isInteger(value) ? 'int' : 'double') : typeof value;
}

function checkPath(path, make) {
	const segments = path.split('.');
	if (segments.some((s) => s.length == 0)) throw failure(make, `The update path '${path}' contains an empty field name, which is not allowed.`, 56);
	if (segments.some((s) => s.startsWith('$'))) throw new Error(`mostik: positional update paths are not supported ('${path}')`);
	return segments;
}

/** $push / $addToSet: the values to add, with $push's modifiers. */
function arrayChange(op, path, value, make) {
	if (!isPlainObject(value) || !Object.keys(value).some((key) => key.startsWith('$'))) return { each: [value] };
	const keys = Object.keys(value);
	const allowed = op == '$push' ? PUSH_MODIFIERS : new Set(['$each']);
	for (const key of keys) {
		if (!allowed.has(key)) throw failure(make, `Unrecognized clause in ${op}: ${key}`, 2);
	}
	if (!Array.isArray(value.$each)) throw failure(make, `The argument to $each in ${op} must be an array but it was of type: ${typeName(value.$each)}`, 2);
	const change = { each: value.$each };
	if ('$position' in value) {
		if (!Number.isInteger(value.$position)) throw failure(make, `The value for $position must be an integer value, not of type: ${typeName(value.$position)}`, 2);
		change.position = value.$position;
	}
	if ('$slice' in value) {
		if (!Number.isInteger(value.$slice)) throw failure(make, `The value for $slice must be an integer value but was given type: ${typeName(value.$slice)}`, 2);
		change.slice = value.$slice;
	}
	if ('$sort' in value) {
		const sort = value.$sort;
		if (sort === 1 || sort === -1) change.sort = sort;
		else if (isPlainObject(sort) && Object.keys(sort).length > 0 && Object.values(sort).every((d) => d === 1 || d === -1)) change.sort = Object.entries(sort);
		else throw failure(make, 'The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields', 2);
	}
	return change;
}

/**
 * Checks an update document and returns its changes as `{ op, path, segments, value, ... }`,
 * sorted by path: MongoDB applies them in that order, so new fields come out in that order too.
 * `$rename` also has `to` / `toSegments`; `$push` / `$addToSet` have `each` and modifiers.
 */
function parseUpdate(update, make) {
	if (!isPlainObject(update)) throw new TypeError('The update must be an object with update operators');
	const operators = Object.keys(update);
	if (operators.length == 0 || operators.some((op) => !op.startsWith('$')))
		throw new TypeError('Update document requires atomic operators');
	const changes = [];
	for (const op of operators) {
		if (!OPERATORS.has(op)) throw new Error(`mostik: update operator ${op} is not supported`);
		const fields = update[op];
		if (!isPlainObject(fields)) throw failure(make, `Modifiers operate on fields but we found type ${Array.isArray(fields) ? 'array' : typeof fields} instead`, 9);
		for (const path in fields) {
			const segments = checkPath(path, make);
			const value = fields[path];
			const change = { op, path, segments, value };
			switch (op) {
				case '$inc':
					if (typeof value != 'number') throw failure(make, `Cannot increment with non-numeric argument: {${path}: ${show(value)}}`, 14);
					break;
				case '$set':
				case '$setOnInsert':
				case '$min':
				case '$max':
					if (value === undefined) throw new TypeError(`${op} value for "${path}" is undefined`);
					break;
				case '$rename':
					if (typeof value != 'string' || value.length == 0) throw failure(make, `The 'to' field for $rename must be a string: ${path}: ${show(value)}`, 2);
					if (value == path || value.startsWith(path + '.') || path.startsWith(value + '.'))
						throw failure(make, `The source and target field for $rename must not be on the same path: ${path}: ${show(value)}`, 2);
					change.to = value;
					change.toSegments = checkPath(value, make);
					break;
				case '$push':
				case '$addToSet':
					Object.assign(change, arrayChange(op, path, value, make));
					break;
				case '$pull':
					if (value === undefined) throw new TypeError(`$pull value for "${path}" is undefined`);
					break;
			}
			changes.push(change);
		}
	}
	changes.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
	// one change per path: a path and a path inside it would conflict ($rename writes two)
	const claimed = changes.flatMap((c) => (c.to ? [c.path, c.to] : [c.path])).sort();
	for (let i = 1; i < claimed.length; i++) {
		const [a, b] = [claimed[i - 1], claimed[i]];
		if (a == b || b.startsWith(a + '.')) throw failure(make, `Updating the path '${b}' would create a conflict at '${a}'`, 40);
	}
	return changes;
}

/** Array position named by a path segment, or -1. */
function position(segment) {
	return /^\d+$/.test(segment) ? Number(segment) : -1;
}

/**
 * Where `segments` lead in `doc`: `{ container, key, last, present, current, created }`.
 * `create`: make the documents on the way (an error where a value is in the way); otherwise
 * null when the path is not there, and with `viable` an error where a value is in the way (as
 * $pull and the source of $rename have). `plain`: an error when the path goes through an array
 * element.
 */
function locate(doc, segments, create, make, plain, viable = plain == 'source') {
	let container = doc;
	let created = false;
	// `container` holds the value at segments[..i], the path goes on with segments[i]
	const blocked = (i, element) =>
		failure(make, `Cannot use the part (${segments[i]}) of (${segments.join('.')}) to traverse the element ({${segments[i - 1]}: ${show(element)}})`, 28);
	// an array element on the way: an error once the path is known to lead somewhere
	let element = null;
	const through = (i) => {
		if (!plain || !Array.isArray(container)) return;
		if (viable && position(segments[i]) < 0) throw blocked(i, container);
		element ??= failure(make, `The ${plain} field cannot be an array element, '${segments.join('.')}' in doc with _id: ${show(doc._id)} has an array field called '${segments[i - 1]}'`, 2);
		if (!viable) throw element;
	};
	for (let i = 0; i < segments.length - 1; i++) {
		through(i);
		const segment = segments[i];
		let next;
		if (Array.isArray(container)) {
			const at = position(segment);
			if (at < 0) {
				if (create) throw failure(make, `Cannot create field '${segment}' in element {${segments[i - 1]}: ${show(container)}}`, 28);
				if (viable) throw blocked(i, container);
				return null;
			}
			next = container[at];
		} else {
			next = hasOwn(container, segment) ? container[segment] : undefined;
		}
		if (next === undefined) {
			if (!create) return null;
			next = {};
			setAt(container, segment, next);
			created = true;
		} else if (!Array.isArray(next) && !isPlainObject(next)) {
			if (create) throw failure(make, `Cannot create field '${segments[i + 1]}' in element {${segment}: ${show(next)}}`, 28);
			if (viable) throw blocked(i + 1, next);
			return null;
		}
		container = next;
	}
	through(segments.length - 1);
	const last = segments[segments.length - 1];
	if (Array.isArray(container) && position(last) < 0) {
		if (create) throw failure(make, `Cannot create field '${last}' in element {${segments[segments.length - 2]}: ${show(container)}}`, 28);
		if (viable) throw blocked(segments.length - 1, container);
		return null;
	}
	if (element) throw element;
	const key = Array.isArray(container) ? position(last) : last;
	const present = Array.isArray(container) ? key < container.length : hasOwn(container, key);
	return { container, key, last, present, current: present ? container[key] : undefined, created };
}

/** Whether array element `element` is one `$pull` takes: equal to a value, or matching a condition. */
function pulls(element, condition, equal) {
	if (isQueryOperators(condition)) return matches({ v: element }, { v: condition });
	if (isPlainObject(condition)) return isPlainObject(element) && matches(element, condition);
	return equal(element, condition);
}

/** $push's $sort: whole elements (1 / -1), or by fields of embedded documents. */
function sortArray(items, sort) {
	if (typeof sort == 'number') return items.sort((a, b) => sort * compareValues(a, b));
	return items.sort((a, b) => {
		for (const [path, direction] of sort) {
			const order = direction * compareSortKeys(sortKey(isPlainObject(a) ? a : {}, path, direction < 0), sortKey(isPlainObject(b) ? b : {}, path, direction < 0));
			if (order) return order;
		}
		return 0;
	});
}

/**
 * Applies `changes` to `doc` in place. Returns whether anything changed. `equal` compares
 * values as queries do; `inserting`: the document is being inserted (an upsert), so
 * $setOnInsert applies.
 */
function applyUpdate(doc, changes, equal, make, inserting = false) {
	let changed = false;
	for (const change of changes) {
		const { op, path, segments, value } = change;
		if (op == '$setOnInsert' && !inserting) continue;
		if (op == '$rename') {
			const from = locate(doc, segments, false, make, 'source');
			if (!from?.present) continue;
			delete from.container[from.key];
			const to = locate(doc, change.toSegments, true, make, 'destination');
			setAt(to.container, to.last, from.current);
			changed = true;
			continue;
		}
		const at = locate(doc, segments, CREATING.has(op), make, undefined, op == '$pull');
		if (!at) continue; // $unset, $pull of a path that is not there
		changed ||= at.created;
		const { container, key, last, present, current } = at;
		const notArray = (what) =>
			failure(make, `${what ? what + ' ' : ''}The field '${path}' must be an array but is of type ${typeName(current)} in document {_id: ${show(doc._id)}}`, 2);
		switch (op) {
			case '$set':
			case '$setOnInsert':
				if (present && equal(current, value) && typeof current == typeof value) break;
				setAt(container, last, value);
				changed = true;
				break;
			case '$unset':
				if (!present) break;
				// an array keeps its length: the element becomes null, as in MongoDB
				if (Array.isArray(container)) {
					if (container[key] === null) break;
					container[key] = null;
				} else {
					delete container[key];
				}
				changed = true;
				break;
			case '$inc':
				if (!present) {
					setAt(container, last, value);
					changed = true;
				} else if (typeof current != 'number') {
					throw failure(make, `Cannot apply $inc to a value of non-numeric type. {_id: ${show(doc._id)}} has the field '${last}' of non-numeric type ${typeName(current)}`, 14);
				} else if (value !== 0) {
					container[key] = current + value;
					changed = true;
				}
				break;
			case '$min':
			case '$max':
				// the smaller or larger in MongoDB's order (of any types)
				if (present && (op == '$min' ? compareValues(value, current) >= 0 : compareValues(value, current) <= 0)) break;
				setAt(container, last, value);
				changed = true;
				break;
			case '$push': {
				if (present && !Array.isArray(current)) throw notArray('');
				const before = present ? current : [];
				let items = [...before];
				const at = change.position === undefined ? items.length : change.position < 0 ? Math.max(0, items.length + change.position) : Math.min(change.position, items.length);
				items.splice(at, 0, ...change.each);
				if (change.sort !== undefined) items = sortArray(items, change.sort);
				if (change.slice !== undefined) items = change.slice >= 0 ? items.slice(0, change.slice) : items.slice(Math.max(0, items.length + change.slice));
				if (present && equal(items, before)) break;
				setAt(container, last, items);
				changed = true;
				break;
			}
			case '$addToSet': {
				if (present && !Array.isArray(current)) throw notArray('Cannot apply $addToSet to non-array field.');
				const items = present ? [...current] : [];
				for (const item of change.each) if (!items.some((existing) => equal(existing, item))) items.push(item);
				if (present && items.length == current.length) break;
				setAt(container, last, items);
				changed = true;
				break;
			}
			case '$pull': {
				if (!present) break;
				if (!Array.isArray(current)) throw failure(make, 'Cannot apply $pull to a non-array value', 2);
				const kept = current.filter((element) => !pulls(element, value, equal));
				if (kept.length == current.length) break;
				setAt(container, last, kept);
				changed = true;
				break;
			}
		}
	}
	return changed;
}

/** Sets `segment` of an object, or of an array (padding it with nulls up to the position). */
function setAt(container, segment, value) {
	if (Array.isArray(container)) {
		const at = position(segment);
		while (container.length < at) container.push(null);
		container[at] = value;
	} else {
		container[segment] = value;
	}
}

/** An operator expression ({ $gt: 5 }): no value an upsert could take from the filter. */
function isOperators(value) {
	if (!isPlainObject(value)) return false;
	for (const key in value) return key.startsWith('$');
	return false;
}

/**
 * The document an upsert inserts: the filter's equality fields (dot paths become embedded
 * documents; conditions with operators give nothing), with `_id` first when the filter has one.
 */
function upsertSeed(filter) {
	const doc = hasOwn(filter, '_id') && !isOperators(filter._id) && !(filter._id instanceof RegExp) ? { _id: filter._id } : {};
	for (const path in filter) {
		// $and, $or, conditions with operators and regular expressions give nothing
		if (path == '_id' || path.startsWith('$') || isOperators(filter[path]) || filter[path] instanceof RegExp) continue;
		const segments = path.split('.');
		let container = doc;
		let ok = true;
		for (const segment of segments.slice(0, -1)) {
			if (!hasOwn(container, segment)) container[segment] = {};
			container = container[segment];
			if (!isPlainObject(container)) {
				ok = false;
				break;
			}
		}
		if (ok) container[segments[segments.length - 1]] = filter[path];
	}
	return doc;
}

module.exports = { parseUpdate, applyUpdate, upsertSeed };
