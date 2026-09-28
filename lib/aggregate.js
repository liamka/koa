'use strict';
// Aggregation stages $match, $group, $count, $sort, $skip, $limit, $project and $unwind, with
// MongoDB semantics.

const { ObjectId } = require('./object-id');
const { matches, checkFilter, compareValues, equal, isPlainObject, filterPaths, sortKeys, compareSortKeys } = require('./query');
const { Budget, sizeOf, sortRecords, RunWriter, RunReader, Heap, removeFile } = require('./spill');
const { Wait, breathe } = require('./pause');

// memory one group's accumulators may take (MongoDB's limit for $push and $addToSet)
const GROUP_MEMORY = 100 << 20;

const STAGES = new Set(['$match', '$group', '$count', '$sort', '$skip', '$limit', '$project', '$unwind']);
const ACCUMULATORS = new Set(['$sum', '$avg', '$min', '$max', '$first', '$last', '$push', '$addToSet', '$count']);

/** A missing value (a field path that leads nowhere), kept apart from null. */
const MISSING = undefined;

/**
 * Checks a pipeline and returns its stages as `{ op, arg }`. `make` builds a MongoServerError
 * from a message and fields.
 */
function parsePipeline(pipeline, make) {
	if (!Array.isArray(pipeline)) throw new TypeError('The pipeline must be an array of stages');
	const stages = pipeline.map((stage) => {
		const keys = isPlainObject(stage) ? Object.keys(stage) : [];
		if (keys.length != 1) throw make('A pipeline stage specification object must contain exactly one field.', { code: 40323 });
		const [op] = keys;
		if (!op.startsWith('$')) throw make(`Unrecognized pipeline stage name: '${op}'`, { code: 40324 });
		if (!STAGES.has(op)) throw new Error(`mostik: aggregation stage ${op} is not supported`);
		const arg = stage[op];
		switch (op) {
			case '$match':
				checkFilter(arg);
				break;
			case '$count':
				if (typeof arg != 'string' || arg.length == 0) throw make('the count field must be a non-empty string', { code: 40156 });
				if (arg.startsWith('$')) throw make("the count field cannot be a $-prefixed path", { code: 40158 });
				if (arg.includes('.')) throw make("the count field cannot contain '.'", { code: 40160 });
				break;
			case '$group':
				checkGroup(arg, make);
				break;
			case '$sort':
				return { op, arg: parseSort(arg, make) };
			case '$skip':
				if (typeof arg != 'number' || !Number.isInteger(arg)) throw make(`invalid argument to $skip stage: Expected an integer: $skip: ${JSON.stringify(arg)}`, { code: 5107200 });
				if (arg < 0) throw make(`invalid argument to $skip stage: Expected a non-negative number in: $skip: ${arg}`, { code: 5107200 });
				break;
			case '$limit':
				if (typeof arg != 'number' || !Number.isInteger(arg)) throw make(`invalid argument to $limit stage: Expected an integer: $limit: ${JSON.stringify(arg)}`, { code: 5107201 });
				if (arg <= 0) throw make('the limit must be positive', { code: 15958 });
				break;
			case '$project':
				return { op, arg: parseProject(arg, make) };
			case '$unwind':
				return { op, arg: parseUnwind(arg, make) };
		}
		return { op, arg };
	});
	// $sort then $skip / $limit: only the first documents are needed
	stages.forEach((stage, i) => {
		if (stage.op != '$sort') return;
		let skipped = 0;
		for (const next of stages.slice(i + 1)) {
			if (next.op == '$skip') skipped += next.arg;
			else if (next.op == '$limit') stage.arg.wanted = Math.min(stage.arg.wanted || Infinity, skipped + next.arg);
			else break;
		}
	});
	return stages;
}

/** `{ a: 1, b: -1 }` as `{ sort: [[path, direction]], wanted, make }` (wanted: 0, all). */
function parseSort(spec, make) {
	if (!isPlainObject(spec)) throw make('the $sort key specification must be an object', { code: 15973 });
	const sort = Object.entries(spec).map(([path, direction]) => {
		if (isPlainObject(direction)) throw new Error('mostik: $sort by $meta is not supported');
		if (direction !== 1 && direction !== -1) throw make('$sort key ordering must be 1 (for ascending) or -1 (for descending)', { code: 15975 });
		return [path, direction];
	});
	if (sort.length == 0) throw make('$sort stage must have at least one sort key', { code: 15976 });
	return { sort, wanted: 0, make };
}

/**
 * A $project specification as a tree: per field `{ include }`, `{ exclude }`, `{ value }` (a
 * compiled expression) or `{ children }` (a sub-projection); `inclusion`: the kind of the
 * projection (fields named, plus computed ones), else fields left out.
 */
function parseProject(spec, make) {
	if (!isPlainObject(spec)) throw make('$project specification must be an object', { code: 15969 });
	if (Object.keys(spec).length == 0) throw make('$project requires at least one output field', { code: 51272 });
	let inclusion = null;
	const expressions = [];
	const kind = (included, path) => {
		// _id is kept or left out in either kind
		if (path == '_id') return;
		if (inclusion === null) inclusion = included;
		else if (inclusion && !included) throw make(`Invalid $project :: caused by :: Cannot do exclusion on field ${path} in inclusion projection`, { code: 31254 });
		else if (!inclusion && included) throw make(`Invalid $project :: caused by :: Cannot do inclusion on field ${path} in exclusion projection`, { code: 31253 });
	};
	const build = (spec, prefix) => {
		const node = new Map();
		for (const key in spec) {
			const path = prefix + key;
			if (key.startsWith('$')) throw make(`Invalid $project :: caused by :: FieldPath field names may not start with '$'. Consider using $getField or $setField.`, { code: 16410 });
			const value = spec[key];
			const segments = key.split('.');
			// a dotted key is a chain of sub-projections
			let at = node;
			for (const segment of segments.slice(0, -1)) {
				let child = at.get(segment);
				if (!child) at.set(segment, (child = { children: new Map() }));
				if (!child.children) throw make(`Invalid $project :: caused by :: Path collision at ${path}`, { code: 31250 });
				at = child.children;
			}
			const last = segments[segments.length - 1];
			if (at.has(last) && !(at.get(last).children && isPlainObject(value) && !isExpression(value)))
				throw make(`Invalid $project :: caused by :: Path collision at ${path}`, { code: 31250 });
			if (typeof value == 'number' || typeof value == 'boolean') {
				kind(Boolean(value), path);
				at.set(last, value ? { include: true } : { exclude: true });
			} else if (isPlainObject(value) && !isExpression(value)) {
				if (Object.keys(value).length == 0) throw make('An empty sub-projection is not a valid value. Found empty object at path', { code: 51270 });
				const sub = build(value, path + '.');
				const had = at.get(last);
				if (had) for (const [k, v] of sub) had.children.set(k, v);
				else at.set(last, { children: sub });
			} else {
				checkExpression(value);
				kind(true, path);
				expressions.push(value);
				at.set(last, { value: compile(value) });
			}
		}
		return node;
	};
	const tree = build(spec, '');
	// only _id: kept, it alone; left out, every other field stays
	if (inclusion === null) inclusion = Boolean(tree.get('_id')?.include);
	if (inclusion && !tree.has('_id')) tree.set('_id', { include: true });
	return { tree, inclusion, expressions };
}

/** An object that is an expression ({ $literal: .. }, an operator) rather than a sub-projection. */
function isExpression(value) {
	return Object.keys(value).some((key) => key.startsWith('$'));
}

function hasValues(tree) {
	for (const node of tree.values()) if (node.value || (node.children && hasValues(node.children))) return true;
	return false;
}

/** The fields of `value` a projection that names them keeps, and its computed fields. */
function included(tree, value, root) {
	if (Array.isArray(value)) {
		const out = [];
		for (const item of value) {
			if (isPlainObject(item) || Array.isArray(item)) out.push(included(tree, item, root));
			else if (hasValues(tree)) out.push(included(tree, {}, root));
		}
		return out;
	}
	if (!isPlainObject(value)) return hasValues(tree) ? included(tree, {}, root) : MISSING;
	const out = {};
	for (const key in value) {
		const node = tree.get(key);
		if (!node) continue;
		if (node.include) out[key] = value[key];
		else if (node.children) {
			const kept = included(node.children, value[key], root);
			if (kept !== MISSING) out[key] = kept;
		}
	}
	for (const [key, node] of tree) {
		if (node.value) {
			const computed = node.value(root);
			if (computed !== MISSING) out[key] = computed;
		} else if (node.children && !Object.hasOwn(value, key) && hasValues(node.children)) {
			out[key] = included(node.children, {}, root);
		}
	}
	return out;
}

/** `value` without the fields a projection leaves out. */
function excluded(tree, value) {
	if (Array.isArray(value)) return value.map((item) => (isPlainObject(item) || Array.isArray(item) ? excluded(tree, item) : item));
	if (!isPlainObject(value)) return value;
	const out = {};
	for (const key in value) {
		const node = tree.get(key);
		if (node?.exclude) continue;
		out[key] = node?.children ? excluded(node.children, value[key]) : value[key];
	}
	return out;
}

/** '$a.b' or `{ path, includeArrayIndex, preserveNullAndEmptyArrays }`. */
function parseUnwind(spec, make) {
	const options = typeof spec == 'string' ? { path: spec } : spec;
	if (!isPlainObject(options)) throw make('expected either a string or an object as specification for $unwind stage', { code: 15981 });
	for (const key in options) {
		if (key != 'path' && key != 'includeArrayIndex' && key != 'preserveNullAndEmptyArrays') throw make(`unrecognized option to $unwind stage: ${key}`, { code: 28811 });
	}
	const { path, includeArrayIndex, preserveNullAndEmptyArrays = false } = options;
	if (path === undefined) throw make('no path specified to $unwind stage', { code: 28812 });
	if (typeof path != 'string') throw make('expected a string as the path for $unwind stage', { code: 28808 });
	if (!path.startsWith('$') || path.length < 2) throw make(`path option to $unwind stage should be prefixed with a '$': ${path}`, { code: 28818 });
	if (typeof preserveNullAndEmptyArrays != 'boolean') throw make('expected a boolean for the preserveNullAndEmptyArrays option to $unwind stage', { code: 28809 });
	if (includeArrayIndex !== undefined) {
		if (typeof includeArrayIndex != 'string' || includeArrayIndex.length == 0) throw make('expected a non-empty string for the includeArrayIndex option to $unwind stage', { code: 28810 });
		if (includeArrayIndex.startsWith('$')) throw make(`includeArrayIndex option to $unwind stage should not be prefixed with a '$': ${includeArrayIndex}`, { code: 28822 });
	}
	return { segments: path.slice(1).split('.'), index: includeArrayIndex?.split('.'), preserve: preserveNullAndEmptyArrays };
}

/** The value at `segments` through embedded documents only (as $unwind reads its path). */
function nested(doc, segments) {
	let value = doc;
	for (const segment of segments) {
		if (!isPlainObject(value) || !Object.hasOwn(value, segment)) return MISSING;
		value = value[segment];
	}
	return value;
}

/** A copy of `doc` with `value` at `segments` (MISSING: the field removed); the rest shared. */
function withValue(doc, segments, value) {
	const [first, ...rest] = segments;
	const out = { ...doc };
	if (rest.length == 0) {
		if (value === MISSING) delete out[first];
		else out[first] = value;
	} else {
		out[first] = withValue(isPlainObject(doc[first]) ? doc[first] : {}, rest, value);
	}
	return out;
}

function* unwind(docs, { segments, index, preserve }) {
	for (const doc of docs) {
		if (doc instanceof Wait) {
			yield doc;
			continue;
		}
		const value = nested(doc, segments);
		if (Array.isArray(value) && value.length > 0) {
			for (let i = 0; i < value.length; i++) {
				const out = withValue(doc, segments, value[i]);
				yield index ? withValue(out, index, i) : out;
			}
		} else if (value === MISSING || value === null || Array.isArray(value)) {
			if (!preserve) continue;
			// an empty array goes, null and a missing field stay as they are
			const out = Array.isArray(value) ? withValue(doc, segments, MISSING) : doc;
			yield index ? withValue(out, index, null) : out;
		} else {
			// a value that is not an array unwinds as one of one
			yield index ? withValue(doc, index, null) : doc;
		}
	}
}

/**
 * Documents by `sort`, the first `wanted` only (0: all), through temporary files when they do
 * not fit in memory (`io`). An array sorts by its smallest element (ascending) or largest
 * (descending), except in a sort on _id alone: MongoDB compares whole values there.
 */
function* sorted(docs, { sort, wanted }, io) {
	const byWholeId = sort.length == 1 && sort[0][0] == '_id';
	const parallel = () => {
		throw io.make('cannot sort with keys that are parallel arrays', { code: 2, codeName: 'BadValue' });
	};
	const keysOf = byWholeId ? (doc) => [doc._id === undefined ? null : doc._id] : (doc) => sortKeys(doc, sort, parallel);
	const compareKey = byWholeId ? compareValues : compareSortKeys;
	const compare = (a, b) => {
		for (let i = 0; i < sort.length; i++) {
			const o = compareKey(a[i], b[i]) * sort[i][1];
			if (o) return o;
		}
		return 0;
	};
	const records = (function* () {
		for (const value of docs) yield value instanceof Wait ? value : { keys: keysOf(value), value };
	})();
	const tooMuch = () =>
		io.make('Sort exceeded memory limit of 104857600 bytes, but did not opt in to external sorting.', { code: 292, codeName: 'QueryExceededMemoryLimitNoDiskUseAllowed' });
	for (const record of sortRecords(records, { compare, wanted, base: io.base, codec: io.codec, allowDisk: io.allowDisk, tooMuch })) {
		if (record instanceof Wait) yield record;
		else yield record.value ?? io.codec.decode(record.payload);
	}
}

function checkGroup(spec, make) {
	if (!isPlainObject(spec)) throw make('a group\'s fields must be specified in an object', { code: 15947 });
	if (!('_id' in spec)) throw make("a group specification must include an _id", { code: 15955 });
	checkExpression(spec._id);
	for (const field in spec) {
		if (field == '_id') continue;
		if (field.includes('.')) throw make(`The field name '${field}' cannot contain '.'`, { code: 40235 });
		if (field.startsWith('$')) throw make(`The field name '${field}' cannot be an operator name`, { code: 40236 });
		const accumulator = spec[field];
		const ops = isPlainObject(accumulator) ? Object.keys(accumulator) : [];
		if (ops.length != 1) throw make(`The field '${field}' must be an accumulator object`, { code: 40234 });
		const [op] = ops;
		if (!ACCUMULATORS.has(op)) {
			if (op.startsWith('$')) throw new Error(`mostik: accumulator ${op} is not supported`);
			throw make(`The field '${field}' must be an accumulator object`, { code: 40234 });
		}
		if (op == '$count') {
			if (!isPlainObject(accumulator.$count) || Object.keys(accumulator.$count).length > 0)
				throw make('$count takes no arguments, i.e. $count:{}', { code: 5362100 });
		} else {
			checkExpression(accumulator[op]);
		}
	}
}

/** Expressions: field paths ("$a.b"), $$ROOT / $$CURRENT, { $literal }, documents and arrays of expressions, constants. */
function checkExpression(expr) {
	if (typeof expr == 'string') {
		if (expr.startsWith('$$') && expr != '$$ROOT' && expr != '$$CURRENT' && !expr.startsWith('$$ROOT.') && !expr.startsWith('$$CURRENT.'))
			throw new Error(`mostik: variable ${expr} is not supported`);
		if (expr == '$') throw new Error("mostik: '$' is not a valid field path");
		return;
	}
	if (Array.isArray(expr)) return expr.forEach(checkExpression);
	if (isPlainObject(expr)) {
		const keys = Object.keys(expr);
		if (keys.length == 1 && keys[0] == '$literal') return;
		const operator = keys.find((key) => key.startsWith('$'));
		if (operator) throw new Error(`mostik: expression operator ${operator} is not supported`);
		for (const key of keys) checkExpression(expr[key]);
	}
}

/**
 * The value of a field path (`segments` from `at` on) in `value`: an array met on the way gives
 * an array of the rest of the path's value in each of its documents (elements that are not
 * documents, arrays included, give none).
 */
function fieldPath(value, segments, at = 0) {
	if (at == segments.length) return value;
	if (Array.isArray(value)) {
		const out = [];
		for (const item of value) {
			if (!isPlainObject(item)) continue;
			const next = fieldPath(item, segments, at);
			if (next !== MISSING) out.push(next);
		}
		return out;
	}
	if (isPlainObject(value) && Object.hasOwn(value, segments[at])) return fieldPath(value[segments[at]], segments, at + 1);
	return MISSING;
}

/** An expression as a function of the document, worked out once per pipeline. */
function compile(expr) {
	if (typeof expr == 'string' && expr.startsWith('$')) {
		const segments = expr.startsWith('$$') ? expr.slice(2).split('.').slice(1) : expr.slice(1).split('.');
		if (segments.length == 0) return (doc) => doc;
		if (segments.length == 1) {
			const [field] = segments;
			return (doc) => (isPlainObject(doc) && Object.hasOwn(doc, field) ? doc[field] : MISSING);
		}
		return (doc) => fieldPath(doc, segments);
	}
	if (Array.isArray(expr)) {
		const items = expr.map(compile);
		return (doc) => items.map((item) => item(doc) ?? null);
	}
	if (isPlainObject(expr)) {
		if ('$literal' in expr) return () => expr.$literal;
		const fields = Object.keys(expr).map((key) => [key, compile(expr[key])]);
		return (doc) => {
			const out = {};
			for (const [key, value] of fields) {
				const v = value(doc);
				if (v !== MISSING) out[key] = v;
			}
			return out;
		};
	}
	return () => expr;
}

/**
 * A Map key for grouping: values MongoDB sees as equal (1 and 1.0, -0 and 0, NaN and NaN, null
 * and missing) share it. Numbers, strings and booleans are their own keys (a Map tells them
 * apart, and takes -0 as 0 and NaN as NaN); the rest get a string of their own.
 */
function groupKey(value, pack) {
	if (value === MISSING || value === null) return null;
	const type = typeof value;
	if (type == 'number' || type == 'string' || type == 'boolean') return value;
	if (value instanceof ObjectId) return '\u0000o' + value.toHexString();
	if (value instanceof Date) return '\u0000D' + value.getTime();
	return '\u0000m' + Buffer.from(pack(normalized(value))).toString('base64');
}

function normalized(value) {
	if (Object.is(value, -0)) return 0;
	if (Array.isArray(value)) return value.map(normalized);
	if (isPlainObject(value)) {
		const out = {};
		for (const key in value) out[key] = normalized(value[key]);
		return out;
	}
	return value;
}

/** One group's accumulators: `{ field, op, value (the compiled expression), state }`. */
function newAccumulators(spec, compiled) {
	const out = [];
	for (const field in spec) {
		if (field == '_id') continue;
		const [op] = Object.keys(spec[field]);
		out.push({ field, op, value: compiled[field], state: initial(op) });
	}
	return out;
}

function initial(op) {
	switch (op) {
		case '$sum':
		case '$count':
			return 0;
		case '$avg':
			return { sum: 0, n: 0 };
		case '$push':
		case '$addToSet':
			return [];
		case '$first':
			return { set: false, value: null };
		default:
			return MISSING; // $min, $max, $last
	}
}

/** Adds `doc` to `acc`; returns the bytes of memory its state grew by (roughly). */
function accumulate(acc, doc) {
	if (acc.op == '$count') {
		acc.state++;
		return 0;
	}
	const value = acc.value(doc);
	switch (acc.op) {
		case '$sum':
			if (typeof value == 'number') acc.state += value;
			return 0;
		case '$avg':
			if (typeof value == 'number') {
				acc.state.sum += value;
				acc.state.n++;
			}
			return 0;
		case '$min':
		case '$max':
			// nulls and missing values do not count
			if (value !== MISSING && value !== null) {
				if (acc.state === MISSING) {
					acc.state = value;
					return sizeOf(value);
				}
				if ((acc.op == '$min' ? -1 : 1) * compareValues(value, acc.state) > 0) acc.state = value;
			}
			return 0;
		case '$first':
			if (acc.state.set) return 0;
			acc.state = { set: true, value: value ?? null };
			return sizeOf(value);
		case '$last':
			acc.state = value ?? null;
			return 0;
		case '$push':
			if (value === MISSING) return 0;
			acc.state.push(value);
			return 8 + sizeOf(value);
		case '$addToSet':
			if (value === MISSING || acc.state.some((seen) => equal(seen, value))) return 0;
			acc.state.push(value);
			return 8 + sizeOf(value);
	}
	return 0;
}

/**
 * The state of `a` (from documents before) and `b` (from documents after) of one accumulator,
 * made one: groups spilled to disk in parts come together.
 */
function combine(op, a, b) {
	switch (op) {
		case '$sum':
		case '$count':
			return a + b;
		case '$avg':
			return { sum: a.sum + b.sum, n: a.n + b.n };
		case '$min':
		case '$max':
			if (a === MISSING) return b;
			if (b === MISSING) return a;
			return (op == '$min' ? -1 : 1) * compareValues(b, a) > 0 ? b : a;
		case '$first':
			return a.set ? a : b;
		case '$last':
			return b;
		case '$push':
			return a.concat(b);
		case '$addToSet':
			return a.concat(b.filter((value) => !a.some((seen) => equal(seen, value))));
	}
	return b;
}

function result(acc) {
	switch (acc.op) {
		case '$avg':
			return acc.state.n ? acc.state.sum / acc.state.n : null;
		case '$first':
			return acc.state.value;
		case '$min':
		case '$max':
		case '$last':
			return acc.state ?? null;
		default:
			return acc.state;
	}
}

/**
 * $group over `docs`: one output document per distinct _id, in the order they first appear.
 * Groups past the memory limits go to temporary files (`io`: lib/spill.js), their partial
 * states in the order of their keys, to be merged; the output is then in that order.
 */
function* group(docs, spec, io) {
	const budget = new Budget();
	const runs = [];
	let groups = new Map();
	const id = compile(spec._id);
	const compiled = {};
	for (const field in spec) if (field != '_id') compiled[field] = compile(Object.values(spec[field])[0]);
	const tooLarge = (op) => io.make(`${op} used too much memory and cannot spill to disk. Memory limit: ${GROUP_MEMORY} bytes`, { code: 146, codeName: 'ExceededMemoryLimit' });
	const spill = () => {
		if (!io.allowDisk) throw io.make("Exceeded memory limit for $group, but didn't allow external sort. Pass allowDiskUse:true to opt in.", { code: 292, codeName: 'QueryExceededMemoryLimitNoDiskUseAllowed' });
		const sorted = [...groups].map(([key, g]) => [canonical(key), g]).sort((a, b) => Buffer.compare(a[0], b[0]));
		const run = new RunWriter(io.base);
		for (const [key, g] of sorted) run.write([key, io.codec.encode([g.id, g.accumulators.map((acc) => acc.state)])]);
		run.close();
		runs.push(run.file);
		groups = new Map();
		budget.release();
	};
	try {
		let read = 0;
		for (const doc of docs) {
			if (doc instanceof Wait) {
				yield doc;
				continue;
			}
			if ((++read & 1023) == 0) yield* breathe();
			const value = id(doc);
			const key = groupKey(value, io.pack);
			let g = groups.get(key);
			if (!g) {
				// the group, its accumulators and its entry in the Map
				g = { id: value === MISSING ? null : value, accumulators: newAccumulators(spec, compiled), size: 400 + 160 * (Object.keys(spec).length - 1) + sizeOf(value) };
				groups.set(key, g);
				budget.take(g.size);
			}
			for (const acc of g.accumulators) {
				const grew = accumulate(acc, doc);
				if (grew == 0) continue;
				g.size += grew;
				budget.take(grew);
				if (g.size > GROUP_MEMORY) throw tooLarge(acc.op);
			}
			if (budget.full) spill();
		}
		if (runs.length == 0) {
			for (const g of groups.values()) yield output(g.id, g.accumulators);
			return;
		}
		if (groups.size > 0) spill();
		yield* mergeGroups(runs, spec, compiled, io, tooLarge);
	} finally {
		budget.release();
		for (const file of runs) removeFile(file);
	}
}

function output(id, accumulators) {
	const out = { _id: id };
	for (const acc of accumulators) out[acc.field] = result(acc);
	return out;
}

/** The groups of runs on disk, each group's parts combined in the order of the runs. */
function* mergeGroups(files, spec, compiled, io, tooLarge) {
	const readers = files.map((file) => new RunReader(file, 2));
	try {
		const heap = new Heap((a, b) => Buffer.compare(a.key, b.key) || a.run - b.run);
		const advance = (run) => {
			const parts = readers[run].next();
			if (parts) heap.push({ key: parts[0], state: parts[1], run });
		};
		readers.forEach((_, run) => advance(run));
		let current = null;
		let read = 0;
		while (heap.size > 0) {
			if ((++read & 1023) == 0) yield* breathe();
			const { key, state, run } = heap.pop();
			advance(run);
			const [gid, states] = io.codec.decode(state);
			if (current && current.key.equals(key)) {
				current.accumulators.forEach((acc, i) => (acc.state = combine(acc.op, acc.state, states[i])));
				current.size += state.length;
				if (current.size > GROUP_MEMORY) throw tooLarge(current.accumulators.find((acc) => acc.op == '$push' || acc.op == '$addToSet')?.op ?? '$group');
				continue;
			}
			if (current) yield output(current.id, current.accumulators);
			const accumulators = newAccumulators(spec, compiled);
			accumulators.forEach((acc, i) => (acc.state = states[i]));
			current = { key, id: gid, accumulators, size: state.length };
		}
		if (current) yield output(current.id, current.accumulators);
	} finally {
		for (const reader of readers) reader.close();
	}
}

/** Bytes for a group key (groupKey's) that equal keys share, to sort groups spilled to disk by. */
function canonical(key) {
	if (key === null) return Buffer.from([0]);
	switch (typeof key) {
		case 'number': {
			const bytes = Buffer.alloc(9);
			bytes[0] = 1;
			if (Number.isNaN(key)) bytes.fill(0xff, 1);
			else bytes.writeDoubleBE(key == 0 ? 0 : key, 1);
			return bytes;
		}
		case 'boolean':
			return Buffer.from([2, key ? 1 : 0]);
		default:
			return Buffer.concat([Buffer.from([3]), Buffer.from(key, 'utf8')]);
	}
}

/**
 * Runs `stages` over `docs` (the documents the first stages did not already narrow down).
 * `io`: what stages need from the database (see Collection#pipelineIo in index.js).
 */
function* run(docs, stages, io) {
	let current = docs;
	for (const { op, arg } of stages) {
		const input = current;
		switch (op) {
			case '$match':
				current = (function* () {
					for (const doc of input) if (doc instanceof Wait || matches(doc, arg)) yield doc;
				})();
				break;
			case '$group':
				current = group(input, arg, io);
				break;
			case '$count':
				current = (function* () {
					let n = 0;
					for (const doc of input) {
						if (doc instanceof Wait) yield doc;
						else n++;
					}
					// no documents, no output (as MongoDB)
					if (n > 0) yield { [arg]: n };
				})();
				break;
			case '$sort':
				current = sorted(input, arg, io);
				break;
			case '$skip':
				current = (function* () {
					let n = arg;
					for (const doc of input) {
						if (doc instanceof Wait) yield doc;
						else if (n > 0) n--;
						else yield doc;
					}
				})();
				break;
			case '$limit':
				current = (function* () {
					let n = arg;
					// stopping here stops the stages before it
					for (const doc of input) {
						yield doc;
						if (!(doc instanceof Wait) && --n == 0) return;
					}
				})();
				break;
			case '$project':
				current = (function* () {
					const { tree, inclusion } = arg;
					for (const doc of input) yield doc instanceof Wait ? doc : inclusion ? included(tree, doc, doc) : excluded(tree, doc);
				})();
				break;
			case '$unwind':
				current = unwind(input, arg);
				break;
		}
	}
	yield* current;
}

/**
 * The top-level fields of the collection's documents that `stages` look at, or null when they
 * need whole documents (no $group, or $$ROOT).
 */
function fieldsUsed(stages) {
	const fields = new Set();
	let whole = false;
	const expression = (expr) => {
		if (typeof expr == 'string' && expr.startsWith('$')) {
			if (expr.startsWith('$$')) whole = true;
			else fields.add(expr.slice(1).split('.')[0]);
		} else if (Array.isArray(expr)) {
			expr.forEach(expression);
		} else if (isPlainObject(expr) && !('$literal' in expr)) {
			Object.values(expr).forEach(expression);
		}
	};
	const top = (path) => fields.add(path.split('.')[0]);
	for (const { op, arg } of stages) {
		if (op == '$match') filterPaths(arg).forEach(top);
		if (op == '$sort') for (const [path] of arg.sort) top(path);
		if (op == '$unwind') top(arg.segments.join('.'));
		// counting needs what the $match stages before it look at
		if (op == '$count') return [...fields];
		if (op == '$project') {
			// fields left out: the rest is kept, all of it
			if (!arg.inclusion) return null;
			for (const [key, node] of arg.tree) if (!node.value) fields.add(key);
			arg.expressions.forEach(expression);
			return whole ? null : [...fields];
		}
		if (op == '$group') {
			expression(arg._id);
			for (const field in arg) if (field != '_id') Object.values(arg[field]).forEach(expression);
			return whole ? null : [...fields];
		}
	}
	return null;
}

module.exports = { parsePipeline, run, groupKey, fieldsUsed, sorted };
