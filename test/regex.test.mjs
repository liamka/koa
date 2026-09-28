// $regex (a prefix goes through an index) and $elemMatch
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { MostikClient } from 'mostik';
import { tempDir } from './helpers.mjs';

async function withCollection(fn) {
	const client = await MostikClient.connect(path.join(tempDir(), 'db'));
	try {
		await fn(client.db('app').collection('items'));
	} finally {
		await client.close();
	}
}

function generator(seed) {
	const random = (n) => {
		seed = (seed * 16807) % 2147483647;
		return seed % n;
	};
	const pick = (list) => list[random(list.length)];
	return { random, pick };
}

const ids = (docs) => docs.map((d) => String(d._id)).sort();

test('$regex forms and options; strings only, array elements, missing fields', async () => {
	await withCollection(async (items) => {
		await items.insertMany([
			{ _id: 1, s: 'Apple' },
			{ _id: 2, s: 'apple pie' },
			{ _id: 3, s: 'line one\nApple two' },
			{ _id: 4, s: ['kiwi', 'Apricot'] },
			{ _id: 5, s: 42 },
			{ _id: 6 },
			{ _id: 7, s: 'a\nb' },
			{ _id: 8, s: { t: 'Apple' } },
		]);
		const find = async (filter) => (await items.find(filter).toArray()).map((d) => d._id).sort((a, b) => a - b);
		assert.deepEqual(await find({ s: /^Ap/ }), [1, 4]);
		assert.deepEqual(await find({ s: { $regex: '^Ap' } }), [1, 4]);
		assert.deepEqual(await find({ s: { $regex: /^ap/i } }), [1, 2, 4]);
		assert.deepEqual(await find({ s: { $regex: '^ap', $options: 'i' } }), [1, 2, 4]);
		assert.deepEqual(await find({ s: { $regex: /^Ap/, $options: 'm' } }), [1, 3, 4]);
		assert.deepEqual(await find({ s: /a.b/ }), []);
		assert.deepEqual(await find({ s: /a.b/s }), [7]);
		assert.deepEqual(await find({ s: /4/ }), [], 'numbers are not strings');
		assert.deepEqual(await find({ s: /pp/ }), [1, 2, 3]);
		assert.deepEqual(await find({ 's.t': /^App/ }), [8]);
		assert.deepEqual(await find({ s: { $in: [/^kiw/, 42] } }), [4, 5]);
		assert.deepEqual(await find({ s: { $nin: [/p/i, 42] } }), [6, 7, 8]);
		assert.deepEqual(await find({ s: { $regex: '^A', $ne: 'Apple' } }), [4]);
		assert.deepEqual(await find({ $or: [{ s: /^line/ }, { _id: 6 }] }), [3, 6]);
		// an upsert does not take a regular expression as the value
		await items.updateOne({ s: /^zzz/ }, { $set: { n: 1 } }, { upsert: true });
		const made = await items.findOne({ n: 1 });
		assert.deepEqual(Object.keys(made).sort(), ['_id', 'n']);
	});
});

test('bad $regex and $elemMatch filters are rejected', async () => {
	await withCollection(async (items) => {
		await assert.rejects(items.findOne({ s: { $options: 'i' } }), /\$options needs a \$regex/);
		await assert.rejects(items.findOne({ s: { $regex: /a/i, $options: 'm' } }), /options set in both/);
		await assert.rejects(items.findOne({ s: { $regex: 5 } }), /\$regex has to be a string/);
		await assert.rejects(items.findOne({ s: { $regex: 'a', $options: 'x' } }), /option x is not supported/);
		await assert.rejects(items.findOne({ s: { $regex: '(' } }), /Regular expression is invalid/);
		await assert.rejects(items.findOne({ s: { $ne: /a/ } }), /value only/);
		await assert.rejects(items.findOne({ a: { $elemMatch: 5 } }), /\$elemMatch needs an Object/);
		await assert.rejects(items.findOne({ a: { $elemMatch: { $gt: 1, b: 2 } } }), /unknown operator: b/);
		await assert.rejects(items.findOne({ a: { $elemMatch: { b: { $mod: [2, 0] } } } }), /\$mod is not supported/);
	});
});

// a reference for regular expressions, apart from lib/query.js
const regexRef = (value, re) => {
	const one = (v) => typeof v == 'string' && new RegExp(re.source, re.flags).test(v);
	return one(value) || (Array.isArray(value) && value.some(one));
};

test('$regex agrees with a reference: scan, single, multikey and compound indexes, _id', async () => {
	await withCollection(async (items) => {
		const { random, pick } = generator(7);
		const letters = ['a', 'b', 'c', 'ab', '.', 'я', 'A', ' ', '\n', '\u{10000}', 'é', '-'];
		const word = () => Array.from({ length: random(5) }, () => pick(letters)).join('');
		const docs = Array.from({ length: 1500 }, (_, i) => {
			const d = { _id: random(3) ? `${word()}#${i}` : i, g: random(4) };
			if (random(8)) d.s = random(5) ? word() : pick([[word(), word()], 7, null, [], true]);
			return d;
		});
		await items.insertMany(docs);
		const patterns = [/^a/, /^ab/, /^abc/, /^a\.b/, /^я/, /^\u{10000}/u, /^ab.*c/, /^a+b/, /^ab?/, /^A/i, /^b/m, /b$/, /a|b/, /^/, /^$/, /^-a/, /^é/, /c/, /^ ?a/, /^ab{2}/, /^\./];
		const check = async (label) => {
			for (const re of patterns) {
				for (const [filter, test] of [
					[{ s: re }, (d) => regexRef(d.s, re)],
					[{ s: { $regex: re.source, $options: re.flags } }, (d) => regexRef(d.s, re)],
					[{ g: 2, s: re }, (d) => d.g == 2 && regexRef(d.s, re)],
					[{ _id: re }, (d) => regexRef(d._id, re)],
					[{ s: { $in: [re, /^c/, 'b'] } }, (d) => regexRef(d.s, re) || regexRef(d.s, /^c/) || d.s === 'b' || (Array.isArray(d.s) && d.s.includes('b'))],
				]) {
					if (filter.s?.$options?.length === 0) delete filter.s.$options;
					const what = `${label}: ${String(re)} ${Object.keys(filter)}`;
					const expected = docs.filter(test);
					assert.deepEqual(ids(await items.find(filter).toArray()), ids(expected), what);
					assert.equal(await items.countDocuments(filter), expected.length, `count ${what}`);
				}
				// sorted by the field of the regular expression, both ways
				const expected = docs.filter((d) => regexRef(d.s, re));
				const sorted = await items.find({ s: re }).sort({ s: -1 }).toArray();
				assert.deepEqual(ids(sorted), ids(expected), `sorted ${label} ${String(re)}`);
			}
		};
		await check('scan');
		await items.createIndex({ s: 1 });
		await check('multikey index');
		await items.createIndex({ g: 1, s: 1 });
		await check('compound index');
		// strings only: sorted through a single, non-multikey index
		await items.dropIndex('s_1');
		await items.dropIndex('g_1_s_1');
		const strings = docs.filter((d) => typeof d.s == 'string');
		await items.deleteMany({ _id: { $in: docs.filter((d) => typeof d.s != 'string').map((d) => d._id) } });
		docs.splice(0, docs.length, ...strings);
		await items.createIndex({ s: -1 });
		await check('single index');
		const inOrder = await items.find({ s: /^a/ }).sort({ s: 1 }).toArray();
		assert.deepEqual(inOrder.map((d) => d.s), docs.filter((d) => d.s.startsWith('a')).map((d) => d.s).sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b))));
	});
});

// a reference for $elemMatch on the forms the test uses
const cmp = (v, op, x) => typeof v == typeof x && v !== null && (op == '$gt' ? v > x : op == '$gte' ? v >= x : op == '$lt' ? v < x : op == '$lte' ? v <= x : false);
const valueRef = (el, spec) => Object.entries(spec).every(([op, x]) => (op == '$in' ? x.includes(el) : op == '$ne' ? el !== x : op == '$regex' ? typeof el == 'string' && x.test(el) : cmp(el, op, x)));
const fieldRef = (el, field, cond) => {
	const v = el[field];
	if (typeof cond != 'object' || cond === null) return v === cond || (Array.isArray(v) && v.includes(cond));
	return Object.entries(cond).every(([op, x]) => cmp(v, op, x) || (Array.isArray(v) && v.some((i) => cmp(i, op, x))));
};
const docRef = (el, spec) => el !== null && typeof el == 'object' && !Array.isArray(el) && Object.entries(spec).every(([f, c]) => fieldRef(el, f, c));

test('$elemMatch agrees with a reference: values and documents, scan and indexes', async () => {
	await withCollection(async (items) => {
		const { random, pick } = generator(13);
		const docs = Array.from({ length: 1500 }, (_, i) => {
			const d = { _id: i, k: random(3) };
			const n = random(4);
			d.v = random(6) ? Array.from({ length: n }, () => (random(5) ? random(20) : pick(['x', 'yz', null]))) : pick([5, 'x', null]);
			d.o = random(6) ? Array.from({ length: n }, () => (random(6) ? { a: random(10), b: random(3) ? random(10) : [random(10), random(10)] } : pick([3, 'x', [{ a: 1 }]]))) : { a: random(10), b: 1 };
			return d;
		});
		await items.insertMany(docs);
		const cases = [
			[{ v: { $elemMatch: { $gt: 5, $lt: 8 } } }, (d) => Array.isArray(d.v) && d.v.some((e) => valueRef(e, { $gt: 5, $lt: 8 }))],
			[{ v: { $elemMatch: { $gte: 18 } } }, (d) => Array.isArray(d.v) && d.v.some((e) => valueRef(e, { $gte: 18 }))],
			[{ v: { $elemMatch: { $in: [3, 'x'] } } }, (d) => Array.isArray(d.v) && d.v.some((e) => valueRef(e, { $in: [3, 'x'] }))],
			[{ v: { $elemMatch: { $regex: /^y/ } } }, (d) => Array.isArray(d.v) && d.v.some((e) => valueRef(e, { $regex: /^y/ }))],
			[{ v: { $elemMatch: { $ne: 4 } }, k: 1 }, (d) => d.k == 1 && Array.isArray(d.v) && d.v.some((e) => valueRef(e, { $ne: 4 }))],
			[{ v: { $gt: 5, $elemMatch: { $lt: 2 } } }, (d) => Array.isArray(d.v) && d.v.some((e) => valueRef(e, { $lt: 2 })) && d.v.some((e) => cmp(e, '$gt', 5))],
			[{ o: { $elemMatch: { a: 3 } } }, (d) => Array.isArray(d.o) && d.o.some((e) => docRef(e, { a: 3 }))],
			[{ o: { $elemMatch: { a: { $gte: 4 }, b: { $lt: 3 } } } }, (d) => Array.isArray(d.o) && d.o.some((e) => docRef(e, { a: { $gte: 4 }, b: { $lt: 3 } }))],
			[{ o: { $elemMatch: { a: 2, b: 7 } } }, (d) => Array.isArray(d.o) && d.o.some((e) => docRef(e, { a: 2, b: 7 }))],
			[{ o: { $elemMatch: { a: { $gt: 1, $lt: 3 } } }, k: { $ne: 0 } }, (d) => d.k != 0 && Array.isArray(d.o) && d.o.some((e) => docRef(e, { a: { $gt: 1, $lt: 3 } }))],
			[{ $or: [{ o: { $elemMatch: { b: 9 } } }, { v: { $elemMatch: { $lte: 0 } } }] }, (d) => (Array.isArray(d.o) && d.o.some((e) => docRef(e, { b: 9 }))) || (Array.isArray(d.v) && d.v.some((e) => valueRef(e, { $lte: 0 })))],
		];
		const check = async (label) => {
			for (const [filter, test] of cases) {
				const what = `${label}: ${JSON.stringify(filter)}`;
				const expected = docs.filter(test);
				assert.deepEqual(ids(await items.find(filter).toArray()), ids(expected), what);
				assert.equal(await items.countDocuments(filter), expected.length, `count ${what}`);
			}
		};
		await check('scan');
		await items.createIndex({ v: 1 });
		await items.createIndex({ 'o.a': 1 });
		await items.createIndex({ 'o.b': 1 });
		await items.createIndex({ k: 1, v: 1 });
		await check('indexes');
		// nested: arrays of arrays, $elemMatch inside $elemMatch
		await items.insertOne({ _id: 'n', m: [[1, 2], [7, 9]], p: [{ q: [{ r: 5 }] }] });
		assert.equal((await items.findOne({ m: { $elemMatch: { $elemMatch: { $gt: 8 } } } }))?._id, 'n');
		assert.equal(await items.findOne({ m: { $elemMatch: { $gt: 8 } } }), null, 'an array element is not compared as its elements');
		assert.equal((await items.findOne({ p: { $elemMatch: { q: { $elemMatch: { r: 5 } } } } }))?._id, 'n');
		assert.equal(await items.findOne({ p: { $elemMatch: { q: { $elemMatch: { r: 6 } } } } }), null);
	});
});

test('$elemMatch on documents whose element shapes are defined inside them, counted natively', async () => {
	await withCollection(async (items) => {
		// use up the shared structures, so new shapes are defined inside each document
		await items.insertMany(Array.from({ length: 40 }, (_, i) => ({ _id: `fill${i}`, [`f${i}`]: 1 })));
		const docs = Array.from({ length: 300 }, (_, i) => ({
			_id: i,
			list: [{ p: i % 5, q: { r: i % 3 } }, { p: (i + 1) % 5, s: [i % 4, 9] }, i % 2 ? 'x' : { q: { r: 7 } }],
			v: [i % 7, [i % 5], 'k'],
		}));
		await items.insertMany(docs);
		const cases = [
			[{ list: { $elemMatch: { p: 2, 'q.r': 1 } } }, (d) => d.list.some((e) => typeof e == 'object' && e.p === 2 && e.q?.r === 1)],
			[{ list: { $elemMatch: { 'q.r': 7 } } }, (d) => d.list.some((e) => typeof e == 'object' && e.q?.r === 7)],
			[{ list: { $elemMatch: { s: { $elemMatch: { $gte: 3, $lt: 5 } } } } }, (d) => d.list.some((e) => Array.isArray(e.s) && e.s.some((x) => x >= 3 && x < 5))],
			[{ list: { $elemMatch: { p: { $exists: false } } } }, (d) => d.list.some((e) => typeof e == 'object' && !('p' in e))],
			[{ v: { $elemMatch: { $ne: 3, $lt: 5 } } }, (d) => d.v.some((e) => typeof e == 'number' && e !== 3 && e < 5)],
			[{ v: { $elemMatch: { $elemMatch: { $in: [2, 4] } } } }, (d) => d.v.some((e) => Array.isArray(e) && e.some((x) => x === 2 || x === 4))],
			[{ v: { $elemMatch: { $gt: 'a' } } }, () => true],
		];
		for (const [filter, test] of cases) {
			const expected = docs.filter(test);
			assert.deepEqual(ids(await items.find(filter).toArray()), ids(expected), JSON.stringify(filter));
			assert.equal(await items.countDocuments(filter), expected.length, `count ${JSON.stringify(filter)}`);
		}
	});
});
