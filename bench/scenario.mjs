// The benchmark: the same operations for both databases. `api`: { db, size() → bytes on disk,
// durable() → a collection whose writes are on disk before they resolve }. `N` documents.
export async function run(api, record, N = 10_000_000) {
	const { db } = api;
	const CHUNK = Math.min(100_000, N);
	// the same random values for both databases
	let seed = 42;
	const rand = (n) => {
		seed = (seed * 16807) % 2147483647;
		return seed % n;
	};
	const now = () => performance.now();
	const once = async (group, label, fn) => {
		const start = now();
		const result = await fn();
		record({ group, label, ms: now() - start, result });
	};
	// warm-up on other values, then the average of `n` calls
	const avg = async (group, label, n, fn) => {
		for (let i = 0; i < Math.min(20, n); i++) await fn(n + i);
		let result;
		const start = now();
		for (let i = 0; i < n; i++) result = await fn(i);
		record({ group, label, ms: (now() - start) / n, result });
	};
	const median = async (group, label, fn) => {
		const times = [];
		let result;
		for (let i = 0; i < 3; i++) {
			const start = now();
			result = await fn();
			times.push(now() - start);
		}
		record({ group, label, ms: times.sort((a, b) => a - b)[1], result });
	};
	const length = (list) => list.length;
	const docs = db.collection('docs');
	const agg = (pipeline) => docs.aggregate(pipeline).toArray();

	// ---- load
	await once('Load', `insertMany ${N.toLocaleString('en')} documents (batches of ${CHUNK.toLocaleString('en')})`, async () => {
		for (let i = 0; i < N; i += CHUNK) {
			const batch = [];
			for (let j = i; j < i + CHUNK; j++) batch.push({ _id: j, title: 'Item ' + j, text: 'lorem ipsum ' + j, g: j % 1000, price: j % 997 });
			await docs.insertMany(batch);
		}
	});
	await once('Load', 'createIndex({ g: 1 })', () => docs.createIndex({ g: 1 }));
	await once('Load', 'createIndex({ text: 1 })', () => docs.createIndex({ text: 1 }));
	await once('Load', 'createIndex({ title: 1 }, { unique: true })', () => docs.createIndex({ title: 1 }, { unique: true }));
	await once('Load', 'createIndex({ g: 1, price: -1 }) (compound)', () => docs.createIndex({ g: 1, price: -1 }));
	record({ group: 'Size', label: 'Data on disk: the documents and 5 indexes', bytes: await api.size() });

	// ---- reads
	const R = 'Reads by key and index';
	await avg(R, 'findOne({ _id })', 20_000, () => docs.findOne({ _id: rand(N) }).then((d) => d && 1));
	await avg(R, 'findOne({ text }) (index)', 20_000, () => docs.findOne({ text: 'lorem ipsum ' + rand(N) }).then((d) => d && 1));
	await avg(R, 'find({ text }).toArray() (index, 1 document)', 20_000, () => docs.find({ text: 'lorem ipsum ' + rand(N) }).toArray().then(length));
	await avg(R, 'find({ _id: { $in: [10 ids] } })', 2_000, () => docs.find({ _id: { $in: Array.from({ length: 10 }, () => rand(N)) } }).toArray().then(length));
	await avg(R, 'find({ g }) (index, 1/1000 of the documents)', 30, () => docs.find({ g: rand(1000) }).toArray().then(length));
	await avg(R, 'find({ g, price }) (compound index)', 1_000, () => docs.find({ g: rand(1000), price: rand(997) }).toArray().then(length));
	await avg(R, 'find({ g: { $gte, $lt } }) (index range, 2/1000)', 20, () => {
		const g = rand(998);
		return docs.find({ g: { $gte: g, $lt: g + 2 } }).toArray().then(length);
	});
	await avg(R, 'find({ $or: [{ text }, { title }] }) (two indexes)', 2_000, () => docs.find({ $or: [{ text: 'lorem ipsum ' + rand(N) }, { title: 'Item ' + rand(N) }] }).toArray().then(length));
	await avg(R, 'find({ text: /^lorem ipsum 12345/ }) (prefix regex, index)', 500, () => docs.find({ text: new RegExp('^lorem ipsum ' + (10000 + rand(90000))) }).toArray().then(length));

	// ---- sorting and paging
	const S = 'Sort and paging';
	await avg(S, 'find({ g }).sort({ price: -1 }).limit(10) (compound index)', 1_000, () => docs.find({ g: rand(1000) }).sort({ price: -1 }).limit(10).toArray().then(length));
	await avg(S, 'find({}).sort({ g: -1 }).limit(10) (index order)', 1_000, () => docs.find({}).sort({ g: -1 }).limit(10).toArray().then(length));
	await avg(S, 'find({ g }).sort({ text: 1 }).skip(100).limit(10) (sorted in memory)', 100, () => docs.find({ g: rand(1000) }).sort({ text: 1 }).skip(100).limit(10).toArray().then(length));
	await median(S, 'find({}).sort({ price: -1, _id: 1 }).limit(10) (no index, all documents)', () => docs.find({}).sort({ price: -1, _id: 1 }).limit(10).toArray().then(length));

	// ---- counting and scans
	const C = 'Counting and scans';
	await avg(C, 'countDocuments({})', 1_000, () => docs.countDocuments({}));
	await avg(C, 'countDocuments({ g }) (index)', 200, () => docs.countDocuments({ g: rand(1000) }));
	await avg(C, 'countDocuments({ g, price: { $lt: 100 } }) (compound index)', 1_000, () => docs.countDocuments({ g: rand(1000), price: { $lt: 100 } }));
	await avg(C, 'countDocuments({ text: /^lorem ipsum 4242/ }) (prefix regex)', 500, () => docs.countDocuments({ text: new RegExp('^lorem ipsum ' + (1000 + rand(9000))) }));
	await median(C, 'countDocuments({ price: 5 }) (no index)', () => docs.countDocuments({ price: 5 }));
	await median(C, 'countDocuments({ price: { $in: [1, 2, 3] }, g: { $ne: 7 } })', () => docs.countDocuments({ price: { $in: [1, 2, 3] }, g: { $ne: 7 } }));
	await median(C, 'countDocuments({ price: { $not: { $gt: 10 } } }) (no index)', () => docs.countDocuments({ price: { $not: { $gt: 10 } } }));
	await median(C, 'countDocuments({ $nor: [{ g: { $lt: 990 } }, { price: { $lt: 900 } }] })', () => docs.countDocuments({ $nor: [{ g: { $lt: 990 } }, { price: { $lt: 900 } }] }));
	await median(C, 'countDocuments({ title: { $type: "string" } })', () => docs.countDocuments({ title: { $type: 'string' } }));
	await median(C, 'find({ price: 5 }).toArray() (no index)', () => docs.find({ price: 5 }).toArray().then(length));
	await median(C, 'find({ text: /ipsum 12345$/ }) (regex, not a prefix)', () => docs.find({ text: /ipsum 12345$/ }).toArray().then(length));

	// ---- aggregation
	const A = 'Aggregation';
	await avg(A, '[$match g, $group by price, $count]', 100, () => agg([{ $match: { g: rand(1000) } }, { $group: { _id: '$price', n: { $sum: 1 } } }, { $count: 'groups' }]).then((a) => a[0]?.groups));
	await avg(A, '[$match g, $count] (index)', 500, () => agg([{ $match: { g: rand(1000) } }, { $count: 'n' }]).then((a) => a[0]?.n));
	await avg(A, '[$sort g -1, $limit 10] (index)', 1_000, () => agg([{ $sort: { g: -1 } }, { $limit: 10 }]).then(length));
	await avg(A, '[$match g, $sort price, $skip 100, $limit 10]', 100, () => agg([{ $match: { g: rand(1000) } }, { $sort: { price: 1 } }, { $skip: 100 }, { $limit: 10 }]).then(length));
	await median(A, '[$match g < 10, $project { t: "$title" }]', () => agg([{ $match: { g: { $lt: 10 } } }, { $project: { _id: 0, t: '$title' } }]).then(length));
	await median(A, '[$group by g: $sum, $avg, $max] (all documents)', () => agg([{ $group: { _id: '$g', s: { $sum: '$price' }, a: { $avg: '$price' }, m: { $max: '$price' } } }]).then(length));
	await median(A, '[$match g < 100, $group null: $avg price]', () => agg([{ $match: { g: { $lt: 100 } } }, { $group: { _id: null, a: { $avg: '$price' } } }]).then(length));
	await median(A, '[$sort price -1, $limit 10] (no index, all documents)', () => agg([{ $sort: { price: -1, _id: 1 } }, { $limit: 10 }]).then(length));
	await median(A, '[$skip half, $limit 1]', () => agg([{ $skip: N / 2 }, { $limit: 1 }]).then(length));

	// ---- writes
	const W = 'Writes';
	await avg(W, 'insertOne', 2_000, (i) => docs.insertOne({ _id: 'n' + i, title: 'New ' + i, text: 'new ' + i, g: i % 1000, price: i % 997 }).then(() => 1));
	await avg(W, 'insertOne rejected by the unique index (E11000)', 500, () => docs.insertOne({ title: 'Item ' + rand(N) }).then(() => 0, (e) => e.code));
	await avg(W, 'updateOne({ _id }, { $set })', 2_000, () => docs.updateOne({ _id: rand(N) }, { $set: { tag: 'x' } }).then((r) => r.modifiedCount));
	await avg(W, 'updateOne({ _id }, { $inc })', 2_000, () => docs.updateOne({ _id: rand(N) }, { $inc: { hits: 1 } }).then((r) => r.modifiedCount));
	await avg(W, 'updateOne({ text }, { $set: { price } }) (index, indexed field)', 1_000, () => docs.updateOne({ text: 'lorem ipsum ' + rand(N) }, { $set: { price: rand(997) } }).then((r) => r.matchedCount));
	await avg(W, 'updateOne({ _id }, { $push })', 2_000, () => docs.updateOne({ _id: rand(N) }, { $push: { log: rand(100) } }).then((r) => r.modifiedCount));
	await avg(W, 'updateOne upsert (inserts)', 1_000, (i) => docs.updateOne({ _id: 'u' + i }, { $set: { g: 1 }, $setOnInsert: { title: 'Upserted ' + i } }, { upsert: true }).then((r) => r.upsertedCount));
	await median(W, 'updateMany({ g }, { $inc }) (index)', () => docs.updateMany({ g: rand(1000) }, { $inc: { price: 1 } }).then((r) => r.modifiedCount));
	await median(W, 'updateMany({ price }, { $set }) (no index)', () => docs.updateMany({ price: rand(997) }, { $set: { flag: true } }).then((r) => r.modifiedCount));
	await avg(W, 'deleteOne({ _id })', 1_000, () => docs.deleteOne({ _id: rand(N) }).then((r) => r.deletedCount));
	await avg(W, 'deleteOne({ text }) (index)', 1_000, () => docs.deleteOne({ text: 'lorem ipsum ' + rand(N) }).then((r) => r.deletedCount));
	await once(W, 'deleteMany({ g: 7 }) (index)', () => docs.deleteMany({ g: 7 }).then((r) => r.deletedCount));
	await once(W, 'deleteMany({ price: 5 }) (no index)', () => docs.deleteMany({ price: 5 }).then((r) => r.deletedCount));
	const durable = await api.durable();
	await avg(W, 'insertOne, durable before it resolves (strict / j: true)', 300, (i) => durable.insertOne({ _id: i, v: i }).then(() => 1));

	// ---- arrays
	const M = N / 10;
	const Y = `Arrays (${M.toLocaleString('en')} documents)`;
	const arr = db.collection('arr');
	await once(Y, 'insertMany { tags: [1-4 numbers], items: [2 documents] }', async () => {
		let s = 7;
		const r = (n) => (s = (s * 16807) % 2147483647) % n;
		const step = Math.min(50_000, M);
		for (let i = 0; i < M; i += step) {
			const batch = [];
			for (let j = i; j < i + step; j++) batch.push({ _id: j, tags: Array.from({ length: 1 + r(4) }, () => r(1000)), items: [{ a: r(100), b: r(100) }, { a: r(100), b: r(100) }] });
			await arr.insertMany(batch);
		}
	});
	await median(Y, 'countDocuments({ tags: { $size: 3 } }) (no index)', () => arr.countDocuments({ tags: { $size: 3 } }));
	await median(Y, 'countDocuments({ tags: { $all: [5, 7] } }) (no index)', () => arr.countDocuments({ tags: { $all: [5, 7] } }));
	await median(Y, 'countDocuments({ items: { $elemMatch: { a: 5, b: 7 } } }) (no index)', () => arr.countDocuments({ items: { $elemMatch: { a: 5, b: 7 } } }));
	await median(Y, '[$unwind tags, $group, $sort, $limit 5]', () => arr.aggregate([{ $unwind: '$tags' }, { $group: { _id: '$tags', n: { $sum: 1 } } }, { $sort: { n: -1 } }, { $limit: 5 }]).toArray().then(length));
	await once(Y, 'createIndex({ tags: 1 }) (multikey)', () => arr.createIndex({ tags: 1 }));
	await avg(Y, 'find({ tags: x }) (multikey index)', 1_000, () => arr.find({ tags: rand(1000) }).toArray().then(length));
	await avg(Y, 'find({ tags: { $all: [x, y] } }) (index)', 1_000, () => arr.find({ tags: { $all: [rand(1000), rand(1000)] } }).toArray().then(length));
	await avg(Y, 'find({ tags: { $elemMatch: { $gte: x, $lt: x + 2 } } }) (index)', 200, () => {
		const x = rand(998);
		return arr.find({ tags: { $elemMatch: { $gte: x, $lt: x + 2 } } }).toArray().then(length);
	});
	await avg(Y, 'updateOne({ _id }, { $addToSet: { tags } }) (multikey index)', 1_000, () => arr.updateOne({ _id: rand(M) }, { $addToSet: { tags: rand(1000) } }).then((r) => r.matchedCount));
	await avg(Y, 'updateOne({ _id }, { $pull: { tags } }) (multikey index)', 1_000, () => arr.updateOne({ _id: rand(M) }, { $pull: { tags: rand(1000) } }).then((r) => r.matchedCount));

	// ---- administration
	const D = 'Administration';
	await avg(D, 'listIndexes()', 1_000, () => docs.listIndexes().toArray().then(length));
	await avg(D, 'listCollections()', 1_000, () => db.listCollections().toArray().then(length));
	await once(D, 'dropIndex("g_1")', () => docs.dropIndex('g_1').then(() => 1));
	await once(D, 'renameCollection', () => db.renameCollection('docs', 'docs2').then(() => 1));
	await once(D, 'drop() (the arrays collection)', () => arr.drop());
	await once(D, 'deleteMany({}) (the other documents)', () => db.collection('docs2').deleteMany({}).then((r) => r.deletedCount));
	await once(D, 'dropDatabase()', () => db.dropDatabase().then(() => 1));
}
