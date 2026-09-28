# mostik

An embedded document database for Node.js with the MongoDB API. It runs inside your process and
keeps its data in a local directory: no server to install, no connection to manage. If you know
the MongoDB driver, you already know mostik: `insertOne`, `find`, `updateMany`, `aggregate`,
`createIndex`, query and update operators, and MongoDB's error codes.

- Queries, sorting and counting answer like MongoDB 7.0 (checked against a real MongoDB server on
  random data, see [Compatibility](#compatibility)).
- On 10 million documents it is faster than MongoDB in most operations and takes less space on
  disk and less memory (see [Benchmarks](#benchmarks)).
- TypeScript types included; ES modules and CommonJS.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Connecting](#connecting)
- [Inserting](#inserting)
- [Finding](#finding)
- [Query operators](#query-operators)
- [Sorting and paging](#sorting-and-paging)
- [Counting](#counting)
- [Updating](#updating)
- [Deleting](#deleting)
- [Indexes](#indexes)
- [Aggregation](#aggregation)
- [Collections and databases](#collections-and-databases)
- [Errors](#errors)
- [Compatibility](#compatibility)
- [Benchmarks](#benchmarks)

## Install

```sh
npm install mostik
```

## Quick start

```js
import { MostikClient } from 'mostik';

const client = await MostikClient.connect('./data');
const users = client.db('app').collection('users');

await users.createIndex({ email: 1 }, { unique: true });
await users.insertOne({ name: 'Ann', email: 'ann@example.com', tags: ['admin'], age: 34 });

const ann = await users.findOne({ email: 'ann@example.com' });
const admins = await users.find({ tags: 'admin', age: { $gte: 18 } }).sort({ name: 1 }).toArray();

await users.updateOne({ _id: ann._id }, { $inc: { logins: 1 }, $set: { lastSeen: new Date() } });

await client.close();
```

CommonJS:

```js
const { MostikClient } = require('mostik');
```

## Connecting

```js
const client = await MostikClient.connect(path, options);
// or: const client = new MostikClient(path, options); await client.connect();

const db = client.db('shop'); // any number of databases in one directory
const orders = db.collection('orders'); // created on the first write
// ...
await client.close(); // waits for pending writes, then releases the files
```

`path` is a directory (created if missing), or a single file when the name has an extension.

Options:

| Option | Default | Meaning |
| --- | --- | --- |
| `durability` | `'journal'` | When a write counts as done. `'journal'`, like MongoDB's default: a crash of the process loses nothing; a power loss can drop the last ~100 ms of writes. `'strict'`, like MongoDB's `j: true`: every write is on disk before its promise resolves. |
| `cacheSize` | `64 * 1024 * 1024` | Bytes of memory for cached data. Larger values keep more of a big database in memory. |
| `compression` | `false` | `true` compresses stored documents larger than 1000 bytes; `{ threshold }` sets the size. Saves disk space on large text documents. |

Only one process can open a database at a time.

### Memory

mostik keeps its memory bounded whatever the size of the database: a cache of `cacheSize`,
changes waiting to be written, and about 64 MB for the sorts and groups of all queries
together; what does not fit goes to temporary files. On 10 million documents the process stays
at about 400 MB at most, even while sorting or grouping all of them. What your code holds is on top of
that: stream large results with `for await` instead of `toArray()`.

Node.js lets garbage pile up while it has room; to keep the whole process under a fixed amount,
cap its heap: `node --max-old-space-size=256 app.js`.

### Long queries

Long queries do not hold up the rest of your process: scans, counts, sorts and groups over
millions of documents let the event loop run every few milliseconds, so a server keeps
answering other requests meanwhile.

## Inserting

```js
const { insertedId } = await users.insertOne({ name: 'Bob' }); // adds an ObjectId _id when missing
await users.insertOne({ _id: 'bob', name: 'Bob' }); // or give your own _id

await users.insertMany([{ name: 'A' }, { name: 'B' }]); // stops at the first failure
await users.insertMany(docs, { ordered: false }); // inserts every document it can
```

A duplicate `_id` or a value a unique index already holds rejects with `MongoServerError`
code `11000`; `insertMany` rejects with `MongoBulkWriteError`, whose `insertedCount` and
`insertedIds` tell what was inserted anyway.

Values: strings, numbers, booleans, `null`, arrays, embedded documents, `Date`, `ObjectId`,
`Buffer` / `Uint8Array`, `BigInt` (64-bit integers).

## Finding

```js
await users.findOne({ _id: id }); // a document or null
await users.findOne({ 'address.city': 'Riga' }); // dot paths reach into embedded documents
await users.find({ tags: 'admin' }).toArray(); // an array field matches when one element does

for await (const user of users.find({ age: { $gt: 30 } })) {
	// documents are read lazily, in chunks
}

const cursor = users.find({ active: true });
while (await cursor.hasNext()) console.log(await cursor.next());
```

Equality follows MongoDB: `1` equals `1.0`, `null` matches a missing field, embedded documents
compare field by field in order, and `'a.0'` means both "element 0 of array `a`" and "field `0`".

## Query operators

| Kind | Operators |
| --- | --- |
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin` |
| Logical | `$and`, `$or`, `$nor`, `$not` |
| Element | `$exists`, `$type` (names such as `"string"`, `"int"`, `"number"`, or BSON type numbers) |
| Arrays | `$all`, `$size`, `$elemMatch` |
| Strings | `$regex` with `$options` (`i`, `m`, `s`), or a regular expression as the value |

```js
await products.find({ price: { $gte: 10, $lt: 100 }, category: { $in: ['books', 'music'] } }).toArray();
await products.find({ $or: [{ stock: 0 }, { discontinued: true }] }).toArray();
await products.find({ name: /^mug/i }).toArray();
await products.find({ sizes: { $all: ['S', 'M'] }, reviews: { $elemMatch: { stars: 5, verified: true } } }).toArray();
await products.find({ price: { $not: { $gt: 100 } }, notes: { $exists: false } }).toArray();
await products.find({ tags: { $size: 0 } }).toArray();
```

A regular expression anchored at the start with a literal prefix (`/^abc/`, without the `i` or
`m` option) reads only that part of an index.

## Sorting and paging

```js
await posts.find({ author: 'ann' }).sort({ createdAt: -1 }).skip(20).limit(10).toArray();
await posts.find({}, { sort: { score: -1, _id: 1 }, limit: 5 }).toArray();
```

`sort` accepts `{ field: 1 | -1 }`, `[['field', 'desc']]`, `'field'`, and `'asc'` / `'desc'`.
Values sort in MongoDB's order of types; an array sorts by its smallest element ascending and its
largest descending. When an index gives the order (a single-field index, or a compound index
whose first fields the filter fixes), results stream in that order without being sorted. A sort
that does not fit in memory goes through temporary files next to the database, as in MongoDB 7;
`.allowDiskUse(false)` (or `find(filter, { allowDiskUse: false })`) makes it fail with error
`292` instead.

## Counting

```js
await orders.countDocuments(); // a stored counter: instant at any size
await orders.countDocuments({ status: 'paid' }); // counts index entries when an index answers exactly
await orders.countDocuments({ total: { $gt: 100 } }, { skip: 10, limit: 1000 });
```

## Updating

```js
await users.updateOne({ _id: id }, { $set: { 'profile.city': 'Oslo' }, $inc: { visits: 1 } });
await users.updateMany({ plan: 'trial' }, { $set: { plan: 'free' }, $unset: { trialEnds: '' } });
await users.updateOne({ email }, { $set: { name }, $setOnInsert: { created: new Date() } }, { upsert: true });
```

Returns `{ matchedCount, modifiedCount, upsertedCount, upsertedId }`.

| Operator | Does |
| --- | --- |
| `$set`, `$unset` | set or remove fields (dot paths and array positions allowed) |
| `$inc` | add to a number |
| `$min`, `$max` | set when the new value is smaller / larger (MongoDB's order of types) |
| `$rename` | move a field |
| `$setOnInsert` | set only when an upsert inserts |
| `$push` | append; with `$each`, `$position`, `$slice`, `$sort` |
| `$addToSet` | append values the array does not hold yet; with `$each` |
| `$pull` | remove elements equal to a value or matching a condition |

`upsert: true` inserts a document built from the filter's equality fields when nothing matches.

## Deleting

```js
await sessions.deleteOne({ token });
await sessions.deleteMany({ expires: { $lt: new Date() } });
await sessions.deleteMany({}); // empties the collection
```

## Indexes

```js
await users.createIndex({ email: 1 }, { unique: true }); // refuses duplicates (code 11000)
await users.createIndex({ 'address.city': 1 }); // dot paths
await posts.createIndex({ author: 1, createdAt: -1 }); // compound: filter on author, sort by date
await products.createIndex({ tags: 1 }); // arrays: every element is indexed

await users.listIndexes().toArray(); // [{ v: 2, key: { _id: 1 }, name: '_id_' }, ...]
await users.dropIndex('email_1'); // by name, or by key pattern: dropIndex({ email: 1 })
```

Queries pick an index by themselves: equality, `$in`, ranges, prefix regular expressions,
`$elemMatch`, `$all`, and `$or` of indexed conditions. A unique index treats a missing field as
`null`, so only one document may lack it. Writes to a collection wait while one of its indexes
is being built; reads do not.

## Aggregation

```js
const top = await orders
	.aggregate([
		{ $match: { status: 'paid' } },
		{ $unwind: '$items' },
		{ $group: { _id: '$items.sku', sold: { $sum: '$items.qty' }, revenue: { $sum: '$items.total' } } },
		{ $sort: { revenue: -1 } },
		{ $limit: 10 },
		{ $project: { _id: 0, sku: '$_id', sold: 1, revenue: 1 } },
	])
	.toArray();
```

| Stage | Supported |
| --- | --- |
| `$match` | every query operator; a leading `$match` uses indexes |
| `$group` | `_id` from field paths, documents or constants; accumulators `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$count` |
| `$sort`, `$skip`, `$limit` | a `$sort` right after the first `$match` uses indexes; `$sort` then `$limit` keeps only the top documents in memory |
| `$project` | include, exclude, rename and compute fields (`'$path'`, `$literal`, constants) |
| `$unwind` | `'$path'`, or `{ path, includeArrayIndex, preserveNullAndEmptyArrays }` |
| `$count` | `$match` then `$count` counts without reading documents |

`$sort` and `$group` that do not fit in memory go through temporary files next to the database;
`aggregate(pipeline, { allowDiskUse: false })` makes them fail with error `292` instead.

Expressions are field paths (`'$a.b'`, `'$$ROOT'`), `{ $literal: value }`, constants, and
documents or arrays of those.

## Collections and databases

```js
await db.listCollections().toArray(); // [{ name, type: 'collection', ... }]
await db.renameCollection('orders', 'orders_2024', { dropTarget: false });
await orders.drop(); // the collection with its documents and indexes
await db.dropDatabase();
```

## Errors

Failures are `MongoServerError` with MongoDB's `code`, so existing error handling keeps working:

```js
import { MongoServerError } from 'mostik';

try {
	await users.insertOne({ email: 'ann@example.com' });
} catch (error) {
	if (error instanceof MongoServerError && error.code === 11000) {
		console.log('taken:', error.keyValue); // { email: 'ann@example.com' }
	}
}
```

Common codes: `11000` duplicate key, `14` wrong type for an operator (`$inc` on a string), `28`
a path that cannot be created or traversed, `40` conflicting update paths, `66` changing `_id`,
`146` one group's `$push` / `$addToSet` over 100 MB, `171` indexing two arrays of one document
in a compound index, `292` a sort or `$group` over the memory limit with `allowDiskUse: false`.

## Compatibility

mostik was checked against MongoDB 7.0 by running the same random documents and operations on
both and comparing every answer: `find`, `countDocuments`, sorts with `skip` / `limit`,
aggregation pipelines, updates, deletes and index builds, with and without indexes. About
180,000 comparisons, no differences, error codes included.

Not supported yet. Unsupported operators and stages fail with a "not supported" error instead of
answering wrongly:

- query operators `$mod`, `$expr`, `$where`, `$text`, geospatial queries;
- update operators `$pop`, `$pullAll`, `$mul`, `$currentDate`, `$bit`, positional `$` / `$[]`;
- aggregation stages other than those listed above (`$lookup`, `$facet`, `$addFields`, ...) and
  operator expressions such as `$add`.

Also missing: `find` projections, `replaceOne`, `findOneAndUpdate`, `bulkWrite`, transactions,
change streams, and text indexes. `createIndex` options other than `name` and `unique` are
ignored: `expireAfterSeconds` (TTL), `sparse` and `partialFilterExpression` create a regular
index.

Other differences:

- regular expressions use JavaScript's syntax (MongoDB uses PCRE); the `x` option is not
  supported;
- `$gt` / `$lt` compare numbers, strings, booleans, dates and ObjectIds (not documents or arrays);
- two indexes on the same fields with different directions (`{ a: 1, b: 1 }` and
  `{ a: 1, b: -1 }`) cannot exist together; one of them serves sorts in any direction.

## Benchmarks

The same script ran against MongoDB 7.0.43 (a local `mongod` with default settings, through the
official Node.js driver) and against mostik, one after the other on the same machine, with the
same data and the same random query values. Both returned identical results in every row.

- Machine: Apple M2 Pro, 16 GB of memory, macOS; Node.js 23.
- Data: 10,000,000 documents `{ _id, title, text, g, price }` (`g`: 1,000 groups of 10,000,
  `price`: 997 values) with indexes on `g`, `text`, `title` (unique) and `{ g, price }`, then a second collection of 1,000,000 documents with arrays.
- Times: µs and short ms figures are the average of hundreds or thousands of calls with random
  values; full scans are the median of 3 runs; loads and drops ran once.
- Both use their default durability (`'journal'` / `w: 1`); one row measures writes that are on
  disk before they resolve (`'strict'` / `j: true`).
- Memory: the peak physical footprint of the `mongod` process, and of the whole Node.js process
  running mostik and the benchmark itself (on Linux the scripts report the peak resident size).
  Index builds run but are left out of the tables.

To run it yourself (`mongod` 7.0 installed; `N=1000000` for a quicker run):

```sh
cd bench
npm install
MONGOD=/path/to/mongod node mongodb.mjs   # results/mongodb.jsonl
node mostik.mjs                           # results/mostik.jsonl
node table.mjs                            # the tables below, as Markdown (--all: index builds too)
```

Of the 67 rows mostik is ahead in 55, behind in 10 and equal in 2.

**Load**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| `insertMany 10,000,000 documents (batches of 100,000)` | 24.22 s | 24.60 s | same |

**Size**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| Data on disk: the documents and 5 indexes | 1.01 GB | 555 MB | **1.9× smaller** |

**Reads by key and index**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| `findOne({ _id })` | 82.5 µs | 7.3 µs | **11× faster** |
| `findOne({ text }) (index)` | 134 µs | 45.6 µs | **2.9× faster** |
| `find({ text }).toArray() (index, 1 document)` | 86.3 µs | 17.4 µs | **5.0× faster** |
| `find({ _id: { $in: [10 ids] } })` | 147 µs | 82.7 µs | **1.8× faster** |
| `find({ g }) (index, 1/1000 of the documents)` | 23.9 ms | 15.8 ms | **1.5× faster** |
| `find({ g, price }) (compound index)` | 342 µs | 113 µs | **3.0× faster** |
| `find({ g: { $gte, $lt } }) (index range, 2/1000)` | 47.4 ms | 31.1 ms | **1.5× faster** |
| `find({ $or: [{ text }, { title }] }) (two indexes)` | 324 µs | 178 µs | **1.8× faster** |
| `find({ text: /^lorem ipsum 12345/ }) (prefix regex, index)` | 326 µs | 284 µs | **1.1× faster** |

**Sort and paging**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| `find({ g }).sort({ price: -1 }).limit(10) (compound index)` | 210 µs | 130 µs | **1.6× faster** |
| `find({}).sort({ g: -1 }).limit(10) (index order)` | 94.4 µs | 60.8 µs | **1.6× faster** |
| `find({ g }).sort({ text: 1 }).skip(100).limit(10) (sorted in memory)` | 14.8 ms | 13.7 ms | **1.1× faster** |
| `find({}).sort({ price: -1, _id: 1 }).limit(10) (no index, all documents)` | 2.90 s | 233 ms | **12× faster** |

**Counting and scans**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| `countDocuments({})` | 1.74 s | 1.4 µs | **over 1000× faster** |
| `countDocuments({ g }) (index)` | 2.27 ms | 563 µs | **4.0× faster** |
| `countDocuments({ g, price: { $lt: 100 } }) (compound index)` | 423 µs | 40.0 µs | **11× faster** |
| `countDocuments({ text: /^lorem ipsum 4242/ }) (prefix regex)` | 561 µs | 52.8 µs | **11× faster** |
| `countDocuments({ price: 5 }) (no index)` | 2.10 s | 179 ms | **12× faster** |
| `countDocuments({ price: { $in: [1, 2, 3] }, g: { $ne: 7 } })` | 13.0 ms | 8.07 ms | **1.6× faster** |
| `countDocuments({ price: { $not: { $gt: 10 } } }) (no index)` | 2.09 s | 174 ms | **12× faster** |
| `countDocuments({ $nor: [{ g: { $lt: 990 } }, { price: { $lt: 900 } }] })` | 1.98 s | 167 ms | **12× faster** |
| `countDocuments({ title: { $type: "string" } })` | 4.13 s | 156 ms | **26× faster** |
| `find({ price: 5 }).toArray() (no index)` | 2.13 s | 181 ms | **12× faster** |
| `find({ text: /ipsum 12345$/ }) (regex, not a prefix)` | 4.63 s | 4.40 s | **1.1× faster** |

**Aggregation**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| `[$match g, $group by price, $count]` | 7.76 ms | 14.5 ms | 1.9× slower |
| `[$match g, $count] (index)` | 2.05 ms | 777 µs | **2.6× faster** |
| `[$sort g -1, $limit 10] (index)` | 119 µs | 66.3 µs | **1.8× faster** |
| `[$match g, $sort price, $skip 100, $limit 10]` | 510 µs | 474 µs | **1.1× faster** |
| `[$match g < 10, $project { t: "$title" }]` | 230 ms | 129 ms | **1.8× faster** |
| `[$group by g: $sum, $avg, $max] (all documents)` | 7.76 s | 2.71 s | **2.9× faster** |
| `[$match g < 100, $group null: $avg price]` | 681 ms | 657 ms | same |
| `[$sort price -1, $limit 10] (no index, all documents)` | 2.95 s | 260 ms | **11× faster** |
| `[$skip half, $limit 1]` | 537 ms | 266 ms | **2.0× faster** |

**Writes**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| `insertOne` | 167 µs | 221 µs | 1.3× slower |
| `insertOne rejected by the unique index (E11000)` | 168 µs | 228 µs | 1.4× slower |
| `updateOne({ _id }, { $set })` | 106 µs | 62.3 µs | **1.7× faster** |
| `updateOne({ _id }, { $inc })` | 91.4 µs | 55.7 µs | **1.6× faster** |
| `updateOne({ text }, { $set: { price } }) (index, indexed field)` | 276 µs | 401 µs | 1.5× slower |
| `updateOne({ _id }, { $push })` | 100 µs | 62.3 µs | **1.6× faster** |
| `updateOne upsert (inserts)` | 81.8 µs | 95.9 µs | 1.2× slower |
| `updateMany({ g }, { $inc }) (index)` | 243 ms | 99.3 ms | **2.5× faster** |
| `updateMany({ price }, { $set }) (no index)` | 2.38 s | 335 ms | **7.1× faster** |
| `deleteOne({ _id })` | 314 µs | 564 µs | 1.8× slower |
| `deleteOne({ text }) (index)` | 304 µs | 373 µs | 1.2× slower |
| `deleteMany({ g: 7 }) (index)` | 810 ms | 205 ms | **3.9× faster** |
| `deleteMany({ price: 5 }) (no index)` | 3.76 s | 700 ms | **5.4× faster** |
| `insertOne, durable before it resolves (strict / j: true)` | 4.53 ms | 4.31 ms | **1.1× faster** |

**Arrays (1,000,000 documents)**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| `insertMany { tags: [1-4 numbers], items: [2 documents] }` | 4.42 s | 3.68 s | **1.2× faster** |
| `countDocuments({ tags: { $size: 3 } }) (no index)` | 239 ms | 8.84 ms | **27× faster** |
| `countDocuments({ tags: { $all: [5, 7] } }) (no index)` | 226 ms | 10.2 ms | **22× faster** |
| `countDocuments({ items: { $elemMatch: { a: 5, b: 7 } } }) (no index)` | 297 ms | 28.5 ms | **10× faster** |
| `[$unwind tags, $group, $sort, $limit 5]` | 867 ms | 810 ms | **1.1× faster** |
| `find({ tags: x }) (multikey index)` | 6.19 ms | 2.35 ms | **2.6× faster** |
| `find({ tags: { $all: [x, y] } }) (index)` | 3.38 ms | 2.39 ms | **1.4× faster** |
| `find({ tags: { $elemMatch: { $gte: x, $lt: x + 2 } } }) (index)` | 12.6 ms | 6.27 ms | **2.0× faster** |
| `updateOne({ _id }, { $addToSet: { tags } }) (multikey index)` | 113 µs | 155 µs | 1.4× slower |
| `updateOne({ _id }, { $pull: { tags } }) (multikey index)` | 71.4 µs | 9.2 µs | **7.8× faster** |

**Administration**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| `listIndexes()` | 76.0 µs | 2.3 µs | **33× faster** |
| `listCollections()` | 72.8 µs | 1.6 µs | **45× faster** |
| `dropIndex("g_1")` | 2.66 ms | 57.2 ms | 21× slower |
| `renameCollection` | 1.50 ms | 419 µs | **3.6× faster** |
| `drop() (the arrays collection)` | 762 µs | 17.5 ms | 23× slower |
| `deleteMany({}) (the other documents)` | 70.12 s | 305 ms | **230× faster** |
| `dropDatabase()` | 1.28 ms | 253 µs | **5.1× faster** |

**Memory**

| Operation | MongoDB | mostik | mostik vs MongoDB |
| --- | ---: | ---: | --- |
| Peak memory of the process | 2.60 GB | 274 MB | **9.7× smaller** |

`countDocuments({})` in mostik reads a stored counter; MongoDB's `countDocuments` counts the
documents (its `estimatedDocumentCount` is the fast path there).

Where mostik is behind: `$group` after a `$match` that selects documents through an index, and
`drop()` / `dropIndex()` on large collections.

Single-document writes (`insertOne`, `updateOne`, `deleteOne`) take 50–600 µs on both, and
their figures move by up to 2× between runs with the load of the machine: in this run some are
behind MongoDB by 1.2–1.8×, in other runs they were ahead by as much.

mostik ran with its default 64 MB cache. Queries that read many documents from all over a large
collection get faster with a larger `cacheSize`: `find({ g }).sort({ text: 1 }).skip(100).limit(10)`
takes 15 ms with 64 MB and 9 ms with 512 MB.

