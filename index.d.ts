export declare class ObjectId {
	/** A new id, or one parsed from 24 hex characters / 12 bytes. */
	constructor(id?: string | Uint8Array | ObjectId);
	readonly id: Buffer;
	static isValid(id: unknown): boolean;
	static createFromHexString(hex: string): ObjectId;
	toHexString(): string;
	toString(): string;
	toJSON(): string;
	getTimestamp(): Date;
	equals(other: ObjectId | string): boolean;
}

export declare class MongoServerError extends Error {
	code: number;
	keyPattern?: Record<string, number>;
	keyValue?: Record<string, unknown>;
}

export interface MostikClientOptions {
	/** LZ4-compress stored documents larger than `threshold` bytes (1000 when `true`). */
	compression?: boolean | { threshold?: number };
	/**
	 * Memory for cached database pages, in bytes (default 64 MB). The process never holds more
	 * of the file than this; reads outside the cache go to disk. The first client to open a file
	 * in a process sets it for that file.
	 */
	cacheSize?: number;
	/**
	 * When a write counts as done. 'journal' (default, like MongoDB's default): written to the
	 * journal; survives the process crashing, a power loss can drop the last ~100 ms of writes.
	 * 'strict': flushed to the disk before the write resolves (like `j: true`).
	 */
	durability?: 'journal' | 'strict';
}

export type Document = { [key: string]: any };
export type WithId<T> = T & { _id: any };

/**
 * Comparisons of a field, all of which must hold. $gt, $gte, $lt, $lte compare values of the
 * same type (numbers, strings, booleans, dates, ObjectIds); $in: equal to one of the values;
 * $ne: not equal (a missing field is not equal). $in and $nin take regular expressions too.
 */
export interface FilterOperators {
	$gt?: unknown;
	$gte?: unknown;
	$lt?: unknown;
	$lte?: unknown;
	$in?: unknown[];
	$ne?: unknown;
	/** equal to none of the values (a missing field is not equal) */
	$nin?: unknown[];
	/** the field is there (null included), or not */
	$exists?: boolean;
	/**
	 * a string matching the regular expression; one anchored at the start with a literal
	 * prefix (/^abc/, no i or m option) reads only that part of an index
	 */
	$regex?: string | RegExp;
	/** options of $regex: i, m, s */
	$options?: string;
	/**
	 * an array with one element that meets every condition: operators on the element itself
	 * ({ $gt: 1, $lt: 5 }) or a filter on the fields of an embedded document ({ a: 1, b: 2 })
	 */
	$elemMatch?: FilterOperators | Filter<Document>;
	/** an array holding every one of the values (regular expressions, $elemMatch objects too) */
	$all?: unknown[];
	/** an array of this length */
	$size?: number;
	/**
	 * of one of these types, by name ("string", "int", "double", "long", "number", "bool",
	 * "date", "objectId", "binData", "object", "array", "null") or BSON number
	 */
	$type?: string | number | (string | number)[];
	/** the conditions do not hold (a missing field included) */
	$not?: FilterOperators | RegExp;
}

/**
 * Per field (dot paths reach into embedded documents and arrays): a value it must equal, or
 * a regular expression (as $regex), or FilterOperators. An array field matches when one of
 * its elements does.
 */
export type Filter<T> = { [K in keyof T]?: T[K] | RegExp | FilterOperators } & {
	/** every one of the filters holds */
	$and?: Filter<T>[];
	/** at least one of the filters holds */
	$or?: Filter<T>[];
	/** none of the filters holds */
	$nor?: Filter<T>[];
	[path: string]: unknown;
};

export interface InsertOneResult {
	acknowledged: true;
	insertedId: any;
}

export interface InsertManyResult {
	acknowledged: true;
	insertedCount: number;
	/** position in the input array -> _id */
	insertedIds: { [index: number]: any };
}

export interface DeleteResult {
	acknowledged: true;
	deletedCount: number;
}

/** $push's value: one element, or several with modifiers. */
export type PushValue = unknown | { $each: unknown[]; $position?: number; $slice?: number; $sort?: 1 | -1 | { [path: string]: 1 | -1 } };

/** Update operators; paths may be dot paths (array positions included). */
export interface UpdateFilter {
	$set?: { [path: string]: unknown };
	$unset?: { [path: string]: unknown };
	$inc?: { [path: string]: number };
	/** set if smaller / larger than the value there (MongoDB's order of types), or missing */
	$min?: { [path: string]: unknown };
	$max?: { [path: string]: unknown };
	/** moves a field's value to another path */
	$rename?: { [path: string]: string };
	/** like $set, only when an upsert inserts the document */
	$setOnInsert?: { [path: string]: unknown };
	/** appends to an array (created when missing) */
	$push?: { [path: string]: PushValue };
	/** appends what the array does not hold yet */
	$addToSet?: { [path: string]: unknown | { $each: unknown[] } };
	/** removes the elements equal to a value, or matching a condition */
	$pull?: { [path: string]: unknown };
}

export interface UpdateOptions {
	/** Insert a document when none matches: the filter's equality fields with the update applied. */
	upsert?: boolean;
}

export interface UpdateResult {
	acknowledged: true;
	matchedCount: number;
	/** documents the update actually changed */
	modifiedCount: number;
	upsertedCount: number;
	/** _id of the inserted document, or null */
	upsertedId: any;
}

export interface IndexDescription {
	v: number;
	key: { [field: string]: 1 | -1 };
	name: string;
	unique?: true;
}

export interface CollectionInfo {
	name: string;
	type: 'collection';
	options?: Record<string, unknown>;
	info?: { readOnly: boolean };
	idIndex?: IndexDescription;
}

export interface WriteError {
	index: number;
	code: number;
	errmsg: string;
	keyValue?: Record<string, unknown>;
}

/** insertMany failures; `insertedCount`/`insertedIds` report what was inserted anyway. */
export declare class MongoBulkWriteError extends MongoServerError {
	writeErrors: WriteError[];
	insertedCount: number;
	insertedIds: { [index: number]: any };
	result: { insertedCount: number; insertedIds: { [index: number]: any } };
}

/** Documents matching a query, read lazily in chunks. */
export type SortDirection = 1 | -1 | 'asc' | 'desc' | 'ascending' | 'descending';
export type Sort = string | { [path: string]: SortDirection } | [string, SortDirection] | [string, SortDirection][] | Map<string, SortDirection>;

export declare class FindCursor<T = Document> implements AsyncIterable<T> {
	/**
	 * Before reading only. Values compare as in MongoDB: types in its order, an array by its
	 * smallest element ascending (largest descending), a missing field as null. By one indexed
	 * field (or _id) the index gives the order; otherwise the documents are sorted in memory,
	 * through temporary files when they do not fit (as in MongoDB 7).
	 */
	sort(spec: Sort): this;
	/** Before reading only. false: a sort that does not fit in memory fails with error 292. */
	allowDiskUse(allow?: boolean): this;
	/** Before reading only. */
	skip(n: number): this;
	/** Before reading only; 0 = no limit, a negative limit counts like its absolute value. */
	limit(n: number): this;
	next(): Promise<T | null>;
	hasNext(): Promise<boolean>;
	toArray(): Promise<T[]>;
	close(): Promise<void>;
	[Symbol.asyncIterator](): AsyncIterator<T>;
}

/** Field paths ("$a.b"), $$ROOT, { $literal: v }, documents and arrays of expressions, constants. */
export type Expression = unknown;

export type Accumulator =
	| { $sum: Expression }
	| { $avg: Expression }
	| { $min: Expression }
	| { $max: Expression }
	| { $first: Expression }
	| { $last: Expression }
	| { $push: Expression }
	| { $addToSet: Expression }
	| { $count: Record<string, never> };

export type PipelineStage =
	| { $match: Filter<Document> }
	| { $group: { _id: Expression; [field: string]: Accumulator | Expression } }
	| { $count: string }
	/** by fields, 1 ascending or -1 descending; right after the first $match (or first) it uses indexes like find */
	| { $sort: { [path: string]: 1 | -1 } }
	| { $skip: number }
	| { $limit: number }
	/** fields kept (1, true) or left out (0, false), or computed from an Expression */
	| { $project: { [path: string]: 0 | 1 | boolean | Expression | { [path: string]: unknown } } }
	/** one document per element of the array at the path ("$tags") */
	| { $unwind: string | { path: string; includeArrayIndex?: string; preserveNullAndEmptyArrays?: boolean } };

/** Aggregation results, read lazily; `sort`, `skip`, `limit` act on the results. */
export declare class AggregationCursor<T = Document> extends FindCursor<T> {}

export declare class MostikClient {
	/** `path` is where the database lives: a directory, or a single file if it has an extension. */
	constructor(path: string, options?: MostikClientOptions);
	static connect(path: string, options?: MostikClientOptions): Promise<MostikClient>;
	connect(): Promise<MostikClient>;
	db(name?: string): Db;
	/** Waits for pending writes, then releases the database file. */
	close(): Promise<void>;
}

export declare class Db {
	/**
	 * Renames a collection, in one catalog change whatever its size; resolves to the renamed
	 * collection. Codes: 26 no source, 48 target exists (unless `dropTarget`), 20 same name.
	 */
	renameCollection<T extends Document = Document>(from: string, to: string, options?: { dropTarget?: boolean }): Promise<Collection<T>>;
	/** Drops every collection of the database; resolves to true. */
	dropDatabase(): Promise<boolean>;
	/** Collections with documents or indexes, by name; `nameOnly`: only { name, type }. */
	listCollections(filter?: Filter<CollectionInfo>, options?: { nameOnly?: boolean }): FindCursor<CollectionInfo>;
	readonly databaseName: string;
	collection<T extends Document = Document>(name: string): Collection<T>;
}

export declare class Collection<T extends Document = Document> {
	readonly collectionName: string;
	readonly dbName: string;
	readonly namespace: string;
	/** Adds an ObjectId `_id` to `doc` when missing; rejects with code 11000 on a duplicate `_id`. */
	insertOne(doc: T): Promise<InsertOneResult>;
	/**
	 * Inserts all documents (adding missing `_id`s). `ordered` (default true) stops at the first
	 * failure; otherwise every insertable document is inserted. Failures: MongoBulkWriteError.
	 */
	insertMany(docs: T[], options?: { ordered?: boolean }): Promise<InsertManyResult>;
	/** First matching document, or null. */
	findOne(filter?: Filter<T>): Promise<WithId<T> | null>;
	/** Every matching document, lazily. */
	find(filter?: Filter<T>, options?: { skip?: number; limit?: number; sort?: Sort; allowDiskUse?: boolean }): FindCursor<WithId<T>>;
	/** Number of matching documents, after `skip`, at most `limit`. `{}` reads a counter. */
	countDocuments(filter?: Filter<T>, options?: { skip?: number; limit?: number }): Promise<number>;
	/** Deletes the first matching document. */
	deleteOne(filter?: Filter<T>): Promise<DeleteResult>;
	/** Deletes every matching document; `{}` empties the collection. */
	deleteMany(filter?: Filter<T>): Promise<DeleteResult>;
	/** Updates the first matching document. */
	updateOne(filter: Filter<T>, update: UpdateFilter, options?: UpdateOptions): Promise<UpdateResult>;
	/** Updates every matching document. */
	updateMany(filter: Filter<T>, update: UpdateFilter, options?: UpdateOptions): Promise<UpdateResult>;
	/**
	 * Runs a pipeline of $match, $group, $count, $sort, $skip, $limit, $project and $unwind
	 * stages. A leading $match and a $sort after it use indexes like find; $match then $count
	 * counts without reading documents.
	 */
	aggregate<R = Document>(pipeline?: PipelineStage[], options?: { allowDiskUse?: boolean }): AggregationCursor<R>;
	/** Drops an index by name ("email_1") or key pattern ({ email: 1 }); code 27 if there is none. */
	dropIndex(index: string | { [field: string]: 1 | -1 }): Promise<{ nIndexesWas: number; ok: 1 }>;
	/** The indexes, _id first; rejects with code 26 if the collection does not exist. */
	listIndexes(): FindCursor<IndexDescription>;
	/** Removes the collection with its documents and indexes; false if it did not exist. */
	drop(): Promise<boolean>;
	/**
	 * Index on one field or several ({ a: 1, b: -1 }: filter on a, sort by b, through it);
	 * `unique` refuses a second document with the same value (code 11000). Resolves to its
	 * name, e.g. "email_1".
	 */
	createIndex(keys: string | { [field: string]: 1 | -1 }, options?: { name?: string; unique?: boolean }): Promise<string>;
}
