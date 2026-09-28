export interface RootDatabaseOptions {
	/** Path to the database: a file if it has an extension, otherwise a directory. Omit for a temporary database. */
	path?: string;
	/** LZ4-compress values larger than `threshold` bytes (1000 when `true`). */
	compression?: boolean | { threshold?: number };
	/** Only 'msgpack' is supported. */
	encoding?: 'msgpack';
	/** Only 'ordered-binary' is supported. */
	keyEncoding?: 'ordered-binary';
	[option: string]: unknown;
}

export type Key = Key[] | string | symbol | number | boolean | bigint | null | Uint8Array;

export declare class Database<V = any, K extends Key = Key> {
	path: string;
	/** Synchronously reads the committed value for `key`, or `undefined`. */
	get(key: K): V | undefined;
	/** Queues a write; the promise resolves to `true` once the batch is durably committed. */
	put(key: K, value: V): Promise<boolean>;
	/** Queues a delete; batched with puts from the same event turn. */
	remove(key: K): Promise<boolean>;
	/** Waits for pending writes and closes the database. */
	close(): Promise<void>;
}

export declare function open<V = any, K extends Key = Key>(path: string, options?: RootDatabaseOptions): Database<V, K>;
export declare function open<V = any, K extends Key = Key>(options?: RootDatabaseOptions): Database<V, K>;
