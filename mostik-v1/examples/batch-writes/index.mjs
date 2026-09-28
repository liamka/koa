// node examples/batch-writes/index.mjs
import { fileURLToPath } from 'node:url';
import { open } from 'mostik';

const db = open(fileURLToPath(new URL('./my-db', import.meta.url)));

// every put/remove made in the same event turn goes into one transaction
// and they all share one promise, which resolves once the data is on disk
const first = db.put('counter', 1);
db.put('status', 'active');
db.remove('stale');
const last = db.put('counter', 2);

console.log(first === last); // true
console.log(db.get('counter')); // undefined: nothing is committed yet
await last;
console.log(db.get('counter')); // 2: the last write to a key wins

await db.close();
