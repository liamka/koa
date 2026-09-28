// node examples/basic/index.mjs
import { fileURLToPath } from 'node:url';
import { open } from 'mostik';

const myDB = open(fileURLToPath(new URL('./my-db', import.meta.url)), {
	compression: true,
});

await myDB.put('greeting', { someText: 'Hello, World!' });
console.log(myDB.get('greeting').someText); // 'Hello, World!'

await myDB.remove('greeting');
console.log(myDB.get('greeting')); // undefined

await myDB.close();
