'use strict';
// Reads are generators, run by async methods. A long one gives the event loop a turn now and
// then, and waits for work done off the main thread, while staying a plain generator: it yields
// a Wait, which the async side (`step`, `finish`) waits for before resuming it. Generators that
// read others pass Waits on (`yield value` when `value instanceof Wait`).

/** Something to wait for; the generator that yielded it finds the outcome on it. */
class Wait {
	constructor(promise) {
		this.promise = promise;
		this.value = undefined;
		this.failed = false;
		this.error = undefined;
	}
}

/** The outcome of `promise`, inside a generator: `const value = yield* wait(promise)`. */
function* wait(promise) {
	const token = new Wait(promise);
	yield token;
	if (token.failed) throw token.error;
	return token.value;
}

// The event loop is held from `stretchStart` on until it turns (a pending immediate runs).
const SLICE_MS = 10;
let stretchStart = 0;
let turning = false;

function held() {
	const now = performance.now();
	if (!turning) {
		turning = true;
		stretchStart = now;
		setImmediate(() => {
			turning = false;
		});
	}
	return now - stretchStart;
}

const turn = () => new Promise((resolve) => setImmediate(resolve));

/** A turn for the event loop when it has been held for long: `yield* breathe()`. */
function* breathe() {
	if (held() >= SLICE_MS) yield* wait(turn());
}

async function settle(token) {
	try {
		token.value = await token.promise;
	} catch (error) {
		token.failed = true;
		token.error = error;
	}
}

/**
 * The next step of `iterator` that is no Wait (those waited for on the way): the step itself
 * when no Wait came, so that what the caller does next stays in this turn (a write issued after
 * it in the same turn sees it), else a promise of it.
 */
function step(iterator) {
	const next = iterator.next();
	if (next.done || !(next.value instanceof Wait)) return next;
	return (async () => {
		let token = next.value;
		for (;;) {
			await settle(token);
			const after = iterator.next();
			if (after.done || !(after.value instanceof Wait)) return after;
			token = after.value;
		}
	})();
}

/** Runs generator `iterator` to its end, Waits waited for; resolves to what it returns. */
async function finish(iterator) {
	for (;;) {
		const next = iterator.next();
		if (next.done) return next.value;
		if (next.value instanceof Wait) await settle(next.value);
	}
}

module.exports = { Wait, wait, breathe, step, finish };
