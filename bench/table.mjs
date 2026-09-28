// The comparison as Markdown, from results/mongodb.jsonl and results/mostik.jsonl:
// node table.mjs [--all]  (--all: index builds too)
import fs from 'node:fs';
import path from 'node:path';
import { RESULTS } from './common.mjs';

const all = process.argv.includes('--all');
const read = (name) =>
	fs
		.readFileSync(path.join(RESULTS, `${name}.jsonl`), 'utf8')
		.trim()
		.split('\n')
		.map((line) => JSON.parse(line));
const [mongodb, mostik] = [read('mongodb'), read('mostik')];

const time = (ms) => (ms < 1 ? `${(ms * 1000).toFixed(ms < 0.1 ? 1 : 0)} µs` : ms < 1000 ? `${ms.toFixed(ms < 10 ? 2 : ms < 100 ? 1 : 0)} ms` : `${(ms / 1000).toFixed(2)} s`);
const size = (bytes) => (bytes >= 2 ** 30 ? `${(bytes / 2 ** 30).toFixed(2)} GB` : `${Math.round(bytes / 2 ** 20)} MB`);
const compare = (theirs, ours, better) => {
	const ratio = theirs / ours;
	const worse = better == 'faster' ? 'slower' : 'larger';
	if (ratio >= 1000) return `**over 1000× ${better}**`;
	if (ratio >= 1.05) return `**${ratio.toFixed(ratio >= 10 ? 0 : 1)}× ${better}**`;
	if (ratio <= 1 / 1.05) return `${(1 / ratio).toFixed(1 / ratio >= 10 ? 0 : 1)}× ${worse}`;
	return 'same';
};

const lines = [];
const counts = { ahead: 0, behind: 0, same: 0 };
let group = null;
for (let i = 0; i < mostik.length; i++) {
	const [a, b] = [mongodb[i], mostik[i]];
	if (a?.label !== b.label) throw new Error(`the result files differ at "${b.label}": run both with the same N`);
	// both did the same work, or the times do not compare
	if (JSON.stringify(a.result) !== JSON.stringify(b.result)) console.error(`warning: "${b.label}" returned ${a.result} on MongoDB and ${b.result} on mostik`);
	if (!all && b.label.startsWith('createIndex')) continue;
	if (b.group !== group) {
		group = b.group;
		lines.push('', `**${group}**`, '', '| Operation | MongoDB | mostik | mostik vs MongoDB |', '| --- | ---: | ---: | --- |');
	}
	const bytes = b.bytes !== undefined;
	const verdict = bytes ? compare(a.bytes, b.bytes, 'smaller') : compare(a.ms, b.ms, 'faster');
	counts[/faster|smaller/.test(verdict) ? 'ahead' : /slower|larger/.test(verdict) ? 'behind' : 'same']++;
	const label = bytes ? b.label : `\`${b.label.replace(/\|/g, '\\|')}\``;
	lines.push(`| ${label} | ${bytes ? size(a.bytes) : time(a.ms)} | ${bytes ? size(b.bytes) : time(b.ms)} | ${verdict} |`);
}
const rows = counts.ahead + counts.behind + counts.same;
console.log(`Of the ${rows} rows mostik is ahead in ${counts.ahead}, behind in ${counts.behind} and equal in ${counts.same}.`);
console.log(lines.join('\n'));
