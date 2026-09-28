//! Reads worked out off the JS thread, over document bytes: counts, the first documents of a
//! sort, whole sorts (through temporary files when they do not fit in memory) and $group. Each
//! is a task on libuv's threads; the documents of a collection, or those index entries point
//! to, are read several leaves at a time.

use std::cell::Cell;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use mostik_engine::PARALLEL_LEAVES;
use napi::bindgen_prelude::*;
use napi::Task;
use napi_derive::napi;

use crate::filter::{self, encode_sort_keys, Filter, SortValue};
use crate::{decode_value, document_key, to_io, to_napi, NativeEnv, Store, RAW};

/// Where documents come from: key ranges of the collection, or of an index whose entries point
/// to them (`docs`: the documents' key prefix).
struct Source {
    ranges: Vec<(Vec<u8>, Vec<u8>)>,
    docs: Option<Vec<u8>>,
}

impl Source {
    fn new(ranges: Vec<Buffer>, docs: Option<Buffer>) -> Result<Source> {
        if ranges.len() % 2 != 0 {
            return Err(Error::from_reason("mostik: ranges come as start, end"));
        }
        Ok(Source { ranges: ranges.chunks(2).map(|r| (r[0].to_vec(), r[1].to_vec())).collect(), docs: docs.map(|d| d.to_vec()) })
    }

    /// Calls `pick(document key, document)` for each document, several leaves at once; `sink`
    /// gets what it returns in batches, in key order, until it returns false.
    fn read<T: Send>(&self, store: &Store, pick: &(dyn Fn(&[u8], &[u8]) -> io::Result<Option<T>> + Sync), mut sink: impl FnMut(Vec<T>) -> bool) -> io::Result<()> {
        let snapshot = store.snapshot();
        let with_value = |key: &[u8], stored: &[u8]| -> io::Result<Option<T>> {
            match stored.split_first() {
                Some((&RAW, value)) => pick(key, value),
                _ => pick(key, &decode_value(stored).map_err(to_io)?),
            }
        };
        let map = |key: &[u8], stored: &[u8]| -> io::Result<Option<T>> {
            let Some(docs) = &self.docs else { return with_value(key, stored) };
            let doc = document_key(docs, key, stored).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "mostik: malformed index entry"))?;
            // an entry whose document is gone (deleted meanwhile) counts for nothing
            Ok(snapshot.get_with(&doc, |stored| with_value(&doc, stored))?.transpose()?.flatten())
        };
        // index entries cost a document read each: many leaves at once from the start
        let first = if self.docs.is_some() { PARALLEL_LEAVES } else { 1 };
        let go_on = Cell::new(true);
        for (start, end) in &self.ranges {
            snapshot.par_range(start, end, first, &map, |found, _| {
                go_on.set(sink(found));
                go_on.get()
            })?;
            if !go_on.get() {
                break;
            }
        }
        Ok(())
    }
}

fn paths_of(paths: Vec<Vec<String>>) -> Vec<Vec<Vec<u8>>> {
    paths.into_iter().map(|segments| segments.into_iter().map(String::into_bytes).collect()).collect()
}

// ---- counts ----

pub struct Count {
    store: Arc<Store>,
    source: Source,
    filter: Filter,
    limit: u64,
}

impl Task for Count {
    type Output = f64;
    type JsValue = f64;

    fn compute(&mut self) -> Result<f64> {
        let filter = &self.filter;
        let pick = |_: &[u8], value: &[u8]| -> io::Result<Option<()>> { Ok(filter.matches(value).then_some(())) };
        let mut count = 0u64;
        let limit = self.limit;
        self.source
            .read(&self.store, &pick, |found| {
                count += found.len() as u64;
                count < limit
            })
            .map_err(to_napi)?;
        Ok(count.min(limit) as f64)
    }

    fn resolve(&mut self, _env: napi::Env, count: f64) -> Result<f64> {
        Ok(count)
    }
}

/// Keys in all of some ranges, counted off the JS thread.
pub struct CountKeys {
    store: Arc<Store>,
    ranges: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Task for CountKeys {
    type Output = f64;
    type JsValue = f64;

    fn compute(&mut self) -> Result<f64> {
        let snapshot = self.store.snapshot();
        let mut count = 0u64;
        for (start, end) in &self.ranges {
            snapshot
                .range(start, end, |_, _| {
                    count += 1;
                    true
                })
                .map_err(to_napi)?;
        }
        Ok(count as f64)
    }

    fn resolve(&mut self, _env: napi::Env, count: f64) -> Result<f64> {
        Ok(count)
    }
}

// ---- the first documents of a sort ----

/// top_keys' answer: the first documents' keys in order, and those it could not place.
#[napi(object)]
pub struct TopKeys {
    pub top: Vec<Buffer>,
    pub unsure: Vec<Buffer>,
}

pub struct Top {
    store: Arc<Store>,
    source: Source,
    filter: Filter,
    paths: Vec<(Vec<Vec<u8>>, bool)>,
    wanted: usize,
    max_unsure: usize,
}

// top_keys calls, and each thread's copy of a call's bar: (call, version, bar)
static TOP_CALLS: AtomicU64 = AtomicU64::new(0);
thread_local! {
    static TOP_BAR: std::cell::RefCell<(u64, u64, Option<Vec<SortValue>>)> = const { std::cell::RefCell::new((0, 0, None)) };
}

enum Found {
    Keyed(Vec<SortValue>, Vec<u8>),
    Unsure(Vec<u8>),
}

impl Task for Top {
    type Output = Option<(Vec<Vec<u8>>, Vec<Vec<u8>>)>;
    type JsValue = Option<TopKeys>;

    fn compute(&mut self) -> Result<Self::Output> {
        let (filter, paths, wanted) = (&self.filter, &self.paths, self.wanted);
        let order = |a: &[SortValue], b: &[SortValue]| {
            for (i, (x, y)) in a.iter().zip(b).enumerate() {
                let o = x.cmp(y);
                let o = if paths[i].1 { o.reverse() } else { o };
                if o.is_ne() {
                    return o;
                }
            }
            std::cmp::Ordering::Equal
        };
        // the last of the first `wanted` so far: a document not before it cannot make them (it
        // comes later, so it loses a tie). Each thread keeps a copy, read again when `version`
        // says it changed.
        let bar: RwLock<Option<Vec<SortValue>>> = RwLock::new(None);
        let version = AtomicU64::new(0);
        let call = TOP_CALLS.fetch_add(1, Ordering::Relaxed) + 1;
        let pick = |key: &[u8], value: &[u8]| -> io::Result<Option<Found>> {
            if !filter.matches(value) {
                return Ok(None);
            }
            Ok(Some(match filter.sort_keys(value, paths) {
                None => Found::Unsure(key.to_vec()),
                Some(keys) => {
                    let seen = version.load(Ordering::Acquire);
                    let beaten = seen > 0
                        && TOP_BAR.with(|copy| {
                            let mut copy = copy.borrow_mut();
                            if copy.0 != call || copy.1 != seen {
                                *copy = (call, seen, bar.read().unwrap().clone());
                            }
                            copy.2.as_ref().is_some_and(|bar| order(&keys, bar).is_ge())
                        });
                    if beaten {
                        return Ok(None);
                    }
                    Found::Keyed(keys, key.to_vec())
                }
            }))
        };
        let mut kept: Vec<(Vec<SortValue>, u64, Vec<u8>)> = Vec::new();
        let mut unsure = Vec::new();
        let mut seq = 0u64;
        let mut overflow = false;
        self.source
            .read(&self.store, &pick, |found| {
                for item in found {
                    match item {
                        Found::Unsure(key) => unsure.push(key),
                        Found::Keyed(keys, key) => {
                            kept.push((keys, seq, key));
                            seq += 1;
                        }
                    }
                }
                // cut to the first `wanted` now and then, and raise the bar
                if kept.len() >= 2 * wanted + 1024 {
                    kept.sort_by(|a, b| order(&a.0, &b.0).then(a.1.cmp(&b.1)));
                    kept.truncate(wanted);
                    *bar.write().unwrap() = kept.last().map(|k| k.0.clone());
                    version.fetch_add(1, Ordering::Release);
                }
                overflow = unsure.len() > self.max_unsure;
                !overflow
            })
            .map_err(to_napi)?;
        if overflow {
            return Ok(None);
        }
        kept.sort_by(|a, b| order(&a.0, &b.0).then(a.1.cmp(&b.1)));
        kept.truncate(wanted);
        Ok(Some((kept.into_iter().map(|k| k.2).collect(), unsure)))
    }

    fn resolve(&mut self, _env: napi::Env, found: Self::Output) -> Result<Option<TopKeys>> {
        Ok(found.map(|(top, unsure)| TopKeys { top: top.into_iter().map(Into::into).collect(), unsure: unsure.into_iter().map(Into::into).collect() }))
    }
}

// ---- whole sorts ----

/// A document to sort: the bytes its keys order as (the arrival order last, so none tie), its
/// key, and the document.
struct Record {
    order: Vec<u8>,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Record {
    fn size(&self) -> usize {
        96 + self.order.len() + self.key.len() + self.value.len()
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}
impl Eq for Record {}
impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Record {
    // reversed: BinaryHeap pops the greatest, the merge wants the smallest
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.order.cmp(&self.order)
    }
}

/// Memory a sort holds before it writes a sorted run to a temporary file, and memory $group
/// holds before it leaves the grouping to the JS side (which goes through temporary files).
static SORT_MEMORY: AtomicUsize = AtomicUsize::new(32 << 20);
static GROUP_MEMORY: AtomicUsize = AtomicUsize::new(32 << 20);

/// Sets the memory sorts and groups hold here (tests use little, to see temporary files).
#[napi]
pub fn set_memory_limits(sort: u32, group: u32) {
    SORT_MEMORY.store(sort as usize, Ordering::Relaxed);
    GROUP_MEMORY.store(group as usize, Ordering::Relaxed);
}
static RUNS: AtomicU64 = AtomicU64::new(0);

/// A temporary file of sorted records, removed when dropped.
struct Run {
    path: String,
    reader: Option<BufReader<File>>,
}

impl Run {
    fn write(base: &str, records: &[Record]) -> io::Result<Run> {
        let path = format!("{base}-sort-{}-{}", std::process::id(), RUNS.fetch_add(1, Ordering::Relaxed));
        let mut out = BufWriter::with_capacity(1 << 20, File::create(&path)?);
        for record in records {
            for part in [&record.order, &record.key, &record.value] {
                out.write_all(&(part.len() as u32).to_le_bytes())?;
                out.write_all(part)?;
            }
        }
        out.flush()?;
        drop(out);
        let reader = Some(BufReader::with_capacity(256 << 10, File::open(&path)?));
        Ok(Run { path, reader })
    }

    fn next(&mut self) -> io::Result<Option<Record>> {
        let Some(reader) = self.reader.as_mut() else { return Ok(None) };
        let mut parts: [Vec<u8>; 3] = Default::default();
        for (i, part) in parts.iter_mut().enumerate() {
            let mut len = [0u8; 4];
            match reader.read_exact(&mut len) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && i == 0 => {
                    self.reader = None;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
            part.resize(u32::from_le_bytes(len) as usize, 0);
            reader.read_exact(part)?;
        }
        let [order, key, value] = parts;
        Ok(Some(Record { order, key, value }))
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        self.reader = None;
        let _ = std::fs::remove_file(&self.path);
    }
}

pub struct Sort {
    store: Arc<Store>,
    source: Source,
    filter: Filter,
    paths: Vec<(Vec<Vec<u8>>, bool)>,
    wanted: usize,
    base: String,
    allow_disk: bool,
}

/// Why a sort gave up: a document it cannot place (for the caller to sort), or memory it may
/// not exceed.
enum Stop {
    Unsure,
    Memory,
}

impl Task for Sort {
    type Output = Option<SortedDocs>;
    type JsValue = Option<SortedDocs>;

    fn compute(&mut self) -> Result<Option<SortedDocs>> {
        let (filter, paths) = (&self.filter, &self.paths);
        let descending: Vec<bool> = paths.iter().map(|p| p.1).collect();
        let unsure = AtomicBool::new(false);
        let pick = |key: &[u8], value: &[u8]| -> io::Result<Option<(Vec<u8>, Vec<u8>, Vec<u8>)>> {
            if unsure.load(Ordering::Relaxed) || !filter.matches(value) {
                return Ok(None);
            }
            let Some(keys) = filter.sort_keys(value, paths) else {
                unsure.store(true, Ordering::Relaxed);
                return Ok(None);
            };
            let mut order = Vec::with_capacity(32);
            encode_sort_keys(&keys, &descending, &mut order);
            Ok(Some((order, key.to_vec(), value.to_vec())))
        };
        let mut kept: Vec<Record> = Vec::new();
        let mut held = 0usize;
        let mut runs: Vec<Run> = Vec::new();
        let mut seq = 0u64;
        let mut stop = None;
        let wanted = self.wanted;
        let base = self.base.clone();
        let allow_disk = self.allow_disk;
        let limit = SORT_MEMORY.load(Ordering::Relaxed);
        let mut error = None;
        self.source
            .read(&self.store, &pick, |found| {
                for (mut order, key, value) in found {
                    order.extend_from_slice(&seq.to_be_bytes());
                    seq += 1;
                    let record = Record { order, key, value };
                    held += record.size();
                    kept.push(record);
                }
                if unsure.load(Ordering::Relaxed) {
                    stop = Some(Stop::Unsure);
                    return false;
                }
                if held > limit {
                    // Record orders reversed for the heap: sort by the bytes themselves
                    kept.sort_unstable_by(|a, b| a.order.cmp(&b.order));
                    if wanted > 0 {
                        kept.truncate(wanted);
                    }
                    held = kept.iter().map(Record::size).sum();
                    if held > limit / 2 {
                        if !allow_disk {
                            stop = Some(Stop::Memory);
                            return false;
                        }
                        match Run::write(&base, &kept) {
                            Ok(run) => runs.push(run),
                            Err(e) => {
                                error = Some(e);
                                return false;
                            }
                        }
                        kept.clear();
                        held = 0;
                    }
                }
                true
            })
            .map_err(to_napi)?;
        if let Some(e) = error {
            return Err(to_napi(e));
        }
        match stop {
            Some(Stop::Unsure) => return Ok(None),
            Some(Stop::Memory) => return Err(Error::from_reason("mostik: sort memory exceeded")),
            None => {}
        }
        kept.sort_unstable_by(|a, b| a.order.cmp(&b.order));
        if wanted > 0 {
            kept.truncate(wanted);
        }
        SortedDocs::new(kept, runs, wanted).map(Some).map_err(to_napi)
    }

    fn resolve(&mut self, _env: napi::Env, sorted: Option<SortedDocs>) -> Result<Option<SortedDocs>> {
        Ok(sorted)
    }
}

/// The documents of a sort, in order, read a chunk at a time like a cursor's.
#[napi]
pub struct SortedDocs {
    /// sorted runs on disk, and the last run in memory (reversed: popped from the end)
    runs: Vec<Run>,
    tail: Vec<Record>,
    heap: BinaryHeap<(Record, usize)>,
    left: usize,
    pending: Option<Record>,
}

impl SortedDocs {
    fn new(mut tail: Vec<Record>, mut runs: Vec<Run>, wanted: usize) -> io::Result<SortedDocs> {
        tail.reverse();
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (i, run) in runs.iter_mut().enumerate() {
            if let Some(record) = run.next()? {
                heap.push((record, i));
            }
        }
        Ok(SortedDocs { runs, tail, heap, left: if wanted > 0 { wanted } else { usize::MAX }, pending: None })
    }

    /// The next record in order, from the runs on disk or the one in memory.
    fn next_record(&mut self) -> io::Result<Option<Record>> {
        if let Some(record) = self.pending.take() {
            return Ok(Some(record));
        }
        if self.left == 0 {
            return Ok(None);
        }
        let from_disk = match (self.heap.peek(), self.tail.last()) {
            (Some((a, _)), Some(b)) => a.order < b.order,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return Ok(None),
        };
        let record = if from_disk {
            let (record, run) = self.heap.pop().unwrap();
            if let Some(next) = self.runs[run].next()? {
                self.heap.push((next, run));
            }
            record
        } else {
            self.tail.pop().unwrap()
        };
        self.left -= 1;
        Ok(Some(record))
    }
}

#[napi]
impl SortedDocs {
    /// Writes the next documents into `out` as `[key len u32][key][value len u32][value]`,
    /// values decoded; returns the bytes written, 0 at the end, or minus the bytes the next
    /// one needs when it does not fit an empty `out`.
    #[napi]
    pub fn read(&mut self, mut out: BufferSlice) -> Result<i64> {
        let out: &mut [u8] = &mut out;
        let mut used = 0usize;
        while let Some(record) = self.next_record().map_err(to_napi)? {
            let value = &record.value;
            let need = 8 + record.key.len() + value.len();
            if used + need > out.len() {
                self.pending = Some(record);
                return Ok(if used == 0 { -(need as i64) } else { used as i64 });
            }
            out[used..used + 4].copy_from_slice(&(record.key.len() as u32).to_le_bytes());
            out[used + 4..used + 4 + record.key.len()].copy_from_slice(&record.key);
            let at = used + 4 + record.key.len();
            out[at..at + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
            out[at + 4..at + 4 + value.len()].copy_from_slice(value);
            used += need;
        }
        Ok(used as i64)
    }

    /// Lets go of the runs (their files are removed) before the end.
    #[napi]
    pub fn close(&mut self) {
        self.runs.clear();
        self.tail.clear();
        self.heap.clear();
        self.pending = None;
        self.left = 0;
    }
}

// ---- $group ----

/// Accumulators $group can have here: an op, and the field path it reads (none: a constant).
const ACC_SUM: u32 = 1;
const ACC_AVG: u32 = 2;
const ACC_MIN: u32 = 3;
const ACC_MAX: u32 = 4;
const ACC_COUNT: u32 = 5;
const ACC_FIRST: u32 = 6;
const ACC_LAST: u32 = 7;

/// One group's state per accumulator.
enum State {
    Sum(f64),
    Avg(f64, u64),
    Pick(Option<(SortValue, Vec<u8>)>),
    Count(u64),
    First(Option<Vec<u8>>),
    Last(Option<Vec<u8>>),
}

struct Group {
    id: Vec<u8>,
    states: Vec<State>,
}


pub struct GroupTask {
    store: Arc<Store>,
    source: Source,
    filter: Filter,
    /// the path of _id (none: every document in one group)
    id: Option<Vec<Vec<u8>>>,
    /// per accumulator: op, the path it reads, or the constant it sums
    accumulators: Vec<(u32, Option<Vec<Vec<u8>>>, f64)>,
}

/// One document's part: its _id's sort bytes and bytes, and each accumulator's value (None:
/// missing).
type Part = (Vec<u8>, Vec<u8>, Vec<Option<Vec<u8>>>);

impl Task for GroupTask {
    type Output = Option<Vec<Vec<u8>>>;
    type JsValue = Option<Vec<Buffer>>;

    fn compute(&mut self) -> Result<Self::Output> {
        let (filter, id, accumulators) = (&self.filter, &self.id, &self.accumulators);
        let give_up = AtomicBool::new(false);
        // documents are read several leaves at once; their values are summed in order, as the
        // JS side does, so that sums come out the same to the last bit
        let pick = |_: &[u8], value: &[u8]| -> io::Result<Option<Part>> {
            if give_up.load(Ordering::Relaxed) || !filter.matches(value) {
                return Ok(None);
            }
            let (key, bytes) = match id {
                None => (Vec::new(), vec![0xc0]),
                Some(path) => match filter.plain_path(value, path) {
                    None => {
                        give_up.store(true, Ordering::Relaxed);
                        return Ok(None);
                    }
                    Some(None) => (vec![0x20], vec![0xc0]),
                    Some(Some(v)) => match filter::plain_sort_value(v) {
                        // an _id of a kind grouped on the JS side (documents, arrays...)
                        None => {
                            give_up.store(true, Ordering::Relaxed);
                            return Ok(None);
                        }
                        Some(sort) => {
                            let mut key = Vec::with_capacity(16);
                            sort.encode(&mut key);
                            (key, v.to_vec())
                        }
                    },
                },
            };
            let mut values = Vec::with_capacity(accumulators.len());
            for (_, path, _) in accumulators {
                values.push(match path {
                    None => None,
                    Some(path) => match filter.plain_path(value, path) {
                        None => {
                            give_up.store(true, Ordering::Relaxed);
                            return Ok(None);
                        }
                        // documents, arrays, binary, 64-bit integers: grouped on the JS side
                        Some(Some(v)) if v.first() != Some(&0xc0) && filter::plain_sort_value(v).is_none() => {
                            give_up.store(true, Ordering::Relaxed);
                            return Ok(None);
                        }
                        Some(v) => v.map(<[u8]>::to_vec),
                    },
                });
            }
            Ok(Some((key, bytes, values)))
        };
        let mut groups: Vec<Group> = Vec::new();
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut held = 0usize;
        let limit = GROUP_MEMORY.load(Ordering::Relaxed);
        let mut failed = false;
        self.source
            .read(&self.store, &pick, |found| {
                for (key, bytes, values) in found {
                    let at = match index.get(&key) {
                        Some(&at) => at,
                        None => {
                            held += 160 + 2 * key.len() + bytes.len() + 48 * accumulators.len();
                            index.insert(key, groups.len());
                            groups.push(Group { id: bytes, states: accumulators.iter().map(|(op, _, _)| initial(*op)).collect() });
                            groups.len() - 1
                        }
                    };
                    let group = &mut groups[at];
                    for (((op, path, constant), state), value) in accumulators.iter().zip(&mut group.states).zip(values) {
                        if !accumulate(*op, path.is_some(), *constant, state, value) {
                            failed = true;
                        }
                    }
                }
                if give_up.load(Ordering::Relaxed) || failed || held > limit {
                    failed = true;
                    return false;
                }
                true
            })
            .map_err(to_napi)?;
        if failed {
            return Ok(None);
        }
        Ok(Some(groups.into_iter().map(|g| encode_group(g)).collect()))
    }

    fn resolve(&mut self, _env: napi::Env, groups: Self::Output) -> Result<Option<Vec<Buffer>>> {
        Ok(groups.map(|groups| groups.into_iter().map(Into::into).collect()))
    }
}

fn initial(op: u32) -> State {
    match op {
        ACC_SUM => State::Sum(0.0),
        ACC_AVG => State::Avg(0.0, 0),
        ACC_MIN | ACC_MAX => State::Pick(None),
        ACC_COUNT => State::Count(0),
        ACC_FIRST => State::First(None),
        ACC_LAST => State::Last(None),
        _ => unreachable!("checked by group_docs"),
    }
}

/// Adds one document's value (None: missing) to a state, as lib/aggregate.js accumulate does;
/// false for a value it cannot order here.
fn accumulate(op: u32, from_path: bool, constant: f64, state: &mut State, value: Option<Vec<u8>>) -> bool {
    match state {
        State::Sum(sum) => {
            if !from_path {
                *sum += constant;
            } else if let Some(n) = value.as_deref().and_then(filter::js_number) {
                *sum += n;
            }
        }
        State::Avg(sum, n) => {
            if let Some(v) = value.as_deref().and_then(filter::js_number) {
                *sum += v;
                *n += 1;
            }
        }
        State::Count(n) => *n += 1,
        State::Pick(best) => {
            // nulls and missing values do not count
            let Some(value) = value.filter(|v| v.first() != Some(&0xc0)) else { return true };
            let Some(sort) = filter::plain_sort_value(&value) else { return false };
            let better = match best {
                None => true,
                Some((current, _)) => {
                    if op == ACC_MIN {
                        sort < *current
                    } else {
                        sort > *current
                    }
                }
            };
            if better {
                *best = Some((sort, value));
            }
        }
        State::First(first) => {
            if first.is_none() {
                *first = Some(value.unwrap_or_else(|| vec![0xc0]));
            }
        }
        State::Last(last) => *last = Some(value.unwrap_or_else(|| vec![0xc0])),
    }
    true
}

/// A group as a msgpack array: its _id, then each accumulator's result.
fn encode_group(group: Group) -> Vec<u8> {
    let count = 1 + group.states.len();
    let mut out = Vec::with_capacity(16 + group.id.len() + 12 * group.states.len());
    if count < 16 {
        out.push(0x90 | count as u8);
    } else {
        out.push(0xdc);
        out.extend_from_slice(&(count as u16).to_be_bytes());
    }
    out.extend_from_slice(&group.id);
    let number = |out: &mut Vec<u8>, n: f64| {
        out.push(0xcb);
        out.extend_from_slice(&n.to_be_bytes());
    };
    for state in group.states {
        match state {
            State::Sum(sum) => number(&mut out, sum),
            State::Avg(sum, n) => {
                if n == 0 {
                    out.push(0xc0);
                } else {
                    number(&mut out, sum / n as f64);
                }
            }
            State::Count(n) => number(&mut out, n as f64),
            State::Pick(best) => match best {
                Some((_, bytes)) => out.extend_from_slice(&bytes),
                None => out.push(0xc0),
            },
            State::First(value) | State::Last(value) => out.extend_from_slice(&value.unwrap_or_else(|| vec![0xc0])),
        }
    }
    out
}

// ---- the JS side ----

#[napi]
impl NativeEnv {
    /// Documents in `ranges` (start, end pairs; with `docs_prefix`, index entries whose
    /// documents are read) that pass `filter`, counted off the JS thread, at most `limit`
    /// (0: all).
    #[napi]
    pub fn count_task(&self, ranges: Vec<Buffer>, docs_prefix: Option<Buffer>, filter: Buffer, limit: f64) -> Result<AsyncTask<Count>> {
        Ok(AsyncTask::new(Count {
            store: self.store()?.clone(),
            source: Source::new(ranges, docs_prefix)?,
            filter: filter::parse(&filter)?,
            limit: if limit > 0.0 { limit as u64 } else { u64::MAX },
        }))
    }

    /// Keys in all of `ranges` (start, end pairs), counted off the JS thread.
    #[napi]
    pub fn count_keys_task(&self, ranges: Vec<Buffer>) -> Result<AsyncTask<CountKeys>> {
        Ok(AsyncTask::new(CountKeys { store: self.store()?.clone(), ranges: Source::new(ranges, None)?.ranges }))
    }

    /// The keys of the first `wanted` documents in `ranges` that pass `filter`, in the order of
    /// `paths` (dot paths split in segments; `descending` per path); ties in the order they are
    /// read. `unsure`: documents whose order this cannot tell (see Filter::sort_keys), for the
    /// caller to place; null when there are more than `max_unsure` of them. A document that
    /// cannot make the first `wanted` is dropped where it is read.
    #[napi]
    pub fn top_keys(&self, ranges: Vec<Buffer>, docs_prefix: Option<Buffer>, filter: Buffer, paths: Vec<Vec<String>>, descending: Vec<bool>, wanted: u32, max_unsure: u32) -> Result<AsyncTask<Top>> {
        Ok(AsyncTask::new(Top {
            store: self.store()?.clone(),
            source: Source::new(ranges, docs_prefix)?,
            filter: filter::parse(&filter)?,
            paths: paths_of(paths).into_iter().zip(descending).collect(),
            wanted: wanted.max(1) as usize,
            max_unsure: max_unsure as usize,
        }))
    }

    /// The documents in `ranges` that pass `filter`, sorted by `paths` (the first `wanted`,
    /// 0: all), read from the SortedDocs it resolves to. Past 32 MB the sorted runs go to
    /// temporary files at `base`, unless `allow_disk` is false: then it fails ("sort memory").
    /// null when a document holds a value this cannot order (the caller sorts then).
    #[napi]
    pub fn sort_docs(&self, ranges: Vec<Buffer>, docs_prefix: Option<Buffer>, filter: Buffer, paths: Vec<Vec<String>>, descending: Vec<bool>, wanted: u32, base: String, allow_disk: bool) -> Result<AsyncTask<Sort>> {
        Ok(AsyncTask::new(Sort {
            store: self.store()?.clone(),
            source: Source::new(ranges, docs_prefix)?,
            filter: filter::parse(&filter)?,
            paths: paths_of(paths).into_iter().zip(descending).collect(),
            wanted: wanted as usize,
            base,
            allow_disk,
        }))
    }

    /// $group of the documents in `ranges` that pass `filter`, by the value at `id_path` (null:
    /// one group), with `ops` (1 $sum, 2 $avg, 3 $min, 4 $max, 5 $count, 6 $first, 7 $last) of
    /// the values at `paths` (an empty path: the constant in `constants`). Resolves to one
    /// msgpack array per group ([_id, results...]) in the order groups first appear, or null
    /// when the grouping is for the JS side (arrays or documents met, too many groups).
    #[napi]
    pub fn group_docs(&self, ranges: Vec<Buffer>, docs_prefix: Option<Buffer>, filter: Buffer, id_path: Option<Vec<String>>, ops: Vec<u32>, paths: Vec<Vec<String>>, constants: Vec<f64>) -> Result<AsyncTask<GroupTask>> {
        if ops.iter().any(|op| !(ACC_SUM..=ACC_LAST).contains(op)) || paths.len() != ops.len() || constants.len() != ops.len() {
            return Err(Error::from_reason("mostik: malformed $group"));
        }
        let accumulators = ops
            .into_iter()
            .zip(paths)
            .zip(constants)
            .map(|((op, path), constant)| (op, (!path.is_empty()).then(|| path.into_iter().map(String::into_bytes).collect()), constant))
            .collect();
        Ok(AsyncTask::new(GroupTask {
            store: self.store()?.clone(),
            source: Source::new(ranges, docs_prefix)?,
            filter: filter::parse(&filter)?,
            id: id_path.map(|segments| segments.into_iter().map(String::into_bytes).collect()),
            accumulators,
        }))
    }
}
