//! Node.js binding. Keys and values arrive already encoded by the JS layer;
//! here values get a one-byte header and optional LZ4 compression.
//!
//! Index entries (keys `[entries prefix, field, value, id]`) store, as their value, the length of
//! the key's id suffix: the document key is `docs prefix + 0x00 + that suffix`, so an index
//! lookup reaches its document without a round trip through JS.

use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::sync::Arc;

mod filter;
mod query;

use filter::{Filter, Projection};
use mostik_engine::{Condition, Durability, Env as Store, Group, Op, Options, Snapshot, CACHE_BYTES, PARALLEL_LEAVES};
use napi::bindgen_prelude::*;
use napi_derive::napi;

const RAW: u8 = 0;
const LZ4: u8 = 1;

const OP_PUT: u8 = 1;
const OP_REMOVE: u8 = 2;
const OP_REQUIRE_ABSENT: u8 = 3;
const OP_END_GROUP: u8 = 4;
const OP_REQUIRE_PRESENT: u8 = 5;
const OP_REQUIRE_EQUAL: u8 = 6;
const OP_ADD: u8 = 7;
const OP_REQUIRE_UNIQUE: u8 = 8;

/// A cursor read with at least one match returns once it has visited this many entries.
const EARLY_RETURN_AFTER: usize = 1024;
/// Index entries gathered before their documents are fetched together.
const DOC_BATCH: usize = 1024;
/// ... at least, for a caller wanting fewer documents
const MIN_DOC_BATCH: usize = 16;
/// Below this many documents, fetching them in parallel costs more than it saves.
const PARALLEL_FROM: usize = 32;

/// Commits at least this large release freed memory back to the OS afterwards.
const RELEASE_AFTER_BYTES: usize = 1 << 20;

use mostik_engine::release_free_memory;

fn to_napi(e: io::Error) -> Error {
    Error::from_reason(e.to_string())
}

fn to_io(e: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.reason.clone())
}

/// `[start, end)` holding every key that extends `prefix` by at least one element.
fn prefix_range(prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    ([prefix, &[0]].concat(), [prefix, &[1]].concat())
}

/// The document key an index entry points to, or None if the entry is malformed.
fn document_key(docs_prefix: &[u8], entry_key: &[u8], stored: &[u8]) -> Option<Vec<u8>> {
    let suffix = match stored {
        [RAW, lo, hi] => u16::from_le_bytes([*lo, *hi]) as usize,
        _ => return None,
    };
    let id = entry_key.get(entry_key.len().checked_sub(suffix)?..)?;
    Some([docs_prefix, &[0], id].concat())
}

#[napi]
pub struct NativeEnv {
    store: Option<Arc<Store>>,
}

#[napi]
impl NativeEnv {
    /// `cache_bytes`: memory for cached pages (default `CACHE_BYTES`). `strict`: every commit
    /// is flushed to the disk before it resolves (default: journaled, flushed every 100 ms).
    #[napi(factory)]
    pub fn open(path: String, lock_path: String, cache_bytes: Option<f64>, strict: Option<bool>) -> Result<NativeEnv> {
        let options = Options {
            cache_bytes: cache_bytes.map_or(CACHE_BYTES, |bytes| bytes as usize),
            durability: if strict == Some(true) { Durability::Strict } else { Durability::Journal },
        };
        let store = Store::open_with(Path::new(&path), Path::new(&lock_path), options).map_err(to_napi)?;
        Ok(NativeEnv { store: Some(store) })
    }

    fn store(&self) -> Result<&Arc<Store>> {
        self.store.as_ref().ok_or_else(|| Error::from_reason("The client has been closed"))
    }

    /// Reads the value of `key[..key_len]` into `out`, reusing JS buffers instead of allocating
    /// one per call. Returns -1 when the key is missing, otherwise the value length; a length
    /// larger than `out` means nothing was written and the caller should retry with a bigger buffer.
    #[napi]
    pub fn get_into(&self, key: BufferSlice, key_len: u32, mut out: BufferSlice) -> Result<i64> {
        let key = key.get(..key_len as usize).ok_or_else(|| Error::from_reason("key_len out of range"))?;
        let read = self.store()?.get_with(key, |stored| decode_value_into(stored, &mut out)).map_err(to_napi)?;
        read.map_or(Ok(-1), |len| len.map(|len| len as i64))
    }

    /// Commits a batch of groups, see `parse_batch` for the format. Resolves to one byte per
    /// group: 1 if it was applied, 0 if one of its conditions did not hold.
    #[napi]
    pub fn commit(&self, batch: Buffer, compression_threshold: u32) -> Result<AsyncTask<Commit>> {
        Ok(AsyncTask::new(Commit {
            store: self.store()?.clone(),
            batch: batch.to_vec(),
            threshold: compression_threshold as usize,
        }))
    }

    /// Iterates entries with `start <= key < end`. With `filter` (see filter.rs), only documents
    /// that may match it are returned, so the caller never decodes the others. With
    /// `docs_prefix`, the range holds index entries and the cursor returns the documents they
    /// point to instead.
    /// `skip`: entries at the start of the range to pass over (index entries in index mode).
    /// `wanted`: how many records the caller needs at most (0: all); index mode reads that few first.
    /// `reverse`: descending key order. `projection` (see filter.rs): documents come with only
    /// those top-level fields.
    #[napi]
    pub fn cursor(
        &self,
        start: Buffer,
        end: Buffer,
        filter: Option<Buffer>,
        docs_prefix: Option<Buffer>,
        skip: Option<u32>,
        wanted: Option<u32>,
        reverse: Option<bool>,
        projection: Option<Buffer>,
    ) -> Result<Cursor> {
        Ok(Cursor {
            skip: skip.unwrap_or(0),
            store: Some(self.store()?.clone()),
            next: start.to_vec(),
            end: end.to_vec(),
            filter: filter.map(|bytes| filter::parse(&bytes).map(Arc::new)).transpose()?,
            docs_prefix: docs_prefix.map(|p| p.to_vec()),
            last: Vec::new(),
            ready: VecDeque::new(),
            scan_pace: 1,
            // a caller wanting few documents starts with a batch that small, and grows it if the
            // filter turns documents down
            doc_pace: match wanted.unwrap_or(0) as usize {
                0 => DOC_BATCH,
                n => n.clamp(MIN_DOC_BATCH, DOC_BATCH),
            },
            prefetched: None,
            reverse: reverse.unwrap_or(false),
            project: projection.map(|bytes| filter::parse_projection(&bytes).map(Arc::new)).transpose()?,
        })
    }

    /// Index lookup in one call: skips `skip` entries under `prefix[..prefix_len]` and copies the
    /// document the next one points to into `out`, all on one snapshot. Returns
    /// `2 * length + more` where `more` tells whether another entry follows (a length larger
    /// than `out`: retry with a bigger buffer), -1 when there is no such entry, -2 when the entry
    /// points at a missing document.
    #[napi]
    pub fn lookup_into(
        &self,
        prefix: BufferSlice,
        prefix_len: u32,
        docs_prefix: BufferSlice,
        skip: u32,
        mut out: BufferSlice,
    ) -> Result<i64> {
        let prefix = prefix.get(..prefix_len as usize).ok_or_else(|| Error::from_reason("prefix_len out of range"))?;
        let (start, end) = prefix_range(prefix);
        let snapshot = self.store()?.snapshot();
        let (mut remaining, mut target, mut more) = (skip, None::<Option<Vec<u8>>>, false);
        snapshot
            .range(&start, &end, |key, stored| {
                if remaining > 0 {
                    remaining -= 1;
                    return true;
                }
                if target.is_some() {
                    more = true;
                    return false;
                }
                target = Some(document_key(&docs_prefix, key, stored));
                true
            })
            .map_err(to_napi)?;
        let Some(key) = target else { return Ok(-1) };
        let key = key.ok_or_else(corrupted_value)?;
        match snapshot.get_with(&key, |stored| decode_value_into(stored, &mut out)).map_err(to_napi)? {
            Some(len) => len.map(|len| 2 * len as i64 + more as i64),
            None => Ok(-2),
        }
    }

    /// Number of distinct documents found by: index entries in `index_ranges` (start, end
    /// pairs; their documents are under `docs_prefix`), document keys in `doc_ranges` (pairs),
    /// and the document keys `doc_keys` that exist. Documents are never read.
    #[napi]
    pub fn count_distinct(&self, docs_prefix: Buffer, index_ranges: Vec<Buffer>, doc_ranges: Vec<Buffer>, doc_keys: Vec<Buffer>) -> Result<f64> {
        if index_ranges.len() % 2 != 0 || doc_ranges.len() % 2 != 0 {
            return Err(Error::from_reason("mostik: ranges come as start, end"));
        }
        let snapshot = self.store()?.snapshot();
        let mut found: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut malformed = false;
        for range in index_ranges.chunks(2) {
            snapshot
                .range(&range[0], &range[1], |key, stored| match document_key(&docs_prefix, key, stored) {
                    Some(doc) => {
                        found.insert(doc);
                        true
                    }
                    None => {
                        malformed = true;
                        false
                    }
                })
                .map_err(to_napi)?;
        }
        if malformed {
            return Err(Error::from_reason("mostik: malformed index entry"));
        }
        for range in doc_ranges.chunks(2) {
            snapshot
                .range(&range[0], &range[1], |key, _| {
                    found.insert(key.to_vec());
                    true
                })
                .map_err(to_napi)?;
        }
        for key in doc_keys {
            if snapshot.get_with(&key, |_| ()).map_err(to_napi)?.is_some() {
                found.insert(key.to_vec());
            }
        }
        Ok(found.len() as f64)
    }

    /// Counts entries in `[start, end)` without handing them to JS, stopping at `limit` (0 = no
    /// limit). With `docs_prefix` the range holds index entries and only those whose document
    /// exists are counted. With `filter`, only documents matching it (entries of a document range,
    /// or the documents index entries point to).
    #[napi]
    pub fn count_range(&self, start: Buffer, end: Buffer, docs_prefix: Option<Buffer>, limit: f64, filter: Option<Buffer>) -> Result<f64> {
        let filter = filter.map(|bytes| filter::parse(&bytes)).transpose()?;
        let snapshot = self.store()?.snapshot();
        let limit = if limit > 0.0 { limit as u64 } else { u64::MAX };
        if let (Some(filter), None) = (&filter, &docs_prefix) {
            // a scan: check the documents of several leaves at once
            let matches = |_: &[u8], stored: &[u8]| -> io::Result<Option<()>> {
                Ok(match stored.split_first() {
                    Some((&RAW, v)) => filter.matches(v).then_some(()),
                    _ => filter.matches(&decode_value(stored).map_err(to_io)?).then_some(()),
                })
            };
            let mut count = 0u64;
            snapshot
                .par_range(&start, &end, 1, &matches, |found, _| {
                    count += found.len() as u64;
                    count < limit
                })
                .map_err(to_napi)?;
            return Ok(count.min(limit) as f64);
        }
        let mut scratch = Vec::new();
        let mut accepts = |stored: &[u8]| -> io::Result<bool> {
            let Some(filter) = &filter else { return Ok(true) };
            scratch.resize(stored_len(stored).map_err(to_io)?, 0);
            decode_value_into(stored, &mut scratch).map_err(to_io)?;
            Ok(filter.matches(&scratch))
        };
        let (mut count, mut error) = (0u64, None);
        snapshot
            .range(&start, &end, |key, stored| {
                let counts = match &docs_prefix {
                    None => accepts(stored),
                    Some(docs) => match document_key(docs, key, stored) {
                        Some(doc) => snapshot.get_with(&doc, |doc| accepts(doc)).and_then(|found| found.transpose()).map(|found| found.unwrap_or(false)),
                        None => Err(io::Error::new(io::ErrorKind::InvalidData, "mostik: malformed index entry")),
                    },
                };
                match counts {
                    Ok(counts) => {
                        count += counts as u64;
                        count < limit
                    }
                    Err(e) => {
                        error = Some(e);
                        false
                    }
                }
            })
            .map_err(to_napi)?;
        if let Some(e) = error {
            return Err(to_napi(e));
        }
        Ok(count as f64)
    }

    /// Keys in all of `ranges` (start, end pairs), on one snapshot.
    #[napi]
    pub fn count_ranges(&self, ranges: Vec<Buffer>) -> Result<f64> {
        if ranges.len() % 2 != 0 {
            return Err(Error::from_reason("mostik: ranges come as start, end"));
        }
        let snapshot = self.store()?.snapshot();
        let mut count = 0u64;
        for range in ranges.chunks(2) {
            snapshot
                .range(&range[0], &range[1], |_, _| {
                    count += 1;
                    true
                })
                .map_err(to_napi)?;
        }
        Ok(count as f64)
    }

    /// Deletes every key of each range, then the keys `remove`, in one commit. `ranges` holds
    /// three buffers per range: start, end (excluded), and the key of a counter to subtract the
    /// range's deleted keys from (empty: none). Resolves to the keys deleted per range. Whole
    /// subtrees inside a range go at once: the time follows the pages, not the keys. Runs off the
    /// JS thread.
    #[napi]
    pub fn delete_ranges(&self, ranges: Vec<Buffer>, remove: Vec<Buffer>) -> Result<AsyncTask<DeleteRanges>> {
        if ranges.len() % 3 != 0 {
            return Err(Error::from_reason("mostik: ranges come as start, end, counter"));
        }
        let ranges = ranges
            .chunks(3)
            .map(|r| (r[0].to_vec(), r[1].to_vec(), Some(r[2].to_vec()).filter(|c| !c.is_empty())))
            .collect();
        Ok(AsyncTask::new(DeleteRanges {
            store: self.store()?.clone(),
            ranges,
            remove: remove.iter().map(|key| Op::Remove(key.to_vec())).collect(),
        }))
    }

    /// Writes every change so far to disk. Runs off the JS thread.
    #[napi]
    pub fn checkpoint(&self) -> Result<AsyncTask<Checkpoint>> {
        Ok(AsyncTask::new(Checkpoint { store: self.store()?.clone() }))
    }

    /// Sorts entries fed to it (spilling sorted runs to files under `temp_prefix` when large)
    /// and writes them in key order: the fast way to fill an empty key range, e.g. a new index.
    #[napi]
    /// `unique`: the entries are index entries (`[prefix][0][_id]`, value: the _id's length) and
    /// no two may share a prefix; the load fails with "mostik: duplicate key <key in hex>".
    pub fn sorted_loader(&self, temp_prefix: String, unique: Option<bool>) -> Result<SortedLoader> {
        Ok(SortedLoader {
            store: Some(self.store()?.clone()),
            unique: unique.unwrap_or(false),
            temp_prefix,
            run: Vec::new(),
            offsets: Vec::new(),
            files: Vec::new(),
        })
    }

    #[napi]
    pub fn close(&mut self) {
        self.store = None;
    }
}

/// Reads a range in chunks. Each chunk comes from the latest committed state; no snapshot is
/// held between chunks, so a long scan never keeps freed pages from being reused (which would
/// make the file grow for as long as the scan runs).
#[napi]
pub struct Cursor {
    store: Option<Arc<Store>>,
    /// first key of the next chunk
    next: Vec<u8>,
    end: Vec<u8>,
    filter: Option<Arc<Filter>>,
    /// entries still to pass over before any is returned
    skip: u32,
    /// set when the range holds index entries: return their documents
    docs_prefix: Option<Vec<u8>>,
    /// last key visited by the current chunk; reused to avoid an allocation per entry
    last: Vec<u8>,
    /// records found that did not fit in the last chunk, (key, value)
    ready: VecDeque<(Vec<u8>, Vec<u8>)>,
    /// leaves a filtered scan checks at once next time
    scan_pace: usize,
    /// entries the next read takes at most (grows up to DOC_BATCH): documents in index mode,
    /// entries otherwise
    doc_pace: usize,
    /// descending key order
    reverse: bool,
    /// documents come with only these fields
    project: Option<Arc<Projection>>,
    /// index mode: the next batch of documents, being fetched while the caller decodes the
    /// last chunk
    prefetched: Option<Prefetch>,
}

struct Prefetch {
    documents: std::sync::mpsc::Receiver<io::Result<Vec<(Vec<u8>, Vec<u8>)>>>,
    /// the last index entry of the batch, and how many it holds
    last: Vec<u8>,
    entries: usize,
}

#[napi]
impl Cursor {
    /// Fills `out` with `[key_len u32][key][value_len u32][value]` records and returns the bytes
    /// written; 0 means the range is exhausted. If the next record alone does not fit, returns
    /// minus the size it needs. In index mode, each record is a document key and document.
    #[napi]
    pub fn read(&mut self, mut out: BufferSlice) -> Result<i64> {
        let Some(store) = &self.store else { return Ok(0) };
        let snapshot = store.snapshot();
        let out: &mut [u8] = &mut out;
        let result = match self.docs_prefix.clone() {
            Some(docs) => self.read_documents(&snapshot, &docs, out),
            None => self.read_entries(&snapshot, out),
        };
        let (used, needed) = result.map_err(to_napi)?;
        if needed > 0 {
            return Ok(-(needed as i64));
        }
        if used == 0 {
            // every remaining entry was visited (and filtered out, if any)
            self.close();
        }
        Ok(used as i64)
    }

    /// Moves `next` past the last key visited.
    /// Moves the bounds past the last key visited: `next` beyond it, or when descending, `end` down to it.
    fn visited(&mut self, key: &[u8]) {
        if self.reverse {
            self.end.clear();
            self.end.extend_from_slice(key);
        } else {
            self.next.clear();
            self.next.extend_from_slice(key);
            self.next.push(0);
        }
    }

    /// Copies documents fetched earlier into `out` from `*used` on. Returns the size of the
    /// first one that does not fit, if any.
    fn drain_ready(&mut self, out: &mut [u8], used: &mut usize) -> Option<usize> {
        while let Some((key, value)) = self.ready.front() {
            let size = 8 + key.len() + value.len();
            if *used + size > out.len() {
                return Some(size);
            }
            let at = *used;
            out[at..at + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
            out[at + 4..at + 4 + key.len()].copy_from_slice(key);
            out[at + 4 + key.len()..at + 8 + key.len()].copy_from_slice(&(value.len() as u32).to_le_bytes());
            out[at + 8 + key.len()..at + size].copy_from_slice(value);
            *used += size;
            self.ready.pop_front();
        }
        None
    }

    /// Filtered or projected range: the leaves are handled several at a time, in parallel.
    fn read_filtered(&mut self, snapshot: &Snapshot, out: &mut [u8]) -> io::Result<(usize, usize)> {
        let mut used = 0;
        if let Some(size) = self.drain_ready(out, &mut used) {
            return Ok((used, if used == 0 { size } else { 0 }));
        }
        let (filter, project) = (self.filter.clone(), self.project.clone());
        let pick = |key: &[u8], stored: &[u8]| -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
            let value = match &filter {
                Some(filter) => filtered_value(stored, filter)?,
                None => Some(decode_value(stored).map_err(to_io)?),
            };
            Ok(value.map(|value| (key.to_vec(), projected(value, project.as_deref()))))
        };
        let mut needed = 0;
        let (next, end) = (std::mem::take(&mut self.next), std::mem::take(&mut self.end));
        // a scan that goes on picks up the pace it had
        let first = self.scan_pace;
        let result = snapshot.par_range(&next, &end, first, &pick, |found, last| {
            self.ready.extend(found);
            self.visited(last);
            self.scan_pace = (self.scan_pace * 2).min(PARALLEL_LEAVES);
            if let Some(size) = self.drain_ready(out, &mut used) {
                needed = if used == 0 { size } else { 0 };
                return false;
            }
            // hand over what we have instead of scanning on until the buffer is full: a caller
            // with a small limit may not need more (the next read keeps the pace)
            used == 0
        });
        if self.next.is_empty() {
            self.next = next;
        }
        self.end = end;
        result?;
        Ok((used, needed))
    }

    /// Plain range: entries as they are. Returns (bytes written, bytes needed for one entry).
    fn read_entries(&mut self, snapshot: &Snapshot, out: &mut [u8]) -> io::Result<(usize, usize)> {
        // documents to filter or cut down: several leaves at once (going forward only)
        if (self.filter.is_some() || self.project.is_some()) && self.skip == 0 && !self.reverse {
            return self.read_filtered(snapshot, out);
        }
        let (mut used, mut needed, mut seen) = (0usize, 0usize, 0usize);
        let pace = self.doc_pace;
        let (filter, project, last, skip) = (self.filter.as_deref(), self.project.as_deref(), &mut self.last, &mut self.skip);
        let mut visited = false;
        let mut error = None;
        walk(snapshot, self.reverse, &self.next, &self.end, |key, stored| {
            if *skip > 0 {
                *skip -= 1;
            } else {
                match write_record(out, &mut used, key, stored, filter, project) {
                    Ok(true) => {}
                    Ok(false) => {
                        if used == 0 {
                            needed = record_size(key, stored).unwrap_or(0);
                        }
                        return false;
                    }
                    Err(e) => {
                        error = Some(e);
                        return false;
                    }
                }
                seen += 1;
            }
            last.clear();
            last.extend_from_slice(key);
            visited = true;
            // sparse matches: hand over what we have instead of scanning on until the buffer
            // is full, a caller with a small limit may not need more (`doc_pace`: as few as
            // it wants at first)
            !(used > 0 && seen >= EARLY_RETURN_AFTER.min(pace))
        })?;
        self.doc_pace = (self.doc_pace * 2).min(DOC_BATCH);
        if let Some(e) = error {
            return Err(e);
        }
        if visited {
            let last = std::mem::take(&mut self.last);
            self.visited(&last);
            self.last = last;
        }
        Ok((used, needed))
    }

    /// Index range: the documents its entries point to. Entries are gathered a batch at a
    /// time, then their documents fetched, decompressed and filtered in parallel.
    fn read_documents(&mut self, snapshot: &Arc<Snapshot>, docs: &[u8], out: &mut [u8]) -> io::Result<(usize, usize)> {
        let (mut used, mut seen) = (0usize, 0usize);
        loop {
            // documents fetched earlier go first: fetching is the costly part, never redo it
            if let Some(size) = self.drain_ready(out, &mut used) {
                return Ok((used, if used == 0 { size } else { 0 }));
            }
            // while the batches are still growing, hand over each one: the caller may need no more
            if used > 0 && (seen >= EARLY_RETURN_AFTER || self.doc_pace < DOC_BATCH) {
                // fetch the next batch while the caller decodes this chunk
                if let Some((batch, last)) = self.next_batch(snapshot, docs)? {
                    let (send, documents) = std::sync::mpsc::sync_channel(1);
                    let (snapshot, filter, project, entries) = (snapshot.clone(), self.filter.clone(), self.project.clone(), batch.len());
                    rayon::spawn(move || {
                        let _ = send.send(fetch_documents(&snapshot, filter.as_deref(), project.as_deref(), batch));
                    });
                    self.prefetched = Some(Prefetch { documents, last, entries });
                }
                return Ok((used, 0));
            }
            let (documents, last, entries) = match self.prefetched.take() {
                Some(Prefetch { documents, last, entries }) => {
                    let fetched = documents.recv().map_err(|_| io::Error::other("mostik: fetching documents failed"))?;
                    (fetched?, last, entries)
                }
                None => match self.next_batch(snapshot, docs)? {
                    Some((batch, last)) => {
                        let entries = batch.len();
                        (fetch_documents(snapshot, self.filter.as_deref(), self.project.as_deref(), batch)?, last, entries)
                    }
                    None => return Ok((used, 0)),
                },
            };
            seen += entries;
            self.ready.extend(documents);
            self.visited(&last);
        }
    }

    /// The document keys of the next index entries (skipping those still to pass over) and
    /// the last entry; None at the end of the range.
    fn next_batch(&mut self, snapshot: &Snapshot, docs: &[u8]) -> io::Result<Option<(Vec<Vec<u8>>, Vec<u8>)>> {
        let size = self.doc_pace;
        self.doc_pace = (self.doc_pace * 2).min(DOC_BATCH);
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(size);
        let mut last = None::<Vec<u8>>;
        let mut error = None;
        let skip = &mut self.skip;
        walk(snapshot, self.reverse, &self.next, &self.end, |key, stored| {
            if *skip > 0 {
                *skip -= 1;
            } else {
                match document_key(docs, key, stored) {
                    Some(doc) => batch.push(doc),
                    None => {
                        error = Some(io::Error::new(io::ErrorKind::InvalidData, "mostik: malformed index entry"));
                        return false;
                    }
                }
            }
            last = Some(key.to_vec());
            batch.len() < size
        })?;
        if let Some(e) = error {
            return Err(e);
        }
        Ok(last.map(|last| (batch, last)))
    }

    #[napi]
    pub fn close(&mut self) {
        self.store = None;
        self.prefetched = None;
        self.ready.clear();
    }
}

pub struct DeleteRanges {
    store: Arc<Store>,
    ranges: Vec<(Vec<u8>, Vec<u8>, Option<Vec<u8>>)>,
    remove: Vec<Op>,
}

impl Task for DeleteRanges {
    type Output = Vec<f64>;
    type JsValue = Vec<f64>;

    fn compute(&mut self) -> Result<Vec<f64>> {
        let counts = self.store.delete_ranges(&self.ranges, std::mem::take(&mut self.remove)).map_err(to_napi)?;
        release_free_memory();
        Ok(counts.into_iter().map(|n| n as f64).collect())
    }

    fn resolve(&mut self, _env: napi::Env, counts: Vec<f64>) -> Result<Vec<f64>> {
        Ok(counts)
    }
}

pub struct Checkpoint {
    store: Arc<Store>,
}

impl Task for Checkpoint {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        self.store.checkpoint().map_err(to_napi)?;
        release_free_memory();
        Ok(())
    }

    fn resolve(&mut self, _env: napi::Env, _: ()) -> Result<()> {
        Ok(())
    }
}

/// In-memory run size before it is sorted and spilled to a file.
const RUN_BYTES: usize = 64 << 20;
/// Entries per commit when writing sorted entries out.
const LOAD_CHUNK: usize = 100_000;

/// External sort into the tree. Entries: `[key_len u32][key][value_len u32][value]`.
#[napi]
pub struct SortedLoader {
    store: Option<Arc<Store>>,
    unique: bool,
    temp_prefix: String,
    /// the current run: entries back to back, and where each starts
    run: Vec<u8>,
    offsets: Vec<u32>,
    files: Vec<std::path::PathBuf>,
}

fn entry_at(run: &[u8], at: usize) -> (&[u8], &[u8]) {
    let klen = u32::from_le_bytes(run[at..at + 4].try_into().unwrap()) as usize;
    let key = &run[at + 4..at + 4 + klen];
    let vat = at + 4 + klen;
    let vlen = u32::from_le_bytes(run[vat..vat + 4].try_into().unwrap()) as usize;
    (key, &run[vat + 4..vat + 4 + vlen])
}

fn sort_run(run: &[u8], offsets: &mut [u32]) {
    offsets.sort_unstable_by(|&a, &b| entry_at(run, a as usize).0.cmp(entry_at(run, b as usize).0));
}

#[napi]
impl SortedLoader {
    /// Adds a chunk of entries.
    #[napi]
    pub fn add(&mut self, chunk: BufferSlice) -> Result<()> {
        let bad = || Error::from_reason("mostik: malformed loader chunk");
        let base = self.run.len();
        let mut at = 0;
        while at < chunk.len() {
            let klen = u32::from_le_bytes(chunk.get(at..at + 4).ok_or_else(bad)?.try_into().unwrap()) as usize;
            let vat = at + 4 + klen;
            let vlen = u32::from_le_bytes(chunk.get(vat..vat + 4).ok_or_else(bad)?.try_into().unwrap()) as usize;
            if chunk.len() < vat + 4 + vlen {
                return Err(bad());
            }
            self.offsets.push((base + at) as u32);
            at = vat + 4 + vlen;
        }
        self.run.extend_from_slice(&chunk);
        if self.run.len() >= RUN_BYTES {
            self.spill().map_err(to_napi)?;
        }
        Ok(())
    }

    fn spill(&mut self) -> io::Result<()> {
        use std::io::Write;
        sort_run(&self.run, &mut self.offsets);
        let path = std::path::PathBuf::from(format!("{}{}", self.temp_prefix, self.files.len()));
        let mut out = io::BufWriter::new(std::fs::File::create(&path)?);
        for &at in &self.offsets {
            let (key, value) = entry_at(&self.run, at as usize);
            out.write_all(&(key.len() as u32).to_le_bytes())?;
            out.write_all(key)?;
            out.write_all(&(value.len() as u32).to_le_bytes())?;
            out.write_all(value)?;
        }
        out.flush()?;
        self.files.push(path);
        release_free_memory();
        self.run = Vec::new();
        self.offsets = Vec::new();
        Ok(())
    }

    /// Merges the runs and writes every entry, in key order. Resolves to the number written.
    #[napi]
    pub fn finish(&mut self) -> Result<AsyncTask<LoadSorted>> {
        let store = self.store.take().ok_or_else(|| Error::from_reason("mostik: loader already finished"))?;
        if !self.files.is_empty() && !self.run.is_empty() {
            // the merge reads every run through a small buffer: holding the last one in memory
            // meanwhile would double what it needs
            self.spill().map_err(to_napi)?;
            release_free_memory();
        }
        sort_run(&self.run, &mut self.offsets);
        Ok(AsyncTask::new(LoadSorted {
            store,
            unique: self.unique,
            run: std::mem::take(&mut self.run),
            offsets: std::mem::take(&mut self.offsets),
            files: std::mem::take(&mut self.files),
        }))
    }
}

impl Drop for SortedLoader {
    fn drop(&mut self) {
        for path in &self.files {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub struct LoadSorted {
    store: Arc<Store>,
    unique: bool,
    run: Vec<u8>,
    offsets: Vec<u32>,
    files: Vec<std::path::PathBuf>,
}

/// A sorted source of entries: the in-memory run or a spilled file.
enum Source {
    Memory(usize),
    File(io::BufReader<std::fs::File>),
}

impl LoadSorted {
    fn next(&self, source: &mut Source) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        use std::io::Read;
        match source {
            Source::Memory(i) => Ok(self.offsets.get(*i).map(|&at| {
                *i += 1;
                let (k, v) = entry_at(&self.run, at as usize);
                (k.to_vec(), v.to_vec())
            })),
            Source::File(file) => {
                let mut len = [0u8; 4];
                match file.read_exact(&mut len) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                    Err(e) => return Err(e),
                }
                let mut key = vec![0u8; u32::from_le_bytes(len) as usize];
                file.read_exact(&mut key)?;
                file.read_exact(&mut len)?;
                let mut value = vec![0u8; u32::from_le_bytes(len) as usize];
                file.read_exact(&mut value)?;
                Ok(Some((key, value)))
            }
        }
    }

    fn load(&mut self) -> io::Result<u64> {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut sources = vec![Source::Memory(0)];
        for path in &self.files {
            sources.push(Source::File(io::BufReader::with_capacity(1 << 20, std::fs::File::open(path)?)));
        }
        let mut heap = BinaryHeap::new();
        for (n, source) in sources.iter_mut().enumerate() {
            if let Some((key, value)) = self.next(source)? {
                heap.push(Reverse((key, n, value)));
            }
        }
        let (mut written, mut ops) = (0u64, Vec::with_capacity(LOAD_CHUNK));
        // unique: the prefix of the last entry (the key without its separator and _id)
        let mut last_prefix: Vec<u8> = Vec::new();
        while let Some(Reverse((key, n, value))) = heap.pop() {
            if self.unique {
                let suffix = match value.as_slice() {
                    [lo, hi] => u16::from_le_bytes([*lo, *hi]) as usize + 1,
                    _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "mostik: malformed index entry")),
                };
                let prefix = key.get(..key.len().saturating_sub(suffix)).unwrap_or(&[]);
                if written + (ops.len() as u64) > 0 && prefix == last_prefix.as_slice() {
                    let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
                    return Err(io::Error::other(format!("mostik: duplicate key {hex}")));
                }
                last_prefix.clear();
                last_prefix.extend_from_slice(prefix);
            }
            ops.push(Op::Put(key, encode_value(&value, 0)));
            if let Some((key, value)) = self.next(&mut sources[n])? {
                heap.push(Reverse((key, n, value)));
            }
            if ops.len() == LOAD_CHUNK {
                written += ops.len() as u64;
                self.store.commit_unjournaled(std::mem::replace(&mut ops, Vec::with_capacity(LOAD_CHUNK)))?;
                // entries come and go by the million: hand freed memory back as the load goes
                release_free_memory();
            }
        }
        written += ops.len() as u64;
        self.store.commit_unjournaled(ops)?;
        Ok(written)
    }
}

impl Task for LoadSorted {
    type Output = f64;
    type JsValue = f64;

    fn compute(&mut self) -> Result<f64> {
        let result = self.load();
        for path in &self.files {
            let _ = std::fs::remove_file(path);
        }
        self.run = Vec::new();
        release_free_memory();
        result.map(|n| n as f64).map_err(to_napi)
    }

    fn resolve(&mut self, _env: napi::Env, written: f64) -> Result<f64> {
        Ok(written)
    }
}

fn record_size(key: &[u8], stored: &[u8]) -> io::Result<usize> {
    Ok(8 + key.len() + stored_len(stored).map_err(to_io)?)
}

/// Appends `[key_len u32][key][value_len u32][value]` if the decoded value passes `filter`.
/// Ok(false) when it does not fit.
/// Keys in `[start, end)`, ascending, or descending when `reverse`.
fn walk(snapshot: &Snapshot, reverse: bool, start: &[u8], end: &[u8], f: impl FnMut(&[u8], &[u8]) -> bool) -> io::Result<()> {
    if reverse {
        snapshot.range_rev(start, end, f)
    } else {
        snapshot.range(start, end, f)
    }
}

/// The documents under `docs` that exist and pass `filter`, as (key, document), fetched,
/// decompressed and filtered in parallel.
fn fetch_documents(snapshot: &Snapshot, filter: Option<&Filter>, project: Option<&Projection>, docs: Vec<Vec<u8>>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    use rayon::prelude::*;
    let fetch = |doc: Vec<u8>| -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        // None: a dangling entry
        let Some(value) = snapshot.get_with(&doc, decode_value)? else { return Ok(None) };
        let value = value.map_err(to_io)?;
        Ok(filter.is_none_or(|f| f.matches(&value)).then(|| (doc, projected(value, project))))
    };
    let fetched: Vec<io::Result<Option<(Vec<u8>, Vec<u8>)>>> =
        if docs.len() >= PARALLEL_FROM { docs.into_par_iter().map(fetch).collect() } else { docs.into_iter().map(fetch).collect() };
    fetched.into_iter().filter_map(|found| found.transpose()).collect()
}

fn write_record(out: &mut [u8], used: &mut usize, key: &[u8], stored: &[u8], filter: Option<&Filter>, project: Option<&Projection>) -> io::Result<bool> {
    let mut size = record_size(key, stored)?;
    if *used + size > out.len() {
        return Ok(false);
    }
    let (at, value_at) = (*used, *used + 8 + key.len());
    decode_value_into(stored, &mut out[value_at..at + size]).map_err(to_io)?;
    if filter.is_none_or(|filter| filter.matches(&out[value_at..at + size])) {
        if let Some(fields) = project.and_then(|p| p.apply(&out[value_at..at + size])) {
            // never longer than the document
            out[value_at..value_at + fields.len()].copy_from_slice(&fields);
            size = 8 + key.len() + fields.len();
        }
        out[at..at + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
        out[at + 4..value_at - 4].copy_from_slice(key);
        out[value_at - 4..value_at].copy_from_slice(&((size - 8 - key.len()) as u32).to_le_bytes());
        *used += size;
    }
    Ok(true)
}

/// `value` with only the fields of `project`, if any.
fn projected(value: Vec<u8>, project: Option<&Projection>) -> Vec<u8> {
    project.and_then(|p| p.apply(&value)).unwrap_or(value)
}

fn corrupted_value() -> Error {
    Error::from_reason("mostik: database is corrupted (value)")
}

/// Length of a stored value once decoded.
fn stored_len(stored: &[u8]) -> Result<usize> {
    match stored.split_first() {
        Some((&RAW, v)) => Ok(v.len()),
        Some((&LZ4, v)) => lz4_flex::block::uncompressed_size(v).map(|(size, _)| size).map_err(|_| corrupted_value()),
        _ => Err(corrupted_value()),
    }
}

fn decode_value_into(stored: &[u8], out: &mut [u8]) -> Result<usize> {
    match stored.split_first() {
        Some((&RAW, v)) => {
            if let Some(dst) = out.get_mut(..v.len()) {
                dst.copy_from_slice(v);
            }
            Ok(v.len())
        }
        Some((&LZ4, v)) => {
            let (size, block) = lz4_flex::block::uncompressed_size(v).map_err(|_| corrupted_value())?;
            if let Some(dst) = out.get_mut(..size) {
                if lz4_flex::block::decompress_into(block, dst).map_err(|_| corrupted_value())? != size {
                    return Err(corrupted_value());
                }
            }
            Ok(size)
        }
        _ => Err(corrupted_value()),
    }
}

/// The decoded value of `stored` if it passes `filter`; raw values are checked without a copy.
fn filtered_value(stored: &[u8], filter: &Filter) -> io::Result<Option<Vec<u8>>> {
    Ok(match stored.split_first() {
        Some((&RAW, v)) => filter.matches(v).then(|| v.to_vec()),
        _ => Some(decode_value(stored).map_err(to_io)?).filter(|v| filter.matches(v)),
    })
}

fn decode_value(stored: &[u8]) -> Result<Vec<u8>> {
    let mut value = vec![0u8; stored_len(stored)?];
    decode_value_into(stored, &mut value)?;
    Ok(value)
}

/// `Condition::Equals` compares against values as JS sees them: header and compression removed.
fn decode_for_conditions(stored: &[u8]) -> io::Result<Vec<u8>> {
    decode_value(stored).map_err(to_io)
}

fn encode_value(value: &[u8], threshold: usize) -> Vec<u8> {
    if threshold > 0 && value.len() > threshold {
        let compressed = lz4_flex::compress_prepend_size(value);
        if compressed.len() < value.len() {
            let mut out = Vec::with_capacity(compressed.len() + 1);
            out.push(LZ4);
            out.extend_from_slice(&compressed);
            return out;
        }
    }
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(RAW);
    out.extend_from_slice(value);
    out
}

pub struct Commit {
    store: Arc<Store>,
    batch: Vec<u8>,
    threshold: usize,
}

/// Batch records: `[op u8]` then, per op,
///   PUT / REQUIRE_EQUAL: `[key_len u32][key][value_len u32][value]`
///   REMOVE / REQUIRE_ABSENT / REQUIRE_PRESENT: `[key_len u32][key]`
///   ADD: `[key_len u32][key][delta i64]`
///   END_GROUP: `[chain u32]`, closing the group built by the records before it
fn parse_batch(batch: &[u8], threshold: usize) -> Result<Vec<Group>> {
    let bad = || Error::from_reason("mostik: malformed write batch");
    let empty = || Group { conditions: Vec::new(), ops: Vec::new(), chain: 0 };
    let mut groups = Vec::new();
    let mut group = empty();
    let mut at = 0;
    let fixed = |at: &mut usize, n: usize| -> Result<&[u8]> {
        let bytes = batch.get(*at..*at + n).ok_or_else(bad)?;
        *at += n;
        Ok(bytes)
    };
    let chunk = |at: &mut usize| -> Result<&[u8]> {
        let len = u32::from_le_bytes(fixed(at, 4)?.try_into().unwrap()) as usize;
        fixed(at, len)
    };
    while at < batch.len() {
        let op = batch[at];
        at += 1;
        if op == OP_END_GROUP {
            let mut done = std::mem::replace(&mut group, empty());
            done.chain = u32::from_le_bytes(fixed(&mut at, 4)?.try_into().unwrap());
            groups.push(done);
            continue;
        }
        let key = chunk(&mut at)?.to_vec();
        match op {
            OP_PUT => group.ops.push(Op::Put(key, encode_value(chunk(&mut at)?, threshold))),
            OP_REMOVE => group.ops.push(Op::Remove(key)),
            OP_ADD => group.ops.push(Op::Add(key, i64::from_le_bytes(fixed(&mut at, 8)?.try_into().unwrap()))),
            OP_REQUIRE_ABSENT => group.conditions.push(Condition::Absent(key)),
            OP_REQUIRE_PRESENT => group.conditions.push(Condition::Present(key)),
            OP_REQUIRE_EQUAL => group.conditions.push(Condition::Equals(key, chunk(&mut at)?.to_vec())),
            OP_REQUIRE_UNIQUE => {
                let prefix_len = u32::from_le_bytes(fixed(&mut at, 4)?.try_into().unwrap()) as usize;
                if prefix_len >= key.len() {
                    return Err(bad());
                }
                group.conditions.push(Condition::Unique { key, prefix_len });
            }
            _ => return Err(bad()),
        }
    }
    if !group.conditions.is_empty() || !group.ops.is_empty() {
        return Err(bad());
    }
    Ok(groups)
}

impl Task for Commit {
    type Output = Vec<u8>;
    type JsValue = Buffer;

    fn compute(&mut self) -> Result<Vec<u8>> {
        let groups = parse_batch(&self.batch, self.threshold)?;
        let applied = self.store.commit_groups(groups, &decode_for_conditions).map_err(to_napi)?;
        if self.batch.len() >= RELEASE_AFTER_BYTES {
            self.batch = Vec::new();
            release_free_memory();
        }
        Ok(applied.into_iter().map(u8::from).collect())
    }

    fn resolve(&mut self, _env: napi::Env, output: Vec<u8>) -> Result<Buffer> {
        Ok(output.into())
    }
}
