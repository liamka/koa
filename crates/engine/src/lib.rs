//! mostik storage engine: a copy-on-write B+ tree with LMDB-style double meta pages and a
//! journal. A commit appends its operations to the journal and keeps the pages it changed in
//! memory under temporary refs; a checkpoint, in the background, compresses them and lays them
//! out on disk. One writer at a time, readers never block.

mod btree;
mod freelist;
mod meta;
mod page;
mod pagefile;
mod reader;
mod wal;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

pub use btree::{Op, PARALLEL_LEAVES};
pub use reader::Snapshot;
use btree::Txn;
use freelist::{Extent, FreeList, Space};
use meta::Meta;
pub use page::{BLOCK, MAX_KEY_SIZE, PAGE_SIZE};
use page::*;
use pagefile::{Dirty, PageFile, View};
use reader::Readers;
use wal::Wal;

/// The file grows by at least this much, so it is not resized on every commit.
const MIN_GROWTH: u64 = 1 << 20;
const MAX_GROWTH: u64 = 64 << 20;
/// Memory for cached pages; the database file itself is never mapped into memory.
pub const CACHE_BYTES: usize = 64 << 20;
/// A background checkpoint starts once this much changed-page memory is waiting to be written ...
const CHECKPOINT_DIRTY_BYTES: usize = 32 << 20;
/// ... and a commit waits for one when writers outrun it past this much.
const MAX_DIRTY_BYTES: usize = 160 << 20;
/// ... or the journal is this long ...
const CHECKPOINT_JOURNAL_BYTES: u64 = 64 << 20;
/// ... or this long after the previous one.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
/// A batch of at least this many groups checks their conditions in parallel.
const PARALLEL_CONDITIONS: usize = 64;
/// Consecutive groups a worker checks, at least.
const CONDITIONS_PER_TASK: usize = 16;
/// Changed pages stay uncompressed up to this many bytes: most are replaced before then.
/// Small in tests, so that they see both kinds.
const RAW_DIRTY_BYTES: usize = if cfg!(test) { 64 << 10 } else { 16 << 20 };
/// In `Durability::Journal`, how often the journal is flushed to the disk.
const JOURNAL_SYNC_INTERVAL: Duration = Duration::from_millis(100);
/// Commits that made this many pages since memory was last handed back hand it back.
const RELEASE_AFTER_PAGES: usize = 4096;

/// Hands memory the process freed back to the system now. The allocator keeps freed pages
/// resident until memory runs short, and they count in the process's resident size meanwhile:
/// commits and checkpoints free many pages at once.
fn release_memory() {
    #[cfg(target_os = "macos")]
    {
        extern "C" {
            fn malloc_zone_pressure_relief(zone: *mut std::ffi::c_void, goal: usize) -> usize;
        }
        // SAFETY: a null zone asks every zone to give back what it can; no memory is touched
        unsafe {
            malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
        }
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        // SAFETY: trims the free memory of glibc's heaps; no memory in use is touched
        unsafe {
            malloc_trim(0);
        }
    }
}

/// When a commit counts as done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Written to the journal (the OS has it): survives the process crashing; a power loss can
    /// drop the last ~100 ms of commits, never corrupt the file. MongoDB's default.
    Journal,
    /// Flushed to the disk before returning (concurrent commits share one flush).
    Strict,
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub cache_bytes: usize,
    pub durability: Durability,
}

impl Default for Options {
    fn default() -> Options {
        Options { cache_bytes: CACHE_BYTES, durability: Durability::Journal }
    }
}

/// What must hold, at commit time, for a group to be applied.
pub enum Condition {
    /// e.g. "insert unless the id exists"
    Absent(Vec<u8>),
    Present(Vec<u8>),
    /// the value, once decoded, is exactly these bytes: "unchanged since it was read"
    Equals(Vec<u8>, Vec<u8>),
    /// no key but this one continues its first `prefix_len` bytes past a 0 separator: an entry
    /// of a unique index, `[prefix][0][_id]`, whose value no other document has
    Unique { key: Vec<u8>, prefix_len: usize },
}

impl Condition {
    fn key(&self) -> &[u8] {
        match self {
            Condition::Absent(k) | Condition::Present(k) | Condition::Equals(k, _) | Condition::Unique { key: k, .. } => k,
        }
    }
}

/// Writes applied together, only if all `conditions` hold (seen after the groups before it).
/// Groups sharing a non-zero `chain` form a sequence: once one of them is rejected, the rest
/// of that chain is rejected too.
pub struct Group {
    pub conditions: Vec<Condition>,
    pub ops: Vec<Op>,
    pub chain: u32,
}

/// Decodes a stored value into the bytes `Condition::Equals` compares against.
pub type Decode<'a> = &'a (dyn Fn(&[u8]) -> io::Result<Vec<u8>> + Sync);

/// A key's state after the earlier accepted groups of a batch.
#[derive(Clone, Copy)]
enum Pending<'a> {
    Put(&'a [u8]),
    Removed,
    Counter,
}

impl Snapshot {
    pub fn get_with<R>(&self, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> io::Result<Option<R>> {
        btree::get_with(self.view(), self.root, key, f)
    }

    /// Calls `f` with each entry where `start <= key < end`, in key order, until `f` returns false.
    pub fn range(&self, start: &[u8], end: &[u8], mut f: impl FnMut(&[u8], &[u8]) -> bool) -> io::Result<()> {
        btree::range(self.view(), self.root, start, end, &mut f)
    }

    /// Like `range`, in descending key order: from the last key below `end` down to `start`.
    pub fn range_rev(&self, start: &[u8], end: &[u8], mut f: impl FnMut(&[u8], &[u8]) -> bool) -> io::Result<()> {
        btree::range_rev(self.view(), self.root, start, end, &mut f)
    }

    /// Like `range`, with `map` running on several leaves at once on worker threads (`first`
    /// leaves at first, then twice as many each time, up to `PARALLEL_LEAVES`): `sink` gets
    /// the results of consecutive leaves in key order, with the last key they hold in the
    /// range, and returns false to stop.
    pub fn par_range<T: Send>(
        &self,
        start: &[u8],
        end: &[u8],
        first: usize,
        map: &(dyn Fn(&[u8], &[u8]) -> io::Result<Option<T>> + Sync),
        mut sink: impl FnMut(Vec<T>, &[u8]) -> bool,
    ) -> io::Result<()> {
        btree::par_range(self.view(), self.root, start, end, first, map, &mut sink)
    }
}

pub struct Env {
    pages: Arc<PageFile>,
    _lock: File,
    readers: Readers,
    writer: Mutex<Writer>,
    durability: Durability,
    /// the two journal files (commits go to one while a checkpoint retires the other), for
    /// flushing without the writer lock; bytes written to and flushed in each
    journals: [File; 2],
    journal_written: [AtomicU64; 2],
    journal_synced: [AtomicU64; 2],
    journal_sync: Mutex<()>,
    journal_dirty: AtomicBool,
    /// one checkpoint at a time
    checkpointing: Mutex<()>,
    checkpoint_wanted: AtomicBool,
    /// tests: runs between planning a checkpoint and writing it
    #[cfg(test)]
    before_write: Mutex<Option<Box<dyn Fn() + Send>>>,
}

struct Writer {
    /// the last checkpoint: what the file holds
    meta: Meta,
    /// the latest commit
    txn: u64,
    root: Ref,
    free: FreeList,
    /// blocks holding the persisted freelist of `meta`
    freelist_blocks: Vec<u64>,
    /// Set when a checkpoint failed: what is on disk is unknown, so reusing space could
    /// corrupt a meta that did reach the disk. Reopening recovers.
    poisoned: bool,
    wals: [Wal; 2],
    /// the journal commits append to
    active: usize,
    next_temp: u64,
    /// pages and values in memory under temporary refs -> their size
    dirty_sizes: HashMap<Ref, usize>,
    dirty_bytes: usize,
    /// pages commits made since memory was last handed back (release_memory)
    made_pages: usize,
    /// the temporary refs of the latest tree: what the next checkpoint writes
    live: HashSet<Ref>,
    /// temporary pages commits dropped, kept in memory until no reader can reach them
    held: VecDeque<(u64, Vec<Ref>)>,
    /// the temporary refs a running checkpoint writes, and those of them commits dropped since
    in_checkpoint: HashSet<Ref>,
    freed_in_checkpoint: Vec<(u64, Ref)>,
    /// temporary refs written by the checkpoint of a commit, still translated for readers and
    /// for pages copied before that checkpoint ended
    translated: VecDeque<(u64, Vec<Ref>)>,
    last_checkpoint: Instant,
}

/// What a checkpoint writes, decided under the writer lock and written without it.
struct CheckpointPlan {
    txn: u64,
    root: Ref,
    pages: HashMap<Ref, Dirty>,
    space: Space,
    /// extents free in the new checkpoint besides what is left of `space`
    free: Vec<Extent>,
    old_chain: Vec<u64>,
    /// the journal holding the records this checkpoint covers
    retiring: usize,
}

/// What a checkpoint wrote.
struct Written {
    meta: Meta,
    refs: HashMap<Ref, Ref>,
    chain: Vec<u64>,
    space: Space,
}

fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<Env>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<Env>>>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

fn journal_path(path: &Path, n: usize) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!("-journal{n}"));
    PathBuf::from(name)
}

fn poisoned_error() -> io::Error {
    io::Error::other("mostik: a checkpoint failed to reach the disk; reopen the database")
}

impl Env {
    /// Opens (creating if needed) the database file at `path`, locked through `lock_path`.
    /// Opening the same file twice in one process returns the same `Env`.
    pub fn open(path: &Path, lock_path: &Path) -> io::Result<Arc<Env>> {
        Env::open_with(path, lock_path, Options::default())
    }

    /// Like `open`, with `cache_bytes` of memory for cached pages.
    pub fn open_with_cache(path: &Path, lock_path: &Path, cache_bytes: usize) -> io::Result<Arc<Env>> {
        Env::open_with(path, lock_path, Options { cache_bytes, ..Options::default() })
    }

    /// Like `open`, with options. If the file is already open in this process, the existing
    /// `Env` is returned with the options it was opened with.
    pub fn open_with(path: &Path, lock_path: &Path, options: Options) -> io::Result<Arc<Env>> {
        let mut open_envs = registry().lock().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let key = path.canonicalize()?;
        if let Some(env) = open_envs.get(&key).and_then(Weak::upgrade) {
            return Ok(env);
        }
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(lock_path)?;
        // An `Env` of this process that is still closing (its last reference may be dropped by
        // the background thread, which then writes the final checkpoint) holds the lock a
        // little longer: wait for it rather than blame another process.
        let closing_here = open_envs.contains_key(&key);
        let deadline = Instant::now() + Duration::from_secs(30);
        while lock.try_lock().is_err() {
            if !closing_here || Instant::now() > deadline {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("mostik: {} is already open in another process", path.display()),
                ));
            }
            drop(open_envs);
            std::thread::sleep(Duration::from_millis(1));
            open_envs = registry().lock().unwrap();
        }
        let journal = |n| OpenOptions::new().read(true).write(true).create(true).truncate(false).open(journal_path(path, n));
        let env = Arc::new(Env::load(file, lock, [journal(0)?, journal(1)?], options)?);
        open_envs.insert(key, Arc::downgrade(&env));
        drop(open_envs);
        spawn_background(Arc::downgrade(&env));
        Ok(env)
    }

    fn load(file: File, lock: File, journals: [File; 2], options: Options) -> io::Result<Env> {
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            // slot 0 holds txn 0; slot 1 an older copy that is never newer
            file.write_all_at(&Meta::empty(0).encode(), 0)?;
            file.write_all_at(&Meta::empty(0).encode(), BLOCK as u64)?;
            file.sync_all()?;
        }
        let mut metas = vec![0u8; 2 * BLOCK];
        file.read_exact_at(&mut metas, 0).map_err(|_| corrupted("file too short"))?;
        let meta = Meta::newest(&metas).ok_or_else(|| corrupted("no valid meta page"))?;
        if meta.end * BLOCK as u64 > file_len.max(2 * BLOCK as u64) {
            return Err(corrupted("file is shorter than its meta says"));
        }
        let (free_extents, freelist_blocks) = read_freelist(&file, meta.freelist, meta.end)?;
        let pages = Arc::new(PageFile::new(file, options.cache_bytes, meta.end));
        let readers = Readers::new(Snapshot { txn: meta.txn, root: meta.root, pages: pages.clone() });
        let (wal0, records0) = Wal::open(journals[0].try_clone()?)?;
        let (wal1, records1) = Wal::open(journals[1].try_clone()?)?;
        let lens = [wal0.len, wal1.len];
        let env = Env {
            pages,
            _lock: lock,
            readers,
            writer: Mutex::new(Writer {
                meta,
                txn: meta.txn,
                root: meta.root,
                free: FreeList::new(free_extents, meta.end),
                freelist_blocks,
                poisoned: false,
                wals: [wal0, wal1],
                active: 0,
                next_temp: 0,
                dirty_sizes: HashMap::new(),
                dirty_bytes: 0,
                made_pages: 0,
                live: HashSet::new(),
                held: VecDeque::new(),
                in_checkpoint: HashSet::new(),
                freed_in_checkpoint: Vec::new(),
                translated: VecDeque::new(),
                last_checkpoint: Instant::now(),
            }),
            durability: options.durability,
            journals,
            journal_written: lens.map(AtomicU64::new),
            journal_synced: lens.map(AtomicU64::new),
            journal_sync: Mutex::new(()),
            journal_dirty: AtomicBool::new(false),
            checkpointing: Mutex::new(()),
            checkpoint_wanted: AtomicBool::new(false),
            #[cfg(test)]
            before_write: Mutex::new(None),
        };
        let mut records = records0;
        records.extend(records1);
        records.sort_by_key(|(txn, _)| *txn);
        env.recover(records)?;
        Ok(env)
    }

    /// Replays the journal records that follow the checkpoint, then checkpoints them and
    /// empties both journals.
    fn recover(&self, records: Vec<(u64, Vec<Op>)>) -> io::Result<()> {
        let mut w = self.writer.lock().unwrap();
        for (txn, ops) in records {
            if txn <= w.txn {
                continue; // already in the checkpoint
            }
            if txn != w.txn + 1 {
                break; // a gap: nothing after it can be trusted
            }
            self.apply_locked(&mut w, &ops, Log::Nothing)?;
        }
        let dirty_journals = w.wals.iter().any(|wal| wal.len > 0);
        drop(w);
        if dirty_journals {
            self.checkpoint()?;
            let mut w = self.writer.lock().unwrap();
            for (n, wal) in w.wals.iter_mut().enumerate() {
                wal.reset()?;
                self.journal_written[n].store(0, Ordering::Release);
                self.journal_synced[n].store(0, Ordering::Release);
            }
            w.active = 0;
        }
        Ok(())
    }

    /// Reads `key` from the latest committed snapshot and hands the value bytes to `f`.
    pub fn get_with<R>(&self, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> io::Result<Option<R>> {
        self.readers.begin().get_with(key, f)
    }

    /// The latest committed state; it stays readable, unchanged, for as long as it is held.
    /// What it uses is not reused until it is dropped, so do not hold it longer than needed.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.readers.begin()
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_with(key, <[u8]>::to_vec)
    }

    /// Applies `ops` in order as one transaction.
    pub fn commit(&self, ops: Vec<Op>) -> io::Result<()> {
        let identity = |v: &[u8]| Ok(v.to_vec());
        self.commit_groups(vec![Group { conditions: Vec::new(), ops, chain: 0 }], &identity).map(drop)
    }

    /// Commits several groups as one transaction. A group is applied only if all of its
    /// conditions hold, as seen after the groups before it; otherwise it is skipped as a whole.
    /// Returns, per group, whether it was applied. Durable as `Options::durability` says.
    pub fn commit_groups(&self, groups: Vec<Group>, decode: Decode) -> io::Result<Vec<bool>> {
        let keys = groups.iter().flat_map(|g| g.conditions.iter().map(Condition::key).chain(g.ops.iter().map(Op::key)));
        for key in keys {
            if key.is_empty() || key.len() > MAX_KEY_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("mostik: invalid key size {}", key.len()),
                ));
            }
        }
        for g in &groups {
            check_ops(&g.ops)?;
        }

        let mut w = self.writer.lock().unwrap();
        if w.poisoned {
            return Err(poisoned_error());
        }
        let view = View { pages: &self.pages };
        let root = w.root;
        // a condition on a key no earlier group of the batch writes depends on the tree alone
        let on_tree = |lookup: &mut btree::Lookup, condition: &Condition| -> io::Result<bool> {
            let key = condition.key();
            Ok(match condition {
                Condition::Absent(_) => lookup.get_with(key, |_| ())?.is_none(),
                Condition::Present(_) => lookup.get_with(key, |_| ())?.is_some(),
                Condition::Equals(_, expected) => {
                    lookup.get_with(key, |stored| decode(stored).map(|v| v == *expected))?.transpose()?.unwrap_or(false)
                }
                // needs what the batch did before it: checked in order, below
                Condition::Unique { .. } => true,
            })
        };
        // many groups: check them all against the tree at once, in parallel, each worker a run
        // of consecutive groups (neighbouring keys share a leaf)
        let checked: Option<Vec<Vec<io::Result<bool>>>> = (groups.len() >= PARALLEL_CONDITIONS).then(|| {
            use rayon::prelude::*;
            groups
                .par_iter()
                .with_min_len(CONDITIONS_PER_TASK)
                .map_init(|| btree::Lookup::new(view, root), |lookup, g| g.conditions.iter().map(|c| on_tree(lookup, c)).collect())
                .collect()
        });
        let mut lookup = btree::Lookup::new(view, root);
        // ordered: unique conditions look at key ranges
        let mut pending: std::collections::BTreeMap<&[u8], Pending> = std::collections::BTreeMap::new();
        let mut broken_chains = HashSet::new();
        let mut applied = Vec::with_capacity(groups.len());
        for (gi, g) in groups.iter().enumerate() {
            let mut ok = g.chain == 0 || !broken_chains.contains(&g.chain);
            for (ci, condition) in g.conditions.iter().enumerate() {
                if !ok {
                    break;
                }
                let key = condition.key();
                if let Condition::Unique { key, prefix_len } = condition {
                    ok = unique(view, root, &pending, key, *prefix_len)?;
                    continue;
                }
                let state = pending.get(key).copied();
                ok = match (condition, state) {
                    (Condition::Absent(_), Some(state)) => matches!(state, Pending::Removed),
                    (Condition::Present(_), Some(state)) => !matches!(state, Pending::Removed),
                    (Condition::Equals(_, expected), Some(Pending::Put(value))) => decode(value)? == *expected,
                    (Condition::Equals(..), Some(_)) => false,
                    (Condition::Unique { .. }, Some(_)) => unreachable!("checked above"),
                    (_, None) => match &checked {
                        Some(checked) => match &checked[gi][ci] {
                            Ok(ok) => *ok,
                            Err(e) => return Err(io::Error::new(e.kind(), e.to_string())),
                        },
                        None => on_tree(&mut lookup, condition)?,
                    },
                };
            }
            if ok {
                for op in &g.ops {
                    let state = match op {
                        Op::Put(_, value) => Pending::Put(value),
                        Op::Remove(_) => Pending::Removed,
                        Op::Add(..) => Pending::Counter,
                        // groups hold key operations only (check_ops)
                        Op::RemoveRange(..) => Pending::Removed,
                    };
                    pending.insert(op.key(), state);
                }
            } else if g.chain != 0 {
                broken_chains.insert(g.chain);
            }
            applied.push(ok);
        }
        drop(pending);
        let ops = merge_ops(groups.into_iter().zip(&applied).filter(|(_, &ok)| ok).flat_map(|(g, _)| g.ops).collect());
        if ops.is_empty() {
            return Ok(applied);
        }
        self.apply_locked(&mut w, &ops, Log::Ops)?;
        self.after_commit(w)?;
        Ok(applied)
    }

    /// Removes every key of each range `[start, end)`, then applies `ops` (key operations), in one
    /// commit. A range with a counter key subtracts the keys it removed from that counter.
    /// Whole subtrees inside a range go without being rebuilt, so this takes time in proportion to
    /// the pages removed, not to their keys. Returns the keys removed per range.
    pub fn delete_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>, Option<Vec<u8>>)], ops: Vec<Op>) -> io::Result<Vec<u64>> {
        let bound = |key: &[u8]| key.len() > MAX_KEY_SIZE + 1;
        if ranges.iter().any(|(start, end, counter)| bound(start) || bound(end) || counter.as_ref().is_some_and(|c| c.is_empty() || c.len() > MAX_KEY_SIZE))
            || ops.iter().any(|op| op.key().is_empty() || op.key().len() > MAX_KEY_SIZE)
        {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "mostik: invalid key size"));
        }
        check_ops(&ops)?;
        let mut w = self.writer.lock().unwrap();
        if w.poisoned {
            return Err(poisoned_error());
        }
        let mut counts = Vec::with_capacity(ranges.len());
        self.apply_with(&mut w, Log::Ops, |txn, mut root| {
            let mut journal = Vec::with_capacity(ranges.len() + ops.len());
            for (start, end, _) in ranges {
                let range = Op::RemoveRange(start.clone(), end.clone());
                let before = txn.removed;
                root = txn.apply(root, std::slice::from_ref(&range))?;
                counts.push(txn.removed - before);
                journal.push(range);
            }
            let mut rest = ops;
            for ((_, _, counter), &n) in ranges.iter().zip(&counts) {
                if let Some(counter) = counter.as_ref().filter(|_| n > 0) {
                    rest.push(Op::Add(counter.clone(), -(n as i64)));
                }
            }
            let rest = merge_ops(rest);
            root = txn.apply(root, &rest)?;
            journal.extend(rest);
            Ok((root, Cow::Owned(journal)))
        })?;
        self.after_commit(w)?;
        Ok(counts)
    }

    /// What follows a journaled commit, the writer lock released: flushing the journal as the
    /// durability asks, and a checkpoint when changed pages pile up.
    fn after_commit(&self, mut w: std::sync::MutexGuard<Writer>) -> io::Result<()> {
        let (journal, journal_end) = (w.active, w.wals[w.active].len);
        self.journal_written[journal].store(journal_end, Ordering::Release);
        let dirty = w.dirty_bytes;
        if dirty > CHECKPOINT_DIRTY_BYTES || journal_end > CHECKPOINT_JOURNAL_BYTES {
            self.checkpoint_wanted.store(true, Ordering::Release);
        }
        let release = w.made_pages >= RELEASE_AFTER_PAGES;
        if release {
            w.made_pages = 0;
        }
        drop(w);
        if release {
            release_memory();
        }
        match self.durability {
            Durability::Strict => self.sync_journal(journal, journal_end)?,
            Durability::Journal => self.journal_dirty.store(true, Ordering::Release),
        }
        if dirty > MAX_DIRTY_BYTES {
            // writers outran the background checkpoints: this one waits for a checkpoint
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Applies `ops` without journaling them (a bulk load): after a crash they are gone unless a
    /// checkpoint wrote them first. The caller makes them count only once `checkpoint` returned.
    pub fn commit_unjournaled(&self, ops: Vec<Op>) -> io::Result<()> {
        if let Some(op) = ops.iter().find(|op| op.key().is_empty() || op.key().len() > MAX_KEY_SIZE) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("mostik: invalid key size {}", op.key().len())));
        }
        check_ops(&ops)?;
        let ops = merge_ops(ops);
        if ops.is_empty() {
            return Ok(());
        }
        let mut w = self.writer.lock().unwrap();
        if w.poisoned {
            return Err(poisoned_error());
        }
        self.apply_locked(&mut w, &ops, Log::Mark)?;
        let (journal, journal_end) = (w.active, w.wals[w.active].len);
        self.journal_written[journal].store(journal_end, Ordering::Release);
        let dirty = w.dirty_bytes;
        if dirty > CHECKPOINT_DIRTY_BYTES {
            self.checkpoint_wanted.store(true, Ordering::Release);
        }
        drop(w);
        if dirty > MAX_DIRTY_BYTES {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Flushes journal `n` to the disk up to at least `end`. Concurrent callers share a flush:
    /// whoever gets the lock flushes everything written so far.
    fn sync_journal(&self, n: usize, end: u64) -> io::Result<()> {
        if self.journal_synced[n].load(Ordering::Acquire) >= end {
            return Ok(());
        }
        let _flushing = self.journal_sync.lock().unwrap();
        if self.journal_synced[n].load(Ordering::Acquire) >= end {
            return Ok(());
        }
        let written = self.journal_written[n].load(Ordering::Acquire);
        self.journals[n].sync_data()?;
        self.journal_synced[n].fetch_max(written, Ordering::AcqRel);
        Ok(())
    }

    /// Releases what no reader can reach any more: disk space, dropped pages kept in memory,
    /// and translations nothing refers to.
    fn release(&self, w: &mut Writer) {
        let oldest = self.readers.oldest();
        w.free.release(oldest);
        let reachable = |tag: u64| oldest.is_some_and(|o| o < tag);
        let mut gone = Vec::new();
        while w.held.front().is_some_and(|(tag, _)| !reachable(*tag)) {
            gone.extend(w.held.pop_front().unwrap().1);
        }
        for r in &gone {
            w.dirty_bytes -= w.dirty_sizes.remove(r).unwrap_or(0);
        }
        if !gone.is_empty() {
            self.pages.remove_dirty(gone);
        }
        // A translation written at checkpoint T is only needed while pages copied before the
        // next checkpoint ended exist: once that one is done and no reader predates it, drop it.
        let latest = w.meta.txn;
        while w.translated.front().is_some_and(|(tag, _)| *tag < latest) && !reachable(latest) {
            self.pages.remove_translations(w.translated.pop_front().unwrap().1);
        }
    }

    /// Builds the new tree for `ops`, journals it (`log`) and publishes it. The changed pages
    /// stay in memory until a checkpoint writes them.
    fn apply_locked(&self, w: &mut Writer, ops: &[Op], log: Log) -> io::Result<()> {
        self.apply_with(w, log, |txn, root| Ok((txn.apply(root, ops)?, Cow::Borrowed(ops))))
    }

    /// Like `apply_locked`, the new tree built by `build` from the old root: it returns the new
    /// root, and the ops to journal (that build the same tree when replayed).
    fn apply_with<'o>(&self, w: &mut Writer, log: Log, build: impl FnOnce(&mut Txn, Ref) -> io::Result<(Ref, Cow<'o, [Op]>)>) -> io::Result<()> {
        self.release(w);
        let txn_id = w.txn + 1;
        let view = View { pages: &self.pages };
        let Writer { next_temp, in_checkpoint, root: old_root, .. } = &mut *w;
        let counter = AtomicU64::new(*next_temp);
        let raw_bytes = std::sync::atomic::AtomicUsize::new(0);
        let mut txn = Txn::new(view, &counter, &raw_bytes, in_checkpoint);
        let (root, ops) = build(&mut txn, *old_root)?;
        let ops: &[Op] = &ops;
        let dirty = std::mem::take(&mut txn.dirty);
        let freed_extents = std::mem::take(&mut txn.freed_extents);
        let freed_temp = std::mem::take(&mut txn.freed_temp);
        let freed_checkpointing = std::mem::take(&mut txn.freed_checkpointing);
        drop(txn);
        *next_temp = counter.into_inner();
        match log {
            Log::Ops => {
                let active = w.active;
                w.wals[active].append(txn_id, ops)?;
            }
            // keeps the journal's commits consecutive without the operations themselves
            Log::Mark => {
                let active = w.active;
                w.wals[active].append(txn_id, &[])?;
            }
            Log::Nothing => {}
        }
        let (mut raw, mut packed) = (Vec::with_capacity(dirty.len()), Vec::new());
        for (r, page) in dirty {
            match page {
                btree::TxnPage::Raw(bytes) => raw.push((r, bytes)),
                btree::TxnPage::Packed(extent, links) => packed.push((r, extent, links)),
            }
        }
        w.made_pages += raw.len() + packed.len();
        for (r, size) in self.pages.add_dirty(raw).into_iter().chain(self.pages.add_packed(packed)) {
            w.dirty_sizes.insert(r, size);
            w.dirty_bytes += size;
            w.live.insert(r);
        }
        if self.pages.raw_bytes() > RAW_DIRTY_BYTES {
            self.release(w);
            for (r, size) in self.pages.pack_dirty() {
                if let Some(old) = w.dirty_sizes.insert(r, size) {
                    w.dirty_bytes = w.dirty_bytes - old + size;
                }
            }
        }
        for r in freed_temp.iter().chain(&freed_checkpointing) {
            w.live.remove(r);
        }
        if !freed_temp.is_empty() {
            w.held.push_back((txn_id, freed_temp));
        }
        w.freed_in_checkpoint.extend(freed_checkpointing.into_iter().map(|r| (txn_id, r)));
        // written by a checkpoint: they wait for the next one, then for readers
        w.free.defer_until_checkpoint(txn_id, freed_extents);
        w.txn = txn_id;
        w.root = root;
        self.readers.publish(Snapshot { txn: txn_id, root, pages: self.pages.clone() });
        Ok(())
    }

    /// Writes every change so far to disk. Commits keep going meanwhile: the pages are written
    /// without the writer lock, and new commits go to the other journal.
    pub fn checkpoint(&self) -> io::Result<()> {
        let _one = self.checkpointing.lock().unwrap();
        self.checkpoint_wanted.store(false, Ordering::Release);
        let plan = {
            let mut w = self.writer.lock().unwrap();
            if w.poisoned {
                return Err(poisoned_error());
            }
            if w.txn == w.meta.txn {
                return Ok(());
            }
            self.prepare_checkpoint(&mut w)
        };
        #[cfg(test)]
        if let Some(hook) = self.before_write.lock().unwrap().as_ref() {
            hook();
        }
        let written = self.write_checkpoint(plan);
        let mut w = self.writer.lock().unwrap();
        let done = match written {
            Ok((plan, written)) => self.finish_checkpoint(&mut w, plan, written),
            Err(e) => {
                w.poisoned = true;
                Err(e)
            }
        };
        drop(w);
        // the pages written are let go of
        release_memory();
        done
    }

    fn prepare_checkpoint(&self, w: &mut Writer) -> CheckpointPlan {
        self.release(w);
        let live: Vec<Ref> = w.live.iter().copied().collect();
        let pages: HashMap<Ref, Dirty> = live.iter().filter_map(|&r| self.pages.dirty_get(r).map(|p| (r, p))).collect();
        w.in_checkpoint = live.into_iter().collect();
        // commits from here on go to the other journal (empty, except while recovering, which
        // empties both afterwards); this one only holds covered records
        let retiring = w.active;
        w.active = 1 - retiring;
        CheckpointPlan {
            txn: w.txn,
            root: w.root,
            pages,
            space: w.free.take_space(),
            free: w.free.waiting(),
            old_chain: w.freelist_blocks.clone(),
            retiring,
        }
    }

    /// The slow part, without the writer lock: lays the new pages out on disk, children first
    /// so that parents can point at them, then the freelist and the meta.
    fn write_checkpoint(&self, mut plan: CheckpointPlan) -> io::Result<(CheckpointPlan, Written)> {
        // a power loss in the middle must still find the retiring journal whole
        self.journals[plan.retiring].sync_data()?;
        let mut refs = HashMap::new();
        let mut file_len = self.pages.file().metadata()?.len();
        let root = self.write_tree(plan.root, &plan.pages, &mut plan.space, &mut refs, &mut file_len)?;
        let mut free = plan.free.clone();
        free.extend(plan.old_chain.iter().map(|&b| (b, FREELIST_BLOCKS)));
        let (freelist, chain, blocks) = build_freelist(&mut plan.space, &free);
        let file = self.pages.file();
        grow(file, &mut file_len, plan.space.end)?;
        for (block, bytes) in &blocks {
            file.write_all_at(bytes, block * BLOCK as u64)?;
        }
        file.sync_data()?;
        let meta = Meta { txn: plan.txn, root, freelist, end: plan.space.end };
        file.write_all_at(&meta.encode(), meta.slot() * BLOCK as u64)?;
        file.sync_data()?;
        let space = std::mem::take(&mut plan.space);
        Ok((plan, Written { meta, refs, chain, space }))
    }

    /// Writes the temporary page `r` and everything temporary below it; returns its physical ref.
    fn write_tree(
        &self,
        r: Ref,
        pages: &HashMap<Ref, Dirty>,
        space: &mut Space,
        refs: &mut HashMap<Ref, Ref>,
        file_len: &mut u64,
    ) -> io::Result<Ref> {
        if r == 0 || !is_temp(r) {
            return Ok(r);
        }
        if let Some(&done) = refs.get(&r) {
            return Ok(done);
        }
        if let Some(earlier) = self.pages.translated(r) {
            return Ok(earlier);
        }
        let stored = pages.get(&r).ok_or_else(|| corrupted("checkpoint lost a page"))?;
        let rewritten = match stored {
            Dirty::Raw(value) if is_temp_overflow(r) => Some(encode_extent(EXTENT_VALUE, value)),
            // values, and pages pointing at no temporary page: written as they were compressed
            Dirty::Packed(_, false) => None,
            _ if is_temp_overflow(r) => None,
            // a page pointing at temporary pages is rewritten to point at where they went
            _ => {
                let (mut page, raw) = match stored {
                    Dirty::Raw(page) => (page.to_vec(), true),
                    Dirty::Packed(extent, _) => (decode_extent(EXTENT_PAGE, extent)?, false),
                };
                let mut changed = false;
                let count = Page(&page).count();
                for i in 0..count {
                    match page[0] {
                        KIND_BRANCH => {
                            let child = Page(&page).branch_child(i)?;
                            if is_temp(child) {
                                let written = self.write_tree(child, pages, space, refs, file_len)?;
                                set_branch_child(&mut page, i, written)?;
                                changed = true;
                            }
                        }
                        _ => {
                            if let ValueRef::Overflow { r: value, .. } = Page(&page).leaf_value(i)? {
                                if is_temp(value) {
                                    let written = self.write_tree(value, pages, space, refs, file_len)?;
                                    set_leaf_overflow(&mut page, i, written)?;
                                    changed = true;
                                }
                            }
                        }
                    }
                }
                (changed || raw).then(|| encode_extent(EXTENT_PAGE, &page))
            }
        };
        let extent: &[u8] = match (&rewritten, stored) {
            (Some(extent), _) => extent,
            (None, Dirty::Packed(extent, _)) => extent,
            (None, Dirty::Raw(_)) => unreachable!("raw pages are always encoded"),
        };
        let blocks = (extent.len() / BLOCK) as u64;
        let block = space.alloc(blocks);
        let file = self.pages.file();
        grow(file, file_len, block + blocks)?;
        file.write_all_at(extent, block * BLOCK as u64)?;
        let written = phys_ref(block, blocks);
        refs.insert(r, written);
        Ok(written)
    }

    fn finish_checkpoint(&self, w: &mut Writer, plan: CheckpointPlan, written: Written) -> io::Result<()> {
        let Written { meta, refs, chain, space } = written;
        // new content at these blocks: drop stale cached copies before readers can reach them
        for phys in refs.values() {
            self.pages.invalidate(extent_of(*phys).0);
        }
        self.pages.set_end(meta.end);
        self.pages.add_translations(refs.iter().map(|(&t, &p)| (t, p)));
        // the written pages are read from disk now
        let gone: Vec<Ref> = refs.keys().copied().collect();
        for r in &gone {
            w.dirty_bytes -= w.dirty_sizes.remove(r).unwrap_or(0);
            w.live.remove(r);
        }
        self.pages.remove_dirty(gone.iter().copied());
        w.in_checkpoint.clear();
        // dropped while being written: their extents are in this checkpoint
        for (tag, r) in std::mem::take(&mut w.freed_in_checkpoint) {
            if let Some(phys) = refs.get(&r) {
                w.free.defer_until_checkpoint(tag, vec![extent_of(*phys)]);
            }
        }
        w.free.return_space(space);
        w.free.defer(plan.txn, plan.old_chain.iter().map(|&b| (b, FREELIST_BLOCKS)).collect());
        w.free.checkpoint_done(plan.txn);
        w.meta = meta;
        w.freelist_blocks = chain;
        w.translated.push_back((plan.txn, gone));
        if w.txn == plan.txn {
            w.root = meta.root;
        }
        // every record there is in the file now
        w.wals[plan.retiring].reset()?;
        self.journal_written[plan.retiring].store(0, Ordering::Release);
        self.journal_synced[plan.retiring].store(0, Ordering::Release);
        w.last_checkpoint = Instant::now();
        release_free_memory();
        Ok(())
    }

    /// Periodic work: flush the journals, checkpoint when it is time.
    fn background_tick(&self) {
        if self.journal_dirty.swap(false, Ordering::AcqRel) {
            for n in 0..2 {
                let written = self.journal_written[n].load(Ordering::Acquire);
                let _ = self.sync_journal(n, written);
            }
        }
        let due = self.checkpoint_wanted.load(Ordering::Acquire)
            || self.writer.try_lock().is_ok_and(|w| w.txn != w.meta.txn && w.last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL);
        if due {
            let _ = self.checkpoint();
        }
    }

    /// The pages of the latest commit.
    #[cfg(test)]
    fn latest_view(&self) -> View<'_> {
        View { pages: &self.pages }
    }

    /// Committed transaction count; mostly useful for tests.
    pub fn txn_id(&self) -> u64 {
        self.writer.lock().unwrap().txn
    }
}

impl Drop for Env {
    /// Closing writes a checkpoint, so the next open has no journal to replay.
    fn drop(&mut self) {
        let _ = self.checkpoint();
    }
}

/// Flushes the journal every `JOURNAL_SYNC_INTERVAL` and checkpoints when due, for as long as
/// the `Env` lives.
fn spawn_background(env: Weak<Env>) {
    let spawned = std::thread::Builder::new().name("mostik-journal".into()).spawn(move || loop {
        std::thread::sleep(JOURNAL_SYNC_INTERVAL);
        let Some(env) = env.upgrade() else { return };
        env.background_tick();
    });
    // without the thread, commits in Journal mode are flushed at checkpoints only
    drop(spawned);
}

/// A checkpoint frees many megabytes of page copies at once; the system allocator keeps such
/// memory mapped, so the process would stay at its peak size. Hand it back (at most once a
/// second: the call walks the allocator's regions).
pub fn release_free_memory() {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64);
    let last = LAST.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 1000 || LAST.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        return;
    }
    #[cfg(target_vendor = "apple")]
    {
        extern "C" {
            fn malloc_zone_pressure_relief(zone: *mut std::ffi::c_void, goal: usize) -> usize;
        }
        // SAFETY: a null zone means every zone; goal 0 means release as much as possible
        unsafe { malloc_zone_pressure_relief(std::ptr::null_mut(), 0) };
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        // SAFETY: plain glibc call without pointers
        unsafe { malloc_trim(0) };
    }
}

/// Makes the file hold at least `blocks` blocks, growing ahead by an eighth (at most
/// MAX_GROWTH): few resizes without a large unused tail.
fn grow(file: &File, len: &mut u64, blocks: u64) -> io::Result<()> {
    let needed = blocks * BLOCK as u64;
    if needed > *len {
        let ahead = (*len / 8).clamp(MIN_GROWTH, MAX_GROWTH);
        *len = needed.max(*len + ahead).div_ceil(BLOCK as u64) * BLOCK as u64;
        file.set_len(*len)?;
    }
    Ok(())
}

/// What a commit leaves in the journal.
#[derive(Clone, Copy)]
enum Log {
    Ops,
    /// only its transaction number
    Mark,
    /// nothing: replaying the journal itself
    Nothing,
}

/// Sorts ops by key, keeping the last op per key; counter additions to one key are summed.
/// (Counter keys only ever receive `Add`.)
/// Whether `key` is the only key starting with its first `prefix_len` bytes and a 0 separator,
/// the tree as `pending` (the batch so far) leaves it.
fn unique(view: View, root: Ref, pending: &std::collections::BTreeMap<&[u8], Pending>, key: &[u8], prefix_len: usize) -> io::Result<bool> {
    let prefix = key.get(..prefix_len).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "mostik: unique prefix"))?;
    let (start, end) = ([prefix, &[0]].concat(), [prefix, &[1]].concat());
    let others = |k: &[u8]| k != key;
    if pending.range::<[u8], _>((std::ops::Bound::Included(&start[..]), std::ops::Bound::Excluded(&end[..]))).any(|(k, state)| others(k) && !matches!(state, Pending::Removed)) {
        return Ok(false);
    }
    let mut taken = false;
    btree::range(view, root, &start, &end, &mut |k, _| {
        taken = others(k) && !matches!(pending.get(k), Some(Pending::Removed));
        !taken
    })?;
    Ok(!taken)
}

/// Key operations only (ranges go through `delete_ranges`), with values that fit.
fn check_ops(ops: &[Op]) -> io::Result<()> {
    for op in ops {
        match op {
            Op::Put(_, value) if value.len() > u32::MAX as usize => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "mostik: value larger than 4 GiB"));
            }
            Op::RemoveRange(..) => return Err(io::Error::new(io::ErrorKind::InvalidInput, "mostik: ranges are removed by delete_ranges")),
            _ => {}
        }
    }
    Ok(())
}

fn merge_ops(mut ops: Vec<Op>) -> Vec<Op> {
    // stable sort keeps submission order among equal keys
    ops.sort_by(|a, b| a.key().cmp(b.key()));
    let mut merged: Vec<Op> = Vec::with_capacity(ops.len());
    for op in ops {
        match (merged.last_mut(), op) {
            (Some(Op::Add(key, sum)), Op::Add(next, delta)) if *key == next => *sum += delta,
            (Some(last), op) if last.key() == op.key() => *last = op,
            (_, op) => merged.push(op),
        }
    }
    merged
}

/// The freelist blocks of a checkpoint listing `free` and what is left of `space`, taking the
/// blocks themselves from `space`. Returns the first block, the chain, and the bytes to write.
fn build_freelist(space: &mut Space, free: &[Extent]) -> (u64, Vec<u64>, Vec<(u64, Vec<u8>)>) {
    let mut chain = Vec::new();
    loop {
        let listed = free.len() + space.extents().count();
        if listed.div_ceil(FREELIST_PER_NODE) <= chain.len() {
            break;
        }
        chain.push(space.alloc(FREELIST_BLOCKS));
    }
    let mut extents: Vec<Extent> = free.iter().copied().chain(space.extents()).collect();
    extents.sort_unstable();
    let mut chunks: Vec<&[Extent]> = extents.chunks(FREELIST_PER_NODE).collect();
    // taking a block can merge or shrink extents; a spare block stays in the chain, empty
    chunks.resize(chain.len(), &[]);
    let (mut next, mut blocks) = (0, Vec::new());
    for (block, extents) in chain.iter().zip(chunks).rev() {
        blocks.push((*block, encode_freelist(extents, next)));
        next = *block;
    }
    (next, chain, blocks)
}

fn read_freelist(file: &File, mut block: u64, end: u64) -> io::Result<(Vec<Extent>, Vec<u64>)> {
    let (mut free, mut chain) = (Vec::new(), Vec::new());
    let mut bytes = vec![0u8; FREELIST_NODE];
    while block != 0 {
        if block + FREELIST_BLOCKS > end || chain.len() as u64 > end {
            return Err(corrupted("freelist chain"));
        }
        file.read_exact_at(&mut bytes, block * BLOCK as u64)?;
        let page = Page(&bytes);
        if page.kind() != KIND_FREELIST {
            return Err(corrupted("freelist block kind"));
        }
        free.extend(decode_freelist(page));
        chain.push(block);
        block = page.next();
    }
    Ok((free, chain))
}

#[cfg(test)]
mod tests;
