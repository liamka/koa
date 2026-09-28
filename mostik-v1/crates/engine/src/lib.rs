//! mostik storage engine: a single-file, copy-on-write B+ tree with LMDB-style
//! double meta pages. One writer at a time, readers never block.

mod btree;
mod freelist;
mod meta;
mod page;
mod reader;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use memmap2::Mmap;

pub use btree::Op;
use btree::Txn;
use freelist::FreeList;
use meta::Meta;
pub use page::{MAX_KEY_SIZE, PAGE_SIZE};
use page::{corrupted, decode_freelist, encode_freelist, FREELIST_PER_PAGE, KIND_FREELIST};
use reader::{Readers, Snapshot};

/// The file grows by at least this much, so it is not remapped on every commit.
const MIN_GROWTH: u64 = 1 << 20;

pub struct Env {
    file: File,
    _lock: File,
    readers: Readers,
    writer: Mutex<Writer>,
}

struct Writer {
    meta: Meta,
    map: Arc<Mmap>,
    free: FreeList,
    /// pages holding the persisted freelist of `meta`
    freelist_pages: Vec<u64>,
    file_len: u64,
    /// Set when a commit failed after its meta write began: what is on disk is unknown,
    /// so reusing pages could corrupt a meta that did reach the disk.
    poisoned: bool,
}

fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<Env>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<Env>>>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

impl Env {
    /// Opens (creating if needed) the database file at `path`, locked through `lock_path`.
    /// Opening the same file twice in one process returns the same `Env`.
    pub fn open(path: &Path, lock_path: &Path) -> io::Result<Arc<Env>> {
        let mut registry = registry().lock().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let key = path.canonicalize()?;
        if let Some(env) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(env);
        }
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(lock_path)?;
        if lock.try_lock().is_err() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("mostik: {} is already open in another process", path.display()),
            ));
        }
        let env = Arc::new(Env::load(file, lock)?);
        registry.insert(key, Arc::downgrade(&env));
        Ok(env)
    }

    fn load(file: File, lock: File) -> io::Result<Env> {
        let mut file_len = file.metadata()?.len();
        if file_len == 0 {
            // slot 0 holds txn 0; slot 1 an older copy that is never newer
            file.write_all_at(&Meta::empty(0).encode(), 0)?;
            file.write_all_at(&Meta::empty(0).encode(), PAGE_SIZE as u64)?;
            file.sync_all()?;
            file_len = 2 * PAGE_SIZE as u64;
        }
        let map = Arc::new(unsafe { Mmap::map(&file)? });
        let meta = Meta::newest(&map).ok_or_else(|| corrupted("no valid meta page"))?;
        if meta.page_count * PAGE_SIZE as u64 > file_len {
            return Err(corrupted("file is shorter than its meta says"));
        }
        let (free_pages, freelist_pages) = read_freelist(&map, meta.freelist)?;
        let free = FreeList::new(free_pages, meta.page_count);
        let readers = Readers::new(Snapshot { txn: meta.txn, root: meta.root, map: map.clone() });
        Ok(Env {
            file,
            _lock: lock,
            readers,
            writer: Mutex::new(Writer { meta, map, free, freelist_pages, file_len, poisoned: false }),
        })
    }

    /// Reads `key` from the latest committed snapshot and hands the value bytes to `f`.
    pub fn get_with<R>(&self, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> io::Result<Option<R>> {
        let snap = self.readers.begin();
        Ok(btree::get(&snap.map, snap.root, key)?.map(f))
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_with(key, <[u8]>::to_vec)
    }

    /// Applies `ops` in order as one durable transaction. Returns once the data is on disk.
    pub fn commit(&self, mut ops: Vec<Op>) -> io::Result<()> {
        for op in &ops {
            let len = op.key().len();
            if len == 0 || len > MAX_KEY_SIZE {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("mostik: invalid key size {len}")));
            }
            if let Op::Put(_, value) = op {
                if value.len() > u32::MAX as usize {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "mostik: value larger than 4 GiB"));
                }
            }
        }
        // stable sort keeps submission order among equal keys; the last op per key wins
        ops.sort_by(|a, b| a.key().cmp(b.key()));
        let mut ops: Vec<Op> = ops.into_iter().rev().collect();
        ops.dedup_by(|later, earlier| later.key() == earlier.key());
        ops.reverse();
        if ops.is_empty() {
            return Ok(());
        }

        let mut w = self.writer.lock().unwrap();
        if w.poisoned {
            return Err(io::Error::other("mostik: a previous commit failed to reach the disk; reopen the database"));
        }
        let saved = w.free.clone();
        let result = self.commit_locked(&mut w, &ops);
        if result.is_err() {
            w.free = saved;
        }
        result
    }

    fn commit_locked(&self, w: &mut Writer, ops: &[Op]) -> io::Result<()> {
        w.free.release(self.readers.oldest());
        let txn_id = w.meta.txn + 1;
        let map = w.map.clone();
        let mut txn = Txn::new(&map, &mut w.free);
        let root = txn.apply(w.meta.root, ops)?;
        let mut freed = std::mem::take(&mut txn.freed);
        let mut dirty = std::mem::take(&mut txn.dirty);

        // The old freelist pages stay intact until this commit is durable.
        freed.extend_from_slice(&w.freelist_pages);
        w.free.defer(txn_id, freed);
        let (freelist_head, freelist_pages) = write_freelist(&mut w.free, &mut dirty);

        let needed = w.free.page_count * PAGE_SIZE as u64;
        let grown = needed > w.file_len;
        if grown {
            let new_len = needed.max(w.file_len + w.file_len / 2).max(w.file_len + MIN_GROWTH);
            let new_len = new_len.div_ceil(PAGE_SIZE as u64) * PAGE_SIZE as u64;
            self.file.set_len(new_len)?;
            w.file_len = new_len;
        }
        let mut pages: Vec<_> = dirty.into_iter().collect();
        pages.sort_unstable_by_key(|(pgno, _)| *pgno);
        for (pgno, bytes) in &pages {
            self.file.write_all_at(bytes, pgno * PAGE_SIZE as u64)?;
        }
        self.file.sync_data()?;

        let meta = Meta { txn: txn_id, root, freelist: freelist_head, page_count: w.free.page_count };
        w.poisoned = true;
        self.file.write_all_at(&meta.encode(), meta.slot() * PAGE_SIZE as u64)?;
        self.file.sync_data()?;
        if grown {
            w.map = Arc::new(unsafe { Mmap::map(&self.file)? });
        }
        w.poisoned = false;
        w.meta = meta;
        w.freelist_pages = freelist_pages;
        self.readers.publish(Snapshot { txn: txn_id, root, map: w.map.clone() });
        Ok(())
    }

    /// Committed transaction count; mostly useful for tests.
    pub fn txn_id(&self) -> u64 {
        self.writer.lock().unwrap().meta.txn
    }
}

/// Persists the free page list as a chain of pages taken from the list itself when possible.
fn write_freelist(free: &mut FreeList, dirty: &mut HashMap<u64, Vec<u8>>) -> (u64, Vec<u64>) {
    let mut chain = Vec::new();
    let mut listed = free.len();
    while listed.div_ceil(FREELIST_PER_PAGE) > chain.len() {
        match free.take_ready() {
            Some(p) => {
                listed -= 1;
                chain.push(p);
            }
            None => chain.push(free.alloc(1)),
        }
    }
    let listed = free.all();
    let mut chunks: Vec<&[u64]> = listed.chunks(FREELIST_PER_PAGE).collect();
    // taking a page can shrink the list by one chunk; a spare page stays in the chain, empty
    chunks.resize(chain.len(), &[]);
    let mut next = 0;
    for (pgno, pages) in chain.iter().zip(chunks).rev() {
        dirty.insert(*pgno, encode_freelist(pages, next));
        next = *pgno;
    }
    (next, chain)
}

fn read_freelist(map: &[u8], mut pgno: u64) -> io::Result<(Vec<u64>, Vec<u64>)> {
    let (mut free, mut chain) = (Vec::new(), Vec::new());
    while pgno != 0 {
        if chain.len() > map.len() / PAGE_SIZE {
            return Err(corrupted("freelist loop"));
        }
        let page = btree::page_at(map, pgno)?;
        if page.kind() != KIND_FREELIST {
            return Err(corrupted("freelist page kind"));
        }
        free.extend(decode_freelist(page));
        chain.push(pgno);
        pgno = page.next();
    }
    Ok((free, chain))
}

#[cfg(test)]
mod tests;
