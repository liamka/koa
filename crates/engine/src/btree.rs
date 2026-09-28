//! Copy-on-write B+ tree. A commit never touches pages of the published tree: every page on
//! the path to a modified key is rebuilt into a new page. New pages get temporary refs and stay
//! in memory; a checkpoint writes them to disk later.

use std::collections::{HashMap, HashSet};
use std::io;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::freelist::Extent;
use crate::page::*;
use crate::pagefile::View;

const MIN_FILL: usize = CAPACITY / 4;
/// A branch routing at least this many changes rebuilds its children's subtrees in parallel.
const PARALLEL_OPS: usize = 256;
const MAX_DEPTH: usize = 64;

#[derive(Clone)]
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
    /// Adds to a counter stored as `COUNTER_TAG` + i64 LE (created at 0 when missing).
    Add(Vec<u8>, i64),
    /// Removes every key in `[start, end)`. Applied before the other ops of its commit.
    RemoveRange(Vec<u8>, Vec<u8>),
}

/// Leading byte of counter values; 0 is also the Node layer's "stored raw" value header, so
/// counters read back through it as 8 raw bytes.
pub const COUNTER_TAG: u8 = 0;

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Put(k, _) | Op::Remove(k) | Op::Add(k, _) | Op::RemoveRange(k, _) => k,
        }
    }
}

fn counter_value(existing: Option<&Value>, delta: i64) -> io::Result<Vec<u8>> {
    let current = match existing {
        None => 0,
        Some(Value::Inline(bytes)) if bytes.len() == 9 && bytes[0] == COUNTER_TAG => {
            i64::from_le_bytes(bytes[1..].try_into().unwrap())
        }
        Some(_) => return Err(corrupted("counter value")),
    };
    let mut value = vec![COUNTER_TAG];
    value.extend_from_slice(&current.wrapping_add(delta).to_le_bytes());
    Ok(value)
}

/// Looks `key` up in the tree rooted at `root` and hands its value to `f`.
pub fn get_with<R>(view: View, root: u64, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> io::Result<Option<R>> {
    if root == 0 {
        return Ok(None);
    }
    let mut pgno = root;
    for _ in 0..MAX_DEPTH {
        let bytes = view.page(pgno)?;
        let page = Page(&bytes);
        match page.kind() {
            KIND_BRANCH if page.count() > 0 => pgno = page.branch_child(page.branch_search(key)?)?,
            KIND_LEAF => {
                let Ok(i) = page.leaf_search(key)? else { return Ok(None) };
                return match page.leaf_value(i)? {
                    ValueRef::Inline(value) => Ok(Some(f(value))),
                    ValueRef::Overflow { r, len } => Ok(Some(f(&view.overflow(r, len)?))),
                };
            }
            _ => return Err(corrupted("unexpected page kind")),
        }
    }
    Err(corrupted("tree too deep"))
}

/// Calls `f` for each entry with `start <= key < end`, in key order, until `f` returns false.
/// Point lookups that remember the last leaf reached and the key range it covers: keys that
/// fall in that range (neighbours, e.g. increasing ids) are answered without a descent.
pub struct Lookup<'a> {
    view: View<'a>,
    root: u64,
    /// the leaf, and the range of keys it holds: `lower <= key < upper` (None: no bound)
    leaf: Option<(Arc<[u8]>, Option<Vec<u8>>, Option<Vec<u8>>)>,
}

impl<'a> Lookup<'a> {
    pub fn new(view: View<'a>, root: u64) -> Self {
        Lookup { view, root, leaf: None }
    }

    pub fn get_with<R>(&mut self, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> io::Result<Option<R>> {
        if self.root == 0 {
            return Ok(None);
        }
        let covered = self.leaf.as_ref().is_some_and(|(_, lower, upper)| {
            lower.as_deref().is_none_or(|l| !compare(key, l).is_lt()) && upper.as_deref().is_none_or(|u| compare(key, u).is_lt())
        });
        if !covered {
            self.leaf = Some(self.locate(key)?);
        }
        let page = Page(&self.leaf.as_ref().unwrap().0);
        match page.leaf_search(key)? {
            Ok(i) => match page.leaf_value(i)? {
                ValueRef::Inline(value) => Ok(Some(f(value))),
                ValueRef::Overflow { r, len } => Ok(Some(f(&self.view.overflow(r, len)?))),
            },
            Err(_) => Ok(None),
        }
    }

    /// The leaf whose range holds `key`, with that range.
    fn locate(&self, key: &[u8]) -> io::Result<(Arc<[u8]>, Option<Vec<u8>>, Option<Vec<u8>>)> {
        let (mut r, mut lower, mut upper) = (self.root, None::<Vec<u8>>, None::<Vec<u8>>);
        for _ in 0..=MAX_DEPTH {
            let bytes = self.view.page(r)?;
            let page = Page(&bytes);
            match page.kind() {
                KIND_LEAF => return Ok((bytes, lower, upper)),
                KIND_BRANCH if page.count() > 0 => {
                    let i = page.branch_search(key)?;
                    if i > 0 {
                        lower = Some(page.branch_key(i)?.to_vec());
                    }
                    if i + 1 < page.count() {
                        upper = Some(page.branch_key(i + 1)?.to_vec());
                    }
                    r = page.branch_child(i)?;
                }
                _ => return Err(corrupted("unexpected page kind")),
            }
        }
        Err(corrupted("tree too deep"))
    }
}

pub fn range(
    view: View,
    root: u64,
    start: &[u8],
    end: &[u8],
    f: &mut dyn FnMut(&[u8], &[u8]) -> bool,
) -> io::Result<()> {
    if root != 0 && compare(start, end).is_lt() {
        walk(view, root, start, end, f, 0)?;
    }
    Ok(())
}

/// Leaves a parallel range walk handles at once, at most. The count starts small and doubles,
/// so a walk that stops early (a small limit) does little work it does not need.
pub const PARALLEL_LEAVES: usize = 256;

/// Like `range`, but `map` runs on several leaves at once, on worker threads, `first` leaves
/// at first. `sink` gets the results of consecutive leaves in key order, with the last key
/// those leaves hold in the range, and returns false to stop.
#[allow(clippy::too_many_arguments)]
pub fn par_range<T: Send>(
    view: View,
    root: u64,
    start: &[u8],
    end: &[u8],
    first: usize,
    map: &(dyn Fn(&[u8], &[u8]) -> io::Result<Option<T>> + Sync),
    sink: &mut dyn FnMut(Vec<T>, &[u8]) -> bool,
) -> io::Result<()> {
    if root == 0 || !compare(start, end).is_lt() {
        return Ok(());
    }
    let mut walk = LeafWalk { view, start, end, map, sink, batch: Vec::new(), ahead: first.clamp(1, PARALLEL_LEAVES) };
    if walk.branch(root, 0)? {
        walk.flush()?;
    }
    Ok(())
}

struct LeafWalk<'a, T> {
    view: View<'a>,
    start: &'a [u8],
    end: &'a [u8],
    map: &'a (dyn Fn(&[u8], &[u8]) -> io::Result<Option<T>> + Sync),
    sink: &'a mut dyn FnMut(Vec<T>, &[u8]) -> bool,
    /// leaves waiting to be handled
    batch: Vec<Ref>,
    ahead: usize,
}

impl<T: Send> LeafWalk<'_, T> {
    /// Collects the leaves under page `r` that hold keys in range. False once the walk must stop.
    fn branch(&mut self, r: Ref, depth: usize) -> io::Result<bool> {
        if depth > MAX_DEPTH {
            return Err(corrupted("tree too deep"));
        }
        let bytes = self.view.page(r)?;
        let page = Page(&bytes);
        match page.kind() {
            KIND_LEAF => {
                // the root is a leaf
                self.batch.push(r);
                Ok(true)
            }
            KIND_BRANCH if page.count() > 0 => {
                let first = page.branch_search(self.start)?;
                // the tree is balanced: if one child is a leaf, all are
                let over_leaves = self.view.page(page.branch_child(first)?)?[0] == KIND_LEAF;
                for i in first..page.count() {
                    if i > first && !compare(page.branch_key(i)?, self.end).is_lt() {
                        // past the range: what is collected is all there is
                        self.flush()?;
                        return Ok(false);
                    }
                    let child = page.branch_child(i)?;
                    if over_leaves {
                        self.batch.push(child);
                        if self.batch.len() >= self.ahead && !self.flush()? {
                            return Ok(false);
                        }
                    } else if !self.branch(child, depth + 1)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Err(corrupted("unexpected page kind")),
        }
    }

    /// Handles the collected leaves in parallel. False once the walk must stop.
    fn flush(&mut self) -> io::Result<bool> {
        use rayon::prelude::*;
        if self.batch.is_empty() {
            return Ok(true);
        }
        let (view, start, end, map) = (self.view, self.start, self.end, self.map);
        let leaf = |&r: &Ref| -> io::Result<(Vec<T>, Option<Vec<u8>>)> {
            let bytes = view.page(r)?;
            let page = Page(&bytes);
            if page.kind() != KIND_LEAF {
                return Err(corrupted("unexpected page kind"));
            }
            let (Ok(first) | Err(first)) = page.leaf_search(start)?;
            let (mut out, mut last) = (Vec::new(), None);
            for i in first..page.count() {
                let key = page.leaf_key(i)?;
                if !compare(key, end).is_lt() {
                    break;
                }
                let found = match page.leaf_value(i)? {
                    ValueRef::Inline(value) => map(key, value)?,
                    ValueRef::Overflow { r, len } => map(key, &view.overflow(r, len)?)?,
                };
                out.extend(found);
                last = Some(i);
            }
            Ok((out, last.map(|i| page.leaf_key(i).map(<[u8]>::to_vec)).transpose()?))
        };
        let done: Vec<io::Result<(Vec<T>, Option<Vec<u8>>)>> =
            if self.batch.len() > 1 { self.batch.par_iter().map(leaf).collect() } else { self.batch.iter().map(leaf).collect() };
        self.batch.clear();
        self.ahead = (self.ahead * 2).min(PARALLEL_LEAVES);
        let (mut results, mut last) = (Vec::new(), None);
        for leaf in done {
            let (found, leaf_last) = leaf?;
            results.extend(found);
            last = leaf_last.or(last);
        }
        match last {
            Some(last) => Ok((self.sink)(results, &last)),
            None => Ok(true),
        }
    }
}

/// Like `range`, from the last key before `end` down to `start`.
pub fn range_rev(
    view: View,
    root: u64,
    start: &[u8],
    end: &[u8],
    f: &mut dyn FnMut(&[u8], &[u8]) -> bool,
) -> io::Result<()> {
    if root != 0 && compare(start, end).is_lt() {
        walk_rev(view, root, start, end, f, 0)?;
    }
    Ok(())
}

/// Returns false once the walk must stop.
fn walk_rev(
    view: View,
    pgno: u64,
    start: &[u8],
    end: &[u8],
    f: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    depth: usize,
) -> io::Result<bool> {
    if depth > MAX_DEPTH {
        return Err(corrupted("tree too deep"));
    }
    let bytes = view.page(pgno)?;
    let page = Page(&bytes);
    match page.kind() {
        KIND_BRANCH if page.count() > 0 => {
            // the child holding `end`, then the ones before it
            let last = page.branch_search(end)?;
            for i in (0..=last).rev() {
                if !walk_rev(view, page.branch_child(i)?, start, end, f, depth + 1)? {
                    return Ok(false);
                }
                // keys of the children before this one are all below its first key
                if i > 0 && !compare(page.branch_key(i)?, start).is_gt() {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        KIND_LEAF => {
            let (Ok(below) | Err(below)) = page.leaf_search(end)?;
            for i in (0..below).rev() {
                let key = page.leaf_key(i)?;
                if compare(key, start).is_lt() {
                    return Ok(false);
                }
                let more = match page.leaf_value(i)? {
                    ValueRef::Inline(value) => f(key, value),
                    ValueRef::Overflow { r, len } => f(key, &view.overflow(r, len)?),
                };
                if !more {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Err(corrupted("unexpected page kind")),
    }
}

/// Returns false once the walk must stop.
fn walk(
    view: View,
    pgno: u64,
    start: &[u8],
    end: &[u8],
    f: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    depth: usize,
) -> io::Result<bool> {
    if depth > MAX_DEPTH {
        return Err(corrupted("tree too deep"));
    }
    let bytes = view.page(pgno)?;
    let page = Page(&bytes);
    match page.kind() {
        KIND_BRANCH if page.count() > 0 => {
            let first = page.branch_search(start)?;
            for i in first..page.count() {
                if i > first && !compare(page.branch_key(i)?, end).is_lt() {
                    return Ok(false);
                }
                if !walk(view, page.branch_child(i)?, start, end, f, depth + 1)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        KIND_LEAF => {
            let (Ok(first) | Err(first)) = page.leaf_search(start)?;
            for i in first..page.count() {
                let key = page.leaf_key(i)?;
                if !compare(key, end).is_lt() {
                    return Ok(false);
                }
                let more = match page.leaf_value(i)? {
                    ValueRef::Inline(value) => f(key, value),
                    ValueRef::Overflow { r, len } => f(key, &view.overflow(r, len)?),
                };
                if !more {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Err(corrupted("unexpected page kind")),
    }
}

/// A page read during a write: one of this commit's new pages or a committed one.
enum PageBytes<'a> {
    Dirty(&'a [u8]),
    Committed(Arc<[u8]>),
    Unpacked(Vec<u8>),
}

impl std::ops::Deref for PageBytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            PageBytes::Dirty(bytes) => bytes,
            PageBytes::Committed(bytes) => bytes,
            PageBytes::Unpacked(bytes) => bytes,
        }
    }
}

/// A write transaction: its new pages and values live in `dirty` under temporary refs.
pub struct Txn<'a> {
    pub view: View<'a>,
    /// keys `RemoveRange` ops removed
    pub removed: u64,
    /// temporary refs a running checkpoint is writing to disk
    checkpointing: &'a HashSet<Ref>,
    /// shared by the sub-transactions of a parallel commit
    next_temp: &'a AtomicU64,
    /// bytes of the pages kept whole, over all the sub-transactions of a commit
    raw_bytes: &'a AtomicUsize,
    pub dirty: HashMap<Ref, TxnPage>,
    /// extents of checkpointed pages and values this commit stops using: they are in a checkpoint
    pub freed_extents: Vec<Extent>,
    /// published temporary pages this commit stops using: in no checkpoint, they only wait for readers
    pub freed_temp: Vec<Ref>,
    /// published temporary pages the running checkpoint writes: their extents will be in it
    pub freed_checkpointing: Vec<Ref>,
}

enum Node {
    Leaf(Vec<LeafEntry>),
    Branch(Vec<BranchEntry>),
}

/// Reference from a parent to a (possibly rebuilt) child.
struct Child {
    key: Vec<u8>,
    pgno: u64,
    /// bytes used on the page; only meaningful when `touched`
    fill: usize,
    touched: bool,
    /// left under-filled on purpose (the tail of an ascending append): never merge it
    settled: bool,
}

/// How entries are spread over pages when they no longer fit on one.
#[derive(Clone, Copy, PartialEq)]
enum Pack {
    /// equal halves: room on both sides for keys arriving anywhere
    Balanced,
    /// full pages and a short tail: keys arrive in ascending order, nothing will land behind
    Append,
}

/// Every op is a put past the last existing key: the ascending-insert pattern.
/// What a subtree holds: its pages, the overflow values its leaves point to, its keys.
#[derive(Default)]
struct Subtree {
    pages: Vec<Ref>,
    values: Vec<Ref>,
    keys: u64,
}

/// `dirty`: pages of the transaction under way (a subtree it rebuilt may be let go again).
fn subtree(view: View, dirty: &HashMap<Ref, TxnPage>, r: Ref, depth: usize) -> io::Result<Subtree> {
    use rayon::prelude::*;
    if depth > MAX_DEPTH {
        return Err(corrupted("tree too deep"));
    }
    let bytes: Arc<[u8]> = match dirty.get(&r) {
        Some(TxnPage::Raw(page)) => Arc::from(page.as_slice()),
        Some(TxnPage::Packed(extent, _)) => Arc::from(decode_extent(EXTENT_PAGE, extent)?),
        None => view.page_uncached(r)?,
    };
    let page = Page(&bytes);
    let mut found = Subtree::default();
    match page.kind() {
        KIND_LEAF => {
            found.keys = page.count() as u64;
            for i in 0..page.count() {
                if let ValueRef::Overflow { r, .. } = page.leaf_value(i)? {
                    found.values.push(r);
                }
            }
        }
        KIND_BRANCH => {
            let children = (0..page.count()).map(|i| page.branch_child(i)).collect::<io::Result<Vec<_>>>()?;
            let below: Vec<io::Result<Subtree>> = children.par_iter().map(|&child| subtree(view, dirty, child, depth + 1)).collect();
            for part in below {
                let part = part?;
                found.pages.extend(part.pages);
                found.values.extend(part.values);
                found.keys += part.keys;
            }
        }
        _ => return Err(corrupted("unexpected page kind")),
    }
    found.pages.push(r);
    Ok(found)
}

fn is_append(ops: &[Op], last_key: Option<&[u8]>) -> bool {
    ops.iter().all(|op| matches!(op, Op::Put(..))) && last_key.is_none_or(|last| last < ops[0].key())
}

/// A page a transaction made: whole, or compressed (with whether it refers to temporary pages)
/// once the commit's whole pages pass `TXN_RAW_BYTES`.
pub enum TxnPage {
    Raw(Vec<u8>),
    Packed(Vec<u8>, bool),
}

/// A commit keeps the pages it makes whole up to this many bytes, then compresses each as it is
/// made: a large commit spread over the tree changes a page per operation. Small in tests, so
/// that they see both kinds.
const TXN_RAW_BYTES: usize = if cfg!(test) { 64 << 10 } else { 32 << 20 };

impl<'a> Txn<'a> {
    /// `next_temp`: the counter of temporary refs; `raw_bytes`: of the pages kept whole, shared
    /// likewise; `checkpointing`: temporary refs a running checkpoint is writing to disk.
    pub fn new(view: View<'a>, next_temp: &'a AtomicU64, raw_bytes: &'a AtomicUsize, checkpointing: &'a HashSet<Ref>) -> Txn<'a> {
        Txn {
            view,
            removed: 0,
            checkpointing,
            next_temp,
            raw_bytes,
            dirty: HashMap::new(),
            freed_extents: Vec::new(),
            freed_temp: Vec::new(),
            freed_checkpointing: Vec::new(),
        }
    }

    /// Reads a tree page into owned entries; `lower` is the key bound the parent gives it.
    fn decode(&self, pgno: u64, lower: &[u8]) -> io::Result<Node> {
        let bytes = self.page(pgno)?;
        let page = Page(&bytes);
        match page.kind() {
            KIND_LEAF => Ok(Node::Leaf(decode_leaf(page, self.view)?)),
            KIND_BRANCH if page.count() > 0 => Ok(Node::Branch(decode_branch(page, lower, self.view)?)),
            _ => Err(corrupted("unexpected page kind")),
        }
    }

    fn page(&self, pgno: Ref) -> io::Result<PageBytes<'_>> {
        match self.dirty.get(&pgno) {
            Some(TxnPage::Raw(bytes)) => Ok(PageBytes::Dirty(bytes)),
            Some(TxnPage::Packed(extent, _)) => Ok(PageBytes::Unpacked(decode_extent(EXTENT_PAGE, extent)?)),
            None => self.view.page(pgno).map(PageBytes::Committed),
        }
    }

    fn next_ref(&mut self, overflow: bool) -> Ref {
        temp_ref(self.next_temp.fetch_add(1, Ordering::Relaxed) + 1, overflow)
    }

    /// A new tree page.
    pub fn write(&mut self, mut bytes: Vec<u8>) -> Ref {
        // copied cells may still point at pages a checkpoint has written since: point them at
        // the written pages, so temporary refs die out
        normalize_page(&mut bytes, self.view);
        let r = self.next_ref(false);
        let page = if self.raw_bytes.load(Ordering::Relaxed) < TXN_RAW_BYTES {
            self.raw_bytes.fetch_add(bytes.len(), Ordering::Relaxed);
            TxnPage::Raw(bytes)
        } else {
            TxnPage::Packed(encode_extent(EXTENT_PAGE, &bytes), refers_to_temp(&bytes))
        };
        self.dirty.insert(r, page);
        r
    }

    /// A new overflow value.
    fn write_value(&mut self, bytes: Vec<u8>) -> Ref {
        let r = self.next_ref(true);
        self.raw_bytes.fetch_add(bytes.len(), Ordering::Relaxed);
        self.dirty.insert(r, TxnPage::Raw(bytes));
        r
    }

    pub fn free(&mut self, r: Ref) {
        if !is_temp(r) {
            self.freed_extents.push(extent_of(r));
        } else if let Some(page) = self.dirty.remove(&r) {
            // created by this commit: never published, nothing to wait for
            if let TxnPage::Raw(bytes) = page {
                self.raw_bytes.fetch_sub(bytes.len(), Ordering::Relaxed);
            }
        } else if let Some(written) = self.view.pages.translated(r) {
            self.freed_extents.push(extent_of(written));
        } else if self.checkpointing.contains(&r) {
            self.freed_checkpointing.push(r);
        } else {
            self.freed_temp.push(r);
        }
    }

    /// Applies `ops` (sorted by key, one op per key) to the tree at `root`; returns the new root.
    /// `RemoveRange` ops go first, in order; the others must be sorted by key, one per key.
    /// Keys removed by ranges are counted in `removed`.
    pub fn apply(&mut self, root: Ref, ops: &[Op]) -> io::Result<Ref> {
        let mut root = self.view.pages.normalize(root);
        if ops.iter().any(|op| matches!(op, Op::RemoveRange(..))) {
            for op in ops {
                if let Op::RemoveRange(start, end) = op {
                    root = self.remove_range(root, start, end)?;
                }
            }
            let rest: Vec<Op> = ops.iter().filter(|op| !matches!(op, Op::RemoveRange(..))).cloned().collect();
            return self.apply(root, &rest);
        }
        if ops.is_empty() {
            return Ok(root);
        }
        let level = if root == 0 { self.rebuild_leaf(Vec::new(), ops, None, &[])? } else { self.modify(root, &[], ops, 0)? };
        self.new_root(level)
    }

    /// The root over the top level of rebuilt pages: branches over them while there are
    /// several, and without branches that have a single child.
    fn new_root(&mut self, mut level: Vec<Child>) -> io::Result<Ref> {
        let mut root = loop {
            match level.len() {
                0 => break 0,
                1 => break level[0].pgno,
                _ => {
                    let entries = level.into_iter().map(|c| BranchEntry { key: c.key, child: c.pgno }).collect();
                    level = self.write_branches(entries, None, Pack::Balanced);
                }
            }
        };
        while root != 0 {
            let child = {
                let bytes = self.page(root)?;
                let page = Page(&bytes);
                if page.kind() != KIND_BRANCH || page.count() != 1 {
                    break;
                }
                page.branch_child(0)?
            };
            self.free(root);
            root = self.view.pages.normalize(child);
        }
        Ok(root)
    }

    /// Removes every key in `[start, end)`. Subtrees wholly inside the range are let go as they
    /// are (their pages are read only to free what they refer to): only pages on the range's
    /// edges are rebuilt.
    fn remove_range(&mut self, root: Ref, start: &[u8], end: &[u8]) -> io::Result<Ref> {
        if root == 0 || !compare(start, end).is_lt() {
            return Ok(root);
        }
        let level = self.remove_in(root, &[], None, start, end, 0)?;
        self.new_root(level)
    }

    /// `remove_range` below page `pgno`, whose keys lie in `[lower, upper)` (None: no bound).
    fn remove_in(&mut self, pgno: u64, lower: &[u8], upper: Option<&[u8]>, start: &[u8], end: &[u8], depth: usize) -> io::Result<Vec<Child>> {
        if depth > MAX_DEPTH {
            return Err(corrupted("tree too deep"));
        }
        let untouched = || vec![Child { key: lower.to_vec(), pgno, fill: 0, touched: false, settled: false }];
        let bytes = self.page_arc(pgno)?;
        let page = Page(&bytes);
        match page.kind() {
            KIND_LEAF => {
                let entries = decode_leaf(page, self.view)?;
                let before = entries.len();
                let mut kept = Vec::with_capacity(before);
                for e in entries {
                    if compare(&e.key, start).is_lt() || !compare(&e.key, end).is_lt() {
                        kept.push(e);
                    } else {
                        if let Value::Overflow { r, .. } = e.value {
                            self.free(r);
                        }
                        self.removed += 1;
                    }
                }
                if kept.len() == before {
                    return Ok(untouched());
                }
                self.free(pgno);
                Ok(self.write_leaves(kept, None, Pack::Balanced))
            }
            KIND_BRANCH if page.count() > 0 => {
                let count = page.count();
                let (mut out, mut whole, mut changed) = (Vec::with_capacity(count), Vec::new(), false);
                for i in 0..count {
                    let key = if i == 0 { lower.to_vec() } else { page.branch_key(i)?.to_vec() };
                    let high = if i + 1 < count { Some(page.branch_key(i + 1)?) } else { upper };
                    let child = page.branch_child(i)?;
                    // the child holds keys in [key, high); the first child of the root starts at -inf
                    let low_unbounded = i == 0 && lower.is_empty();
                    let before_range = high.is_some_and(|h| !compare(h, start).is_gt());
                    let after_range = !low_unbounded && !compare(&key, end).is_lt();
                    if before_range || after_range {
                        out.push(Child { key, pgno: child, fill: 0, touched: false, settled: false });
                        continue;
                    }
                    changed = true;
                    let inside = (!low_unbounded && !compare(&key, start).is_lt() || start.is_empty()) && high.is_some_and(|h| !compare(h, end).is_gt());
                    if inside {
                        whole.push(child);
                    } else {
                        out.extend(self.remove_in(child, &key, high, start, end, depth + 1)?);
                    }
                }
                if !changed {
                    return Ok(untouched());
                }
                self.let_go(&whole)?;
                self.free(pgno);
                if out.is_empty() {
                    return Ok(Vec::new());
                }
                self.rebalance(&mut out)?;
                let entries = out.into_iter().map(|c| BranchEntry { key: c.key, child: c.pgno }).collect();
                Ok(self.write_branches(entries, None, Pack::Balanced))
            }
            _ => Err(corrupted("unexpected page kind")),
        }
    }

    /// Frees whole subtrees: their pages, and the values they point to. Their pages are read in
    /// parallel, and not kept in the cache.
    fn let_go(&mut self, roots: &[Ref]) -> io::Result<()> {
        use rayon::prelude::*;
        let (view, dirty) = (self.view, &self.dirty);
        let found: Vec<io::Result<Subtree>> = roots.par_iter().map(|&r| subtree(view, dirty, r, 0)).collect();
        for subtree in found {
            let subtree = subtree?;
            for r in subtree.pages.into_iter().chain(subtree.values) {
                self.free(r);
            }
            self.removed += subtree.keys;
        }
        Ok(())
    }

    /// The page as shared bytes the caller can hold while modifying the transaction.
    fn page_arc(&self, pgno: u64) -> io::Result<Arc<[u8]>> {
        match self.page(pgno)? {
            PageBytes::Dirty(bytes) => Ok(Arc::from(bytes)),
            PageBytes::Committed(bytes) => Ok(bytes),
            PageBytes::Unpacked(bytes) => Ok(Arc::from(bytes)),
        }
    }

    fn modify(&mut self, pgno: u64, lower: &[u8], ops: &[Op], depth: usize) -> io::Result<Vec<Child>> {
        if depth > MAX_DEPTH {
            return Err(corrupted("tree too deep"));
        }
        let bytes = self.page_arc(pgno)?;
        let page = Page(&bytes);
        match page.kind() {
            KIND_LEAF => match self.patch_leaf(pgno, page, lower, ops)? {
                Some(children) => Ok(children),
                None => self.rebuild_leaf(decode_leaf(page, self.view)?, ops, Some(pgno), lower),
            },
            KIND_BRANCH if page.count() > 0 => self.modify_branch(pgno, page, lower, ops, depth),
            _ => Err(corrupted("unexpected page kind")),
        }
    }

    /// The common case, without decoding the leaf into entries: when the result fits on one
    /// page and needs no new overflow value, the new leaf is assembled from the old page's cell
    /// bytes and the ops. None: take the general path (`rebuild_leaf`).
    fn patch_leaf(&mut self, pgno: u64, page: Page, lower: &[u8], ops: &[Op]) -> io::Result<Option<Vec<Child>>> {
        let count = page.count();
        let settled = is_append(ops, if count > 0 { Some(page.leaf_key(count - 1)?) } else { None });
        let mut cells: Vec<LeafCell> = Vec::with_capacity(count + ops.len());
        let mut counters: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut runs = Vec::new();
        let (mut size, mut changed, mut i) = (0, false, 0);
        for op in ops {
            while i < count && compare(page.leaf_key(i)?, op.key()).is_lt() {
                let cell = page.leaf_cell(i)?;
                size += 2 + cell.len();
                cells.push(LeafCell::Raw(cell));
                i += 1;
            }
            let existing = if i < count && page.leaf_key(i)? == op.key() {
                i += 1;
                Some(page.leaf_value(i - 1)?)
            } else {
                None
            };
            let old_run = match &existing {
                Some(ValueRef::Overflow { r, .. }) => Some(*r),
                _ => None,
            };
            match op {
                Op::Put(key, value) => {
                    if !inline_fits(key.len(), value.len()) {
                        return Ok(None);
                    }
                    changed = true;
                    runs.extend(old_run);
                    size += inline_cell_size(key, value);
                    cells.push(LeafCell::Inline(key, value));
                }
                Op::Remove(_) => {
                    if existing.is_some() {
                        changed = true;
                        runs.extend(old_run);
                    }
                }
                Op::Add(key, delta) => {
                    let current = match existing {
                        Some(ValueRef::Inline(v)) => Some(Value::Inline(v.to_vec())),
                        Some(ValueRef::Overflow { r, len }) => Some(Value::Overflow { r, len }),
                        None => None,
                    };
                    let value = counter_value(current.as_ref(), *delta)?;
                    changed = true;
                    size += inline_cell_size(key, &value);
                    // the value is owned here: point the cell at it once all cells are known
                    counters.push((cells.len(), value));
                    cells.push(LeafCell::Inline(key, &[]));
                }
                Op::RemoveRange(..) => return Err(corrupted("range removal among key operations")),
            }
        }
        while i < count {
            let cell = page.leaf_cell(i)?;
            size += 2 + cell.len();
            cells.push(LeafCell::Raw(cell));
            i += 1;
        }
        if !changed {
            return Ok(Some(vec![Child { key: lower.to_vec(), pgno, fill: 0, touched: false, settled: false }]));
        }
        if cells.is_empty() || size > CAPACITY {
            return Ok(None);
        }
        for (at, value) in &counters {
            if let LeafCell::Inline(key, _) = cells[*at] {
                cells[*at] = LeafCell::Inline(key, value);
            }
        }
        let first_key = cells[0].key().to_vec();
        let bytes = encode_leaf_cells(cells.into_iter());
        for r in runs {
            self.free(r);
        }
        self.free(pgno);
        let new = self.write(bytes);
        Ok(Some(vec![Child { key: first_key, pgno: new, fill: size, touched: true, settled }]))
    }

    fn rebuild_leaf(
        &mut self,
        entries: Vec<LeafEntry>,
        ops: &[Op],
        old: Option<u64>,
        lower: &[u8],
    ) -> io::Result<Vec<Child>> {
        let mut out = Vec::with_capacity(entries.len() + ops.len());
        let pack = if is_append(ops, entries.last().map(|e| e.key.as_slice())) { Pack::Append } else { Pack::Balanced };
        let mut rest = entries.into_iter().peekable();
        let mut changed = false;
        for op in ops {
            while rest.peek().is_some_and(|e| e.key.as_slice() < op.key()) {
                out.push(rest.next().unwrap());
            }
            let existing = rest.next_if(|e| e.key.as_slice() == op.key());
            if let Some(LeafEntry { value: Value::Overflow { r, .. }, .. }) = existing {
                self.free(r);
            }
            match op {
                Op::Put(key, value) => {
                    changed = true;
                    let value = if inline_fits(key.len(), value.len()) {
                        Value::Inline(value.clone())
                    } else {
                        Value::Overflow { r: self.write_value(value.clone()), len: value.len() }
                    };
                    out.push(LeafEntry { key: key.clone(), value });
                }
                Op::Remove(_) => changed |= existing.is_some(),
                Op::Add(key, delta) => {
                    changed = true;
                    let value = counter_value(existing.as_ref().map(|e| &e.value), *delta)?;
                    out.push(LeafEntry { key: key.clone(), value: Value::Inline(value) });
                }
                Op::RemoveRange(..) => return Err(corrupted("range removal among key operations")),
            }
        }
        out.extend(rest);
        if !changed {
            return Ok(old.map(|pgno| Child { key: lower.to_vec(), pgno, fill: 0, touched: false, settled: false }).into_iter().collect());
        }
        if let Some(pgno) = old {
            self.free(pgno);
        }
        Ok(self.write_leaves(out, None, pack))
    }

    fn modify_branch(&mut self, pgno: u64, page: Page, lower: &[u8], ops: &[Op], depth: usize) -> io::Result<Vec<Child>> {
        let count = page.count();
        // route the ops to the children they fall into, without decoding the page
        let mut routes: Vec<(usize, Vec<u8>, std::ops::Range<usize>)> = Vec::new();
        let mut start = 0;
        while start < ops.len() {
            let i = page.branch_search(ops[start].key())?;
            let end = if i + 1 < count {
                let next = page.branch_key(i + 1)?;
                start + ops[start..].partition_point(|op| compare(op.key(), next).is_lt())
            } else {
                ops.len()
            };
            let child_lower = if i == 0 { lower.to_vec() } else { page.branch_key(i)?.to_vec() };
            routes.push((i, child_lower, start..end));
            start = end;
        }
        let mut results: Vec<(usize, Vec<Child>)> = Vec::with_capacity(routes.len());
        if ops.len() >= PARALLEL_OPS && routes.len() > 1 {
            // many changes under several children: rebuild the subtrees at once, each in a
            // sub-transaction of its own, then take their pages over in order
            use rayon::prelude::*;
            let (view, checkpointing, next_temp, raw_bytes) = (self.view, self.checkpointing, self.next_temp, self.raw_bytes);
            let built: Vec<io::Result<(usize, Vec<Child>, Txn)>> = routes
                .into_par_iter()
                .map(|(i, child_lower, range)| {
                    let mut sub = Txn::new(view, next_temp, raw_bytes, checkpointing);
                    let children = sub.modify(page.branch_child(i)?, &child_lower, &ops[range], depth + 1)?;
                    Ok((i, children, sub))
                })
                .collect();
            for result in built {
                let (i, children, sub) = result?;
                self.dirty.extend(sub.dirty);
                self.freed_extents.extend(sub.freed_extents);
                self.freed_temp.extend(sub.freed_temp);
                self.freed_checkpointing.extend(sub.freed_checkpointing);
                results.push((i, children));
            }
        } else {
            for (i, child_lower, range) in routes {
                results.push((i, self.modify(page.branch_child(i)?, &child_lower, &ops[range], depth + 1)?));
            }
        }
        // The common case: each child was replaced by one page that needs no merging. Keep the
        // separators (still valid: a child's keys stay within its range) and repoint the cells.
        let one_for_one =
            results.iter().all(|(_, r)| r.len() == 1 && (!r[0].touched || r[0].settled || r[0].fill >= MIN_FILL));
        if one_for_one {
            let mut patched: Option<Vec<u8>> = None;
            for (i, r) in &results {
                if r[0].pgno != page.branch_child(*i)? {
                    let bytes = patched.get_or_insert_with(|| page.0.to_vec());
                    set_branch_child(bytes, *i, r[0].pgno)?;
                }
            }
            let Some(bytes) = patched else {
                return Ok(vec![Child { key: lower.to_vec(), pgno, fill: 0, touched: false, settled: false }]);
            };
            let fill = page.used_bytes()?;
            self.free(pgno);
            let new = self.write(bytes);
            return Ok(vec![Child { key: lower.to_vec(), pgno: new, fill, touched: true, settled: false }]);
        }

        let entries = decode_branch(page, lower, self.view)?;
        // appends all land in the last child; keys equal to its separator belong to it too
        let last_separator = entries.last().filter(|_| entries.len() > 1).map(|e| e.key.as_slice());
        let pack = if ops.iter().all(|op| matches!(op, Op::Put(..))) && last_separator.is_none_or(|k| k <= ops[0].key()) {
            Pack::Append
        } else {
            Pack::Balanced
        };
        let mut out = Vec::with_capacity(entries.len() + 1);
        let mut results = results.into_iter().peekable();
        for (i, e) in entries.iter().enumerate() {
            match results.next_if(|(j, _)| *j == i) {
                Some((_, children)) => out.extend(children),
                None => out.push(Child { key: e.key.clone(), pgno: e.child, fill: 0, touched: false, settled: false }),
            }
        }
        self.rebalance(&mut out)?;
        self.free(pgno);
        let entries = out.into_iter().map(|c| BranchEntry { key: c.key, child: c.pgno }).collect();
        Ok(self.write_branches(entries, None, pack))
    }

    /// Merges each under-filled rebuilt child with a neighbour.
    fn rebalance(&mut self, children: &mut Vec<Child>) -> io::Result<()> {
        let mut j = 0;
        while j < children.len() {
            let child = &children[j];
            if children.len() > 1 && child.touched && !child.settled && child.fill < MIN_FILL {
                let (l, r) = if j + 1 < children.len() { (j, j + 1) } else { (j - 1, j) };
                let merged = self.merge(&children[l], &children[r])?;
                let n = merged.len();
                children.splice(l..=r, merged);
                j = l + n;
            } else {
                j += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, a: &Child, b: &Child) -> io::Result<Vec<Child>> {
        let merged = match (self.decode(a.pgno, &a.key)?, self.decode(b.pgno, &b.key)?) {
            (Node::Leaf(mut entries), Node::Leaf(right)) => {
                entries.extend(right);
                self.free(a.pgno);
                self.free(b.pgno);
                self.write_leaves(entries, Some(&a.key), Pack::Balanced)
            }
            (Node::Branch(mut entries), Node::Branch(right)) => {
                entries.extend(right);
                self.free(a.pgno);
                self.free(b.pgno);
                self.write_branches(entries, Some(&a.key), Pack::Balanced)
            }
            _ => return Err(corrupted("siblings of different kinds")),
        };
        // merged pages are settled; do not merge them again in this pass
        Ok(merged.into_iter().map(|c| Child { touched: false, ..c }).collect())
    }

    fn write_leaves(&mut self, entries: Vec<LeafEntry>, first_key: Option<&[u8]>, pack: Pack) -> Vec<Child> {
        if entries.is_empty() {
            return Vec::new();
        }
        let sizes: Vec<usize> = entries.iter().map(leaf_entry_size).collect();
        let chunks = if pack == Pack::Append { split_full(&sizes) } else { split_points(&sizes) };
        chunks
            .into_iter()
            .enumerate()
            .map(|(n, range)| {
                let key = match first_key {
                    Some(k) if n == 0 => k.to_vec(),
                    _ => entries[range.start].key.clone(),
                };
                let fill = sizes[range.clone()].iter().sum();
                let pgno = self.write(encode_leaf(&entries[range]));
                Child { key, pgno, fill, touched: true, settled: pack == Pack::Append }
            })
            .collect()
    }

    fn write_branches(&mut self, entries: Vec<BranchEntry>, first_key: Option<&[u8]>, pack: Pack) -> Vec<Child> {
        if entries.is_empty() {
            return Vec::new();
        }
        let sizes: Vec<usize> = entries.iter().map(branch_entry_size).collect();
        let chunks = if pack == Pack::Append { split_full(&sizes) } else { split_points(&sizes) };
        chunks
            .into_iter()
            .enumerate()
            .map(|(n, range)| {
                let key = match first_key {
                    Some(k) if n == 0 => k.to_vec(),
                    _ => entries[range.start].key.clone(),
                };
                let fill = sizes[range.clone()].iter().sum();
                let pgno = self.write(encode_branch(&entries[range]));
                Child { key, pgno, fill, touched: true, settled: pack == Pack::Append }
            })
            .collect()
    }
}

/// Refs to pages a checkpoint has written come out physical.
fn decode_leaf(page: Page, view: View) -> io::Result<Vec<LeafEntry>> {
    (0..page.count())
        .map(|i| {
            let key = page.leaf_key(i)?.to_vec();
            let value = match page.leaf_value(i)? {
                ValueRef::Inline(v) => Value::Inline(v.to_vec()),
                ValueRef::Overflow { r, len } => Value::Overflow { r: view.pages.normalize(r), len },
            };
            Ok(LeafEntry { key, value })
        })
        .collect()
}

/// Cell 0 of a branch has no key on disk; its real lower bound comes from the parent.
fn decode_branch(page: Page, lower: &[u8], view: View) -> io::Result<Vec<BranchEntry>> {
    (0..page.count())
        .map(|i| {
            let key = if i == 0 { lower.to_vec() } else { page.branch_key(i)?.to_vec() };
            Ok(BranchEntry { key, child: view.pages.normalize(page.branch_child(i)?) })
        })
        .collect()
}

/// Points refs to pages a checkpoint has written (branch children, overflow values) at the
/// written pages, so that temporary refs stop being copied from page to page.
fn normalize_page(bytes: &mut [u8], view: View) {
    if !view.pages.has_translations() {
        return;
    }
    let (kind, count) = (bytes[0], Page(bytes).count());
    for i in 0..count {
        let r = match kind {
            KIND_BRANCH => Page(bytes).branch_child(i).ok(),
            KIND_LEAF => match Page(bytes).leaf_value(i) {
                Ok(ValueRef::Overflow { r, .. }) => Some(r),
                _ => None,
            },
            _ => None,
        };
        let Some(r) = r.filter(|&r| is_temp(r)) else { continue };
        if let Some(written) = view.pages.translated(r) {
            let _ = match kind {
                KIND_BRANCH => set_branch_child(bytes, i, written),
                _ => set_leaf_overflow(bytes, i, written),
            };
        }
    }
}
