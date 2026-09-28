//! Copy-on-write B+ tree. A commit never touches pages of the published tree: every page on
//! the path to a modified key is rebuilt into a freshly allocated page.

use std::collections::HashMap;
use std::io;

use crate::freelist::FreeList;
use crate::page::*;

const MIN_FILL: usize = CAPACITY / 4;
const MAX_DEPTH: usize = 64;

pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Put(k, _) | Op::Remove(k) => k,
        }
    }
}

pub fn page_at(map: &[u8], pgno: u64) -> io::Result<Page<'_>> {
    let start = (pgno as usize).checked_mul(PAGE_SIZE).ok_or_else(|| corrupted("page number"))?;
    map.get(start..start + PAGE_SIZE).map(Page).ok_or_else(|| corrupted("page out of bounds"))
}

/// Looks `key` up in the tree rooted at `root`; the returned slice borrows the map.
pub fn get<'a>(map: &'a [u8], root: u64, key: &[u8]) -> io::Result<Option<&'a [u8]>> {
    if root == 0 {
        return Ok(None);
    }
    let mut pgno = root;
    for _ in 0..MAX_DEPTH {
        let page = page_at(map, pgno)?;
        match page.kind() {
            KIND_BRANCH if page.count() > 0 => pgno = page.branch_child(page.branch_search(key)?)?,
            KIND_LEAF => {
                let Ok(i) = page.leaf_search(key)? else { return Ok(None) };
                return match page.leaf_value(i)? {
                    ValueRef::Inline(v) => Ok(Some(v)),
                    ValueRef::Overflow { pgno, len } => {
                        let start = pgno as usize * PAGE_SIZE + HEADER;
                        map.get(start..start + len).map(Some).ok_or_else(|| corrupted("overflow value"))
                    }
                };
            }
            _ => return Err(corrupted("unexpected page kind")),
        }
    }
    Err(corrupted("tree too deep"))
}

/// A write transaction: new pages live in `dirty` until the commit writes them out.
pub struct Txn<'a> {
    pub map: &'a [u8],
    pub free: &'a mut FreeList,
    /// first page number -> bytes of one page or of an overflow run
    pub dirty: HashMap<u64, Vec<u8>>,
    /// pages of the published tree that this commit stops using
    pub freed: Vec<u64>,
}

/// Reference from a parent to a (possibly rebuilt) child.
struct Child {
    key: Vec<u8>,
    pgno: u64,
    /// bytes used on the page; only meaningful when `touched`
    fill: usize,
    touched: bool,
}

impl<'a> Txn<'a> {
    pub fn new(map: &'a [u8], free: &'a mut FreeList) -> Txn<'a> {
        Txn { map, free, dirty: HashMap::new(), freed: Vec::new() }
    }

    fn page(&self, pgno: u64) -> io::Result<Page<'_>> {
        match self.dirty.get(&pgno) {
            Some(bytes) => Ok(Page(&bytes[..PAGE_SIZE])),
            None => page_at(self.map, pgno),
        }
    }

    pub fn write(&mut self, bytes: Vec<u8>) -> u64 {
        let pgno = self.free.alloc(bytes.len() / PAGE_SIZE);
        self.dirty.insert(pgno, bytes);
        pgno
    }

    pub fn free(&mut self, pgno: u64, n: usize) {
        let pages = pgno..pgno + n as u64;
        if self.dirty.remove(&pgno).is_some() {
            self.free.give_back(pages);
        } else {
            self.freed.extend(pages);
        }
    }

    /// Applies `ops` (sorted by key, one op per key) to the tree at `root`; returns the new root.
    pub fn apply(&mut self, root: u64, ops: &[Op]) -> io::Result<u64> {
        let mut level =
            if root == 0 { self.rebuild_leaf(Vec::new(), ops, None, &[])? } else { self.modify(root, &[], ops, 0)? };
        let mut root = loop {
            match level.len() {
                0 => break 0,
                1 => break level[0].pgno,
                _ => {
                    let entries = level.into_iter().map(|c| BranchEntry { key: c.key, child: c.pgno }).collect();
                    level = self.write_branches(entries, None);
                }
            }
        };
        while root != 0 {
            let page = self.page(root)?;
            if page.kind() != KIND_BRANCH || page.count() != 1 {
                break;
            }
            let child = page.branch_child(0)?;
            self.free(root, 1);
            root = child;
        }
        Ok(root)
    }

    fn modify(&mut self, pgno: u64, lower: &[u8], ops: &[Op], depth: usize) -> io::Result<Vec<Child>> {
        if depth > MAX_DEPTH {
            return Err(corrupted("tree too deep"));
        }
        let page = self.page(pgno)?;
        match page.kind() {
            KIND_LEAF => {
                let entries = decode_leaf(page)?;
                self.rebuild_leaf(entries, ops, Some(pgno), lower)
            }
            KIND_BRANCH if page.count() > 0 => {
                let entries = decode_branch(page, lower)?;
                self.modify_branch(pgno, entries, ops, depth)
            }
            _ => Err(corrupted("unexpected page kind")),
        }
    }

    fn rebuild_leaf(
        &mut self,
        entries: Vec<LeafEntry>,
        ops: &[Op],
        old: Option<u64>,
        lower: &[u8],
    ) -> io::Result<Vec<Child>> {
        let mut out = Vec::with_capacity(entries.len() + ops.len());
        let mut rest = entries.into_iter().peekable();
        let mut changed = false;
        for op in ops {
            while rest.peek().is_some_and(|e| e.key.as_slice() < op.key()) {
                out.push(rest.next().unwrap());
            }
            let existing = rest.next_if(|e| e.key.as_slice() == op.key());
            if let Some(LeafEntry { value: Value::Overflow { pgno, len }, .. }) = existing {
                self.free(pgno, overflow_run_len(len));
            }
            match op {
                Op::Put(key, value) => {
                    changed = true;
                    let value = if inline_fits(key.len(), value.len()) {
                        Value::Inline(value.clone())
                    } else {
                        Value::Overflow { pgno: self.write(encode_overflow(value)), len: value.len() }
                    };
                    out.push(LeafEntry { key: key.clone(), value });
                }
                Op::Remove(_) => changed |= existing.is_some(),
            }
        }
        out.extend(rest);
        if !changed {
            return Ok(old.map(|pgno| Child { key: lower.to_vec(), pgno, fill: 0, touched: false }).into_iter().collect());
        }
        if let Some(pgno) = old {
            self.free(pgno, 1);
        }
        Ok(self.write_leaves(out, None))
    }

    fn modify_branch(&mut self, pgno: u64, entries: Vec<BranchEntry>, ops: &[Op], depth: usize) -> io::Result<Vec<Child>> {
        let mut out = Vec::with_capacity(entries.len() + 1);
        let mut start = 0;
        for (i, e) in entries.iter().enumerate() {
            let end = match entries.get(i + 1) {
                Some(next) => start + ops[start..].partition_point(|op| op.key() < next.key.as_slice()),
                None => ops.len(),
            };
            if end > start {
                out.extend(self.modify(e.child, &e.key, &ops[start..end], depth + 1)?);
            } else {
                out.push(Child { key: e.key.clone(), pgno: e.child, fill: 0, touched: false });
            }
            start = end;
        }
        if !out.iter().any(|c| c.touched) && out.len() == entries.len() {
            return Ok(vec![Child { key: entries[0].key.clone(), pgno, fill: 0, touched: false }]);
        }
        self.rebalance(&mut out)?;
        self.free(pgno, 1);
        let entries = out.into_iter().map(|c| BranchEntry { key: c.key, child: c.pgno }).collect();
        Ok(self.write_branches(entries, None))
    }

    /// Merges each under-filled rebuilt child with a neighbour.
    fn rebalance(&mut self, children: &mut Vec<Child>) -> io::Result<()> {
        let mut j = 0;
        while j < children.len() {
            if children.len() > 1 && children[j].touched && children[j].fill < MIN_FILL {
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
        let (pa, pb) = (self.page(a.pgno)?, self.page(b.pgno)?);
        if pa.kind() != pb.kind() {
            return Err(corrupted("siblings of different kinds"));
        }
        let merged = if pa.kind() == KIND_LEAF {
            let mut entries = decode_leaf(pa)?;
            entries.extend(decode_leaf(pb)?);
            self.free(a.pgno, 1);
            self.free(b.pgno, 1);
            self.write_leaves(entries, Some(&a.key))
        } else {
            let mut entries = decode_branch(pa, &a.key)?;
            entries.extend(decode_branch(pb, &b.key)?);
            self.free(a.pgno, 1);
            self.free(b.pgno, 1);
            self.write_branches(entries, Some(&a.key))
        };
        // merged pages are settled; do not merge them again in this pass
        Ok(merged.into_iter().map(|c| Child { touched: false, ..c }).collect())
    }

    fn write_leaves(&mut self, entries: Vec<LeafEntry>, first_key: Option<&[u8]>) -> Vec<Child> {
        if entries.is_empty() {
            return Vec::new();
        }
        let sizes: Vec<usize> = entries.iter().map(leaf_entry_size).collect();
        split_points(&sizes)
            .into_iter()
            .enumerate()
            .map(|(n, range)| {
                let key = match first_key {
                    Some(k) if n == 0 => k.to_vec(),
                    _ => entries[range.start].key.clone(),
                };
                let fill = sizes[range.clone()].iter().sum();
                let pgno = self.write(encode_leaf(&entries[range]));
                Child { key, pgno, fill, touched: true }
            })
            .collect()
    }

    fn write_branches(&mut self, entries: Vec<BranchEntry>, first_key: Option<&[u8]>) -> Vec<Child> {
        if entries.is_empty() {
            return Vec::new();
        }
        let sizes: Vec<usize> = entries.iter().map(branch_entry_size).collect();
        split_points(&sizes)
            .into_iter()
            .enumerate()
            .map(|(n, range)| {
                let key = match first_key {
                    Some(k) if n == 0 => k.to_vec(),
                    _ => entries[range.start].key.clone(),
                };
                let fill = sizes[range.clone()].iter().sum();
                let pgno = self.write(encode_branch(&entries[range]));
                Child { key, pgno, fill, touched: true }
            })
            .collect()
    }
}

fn decode_leaf(page: Page) -> io::Result<Vec<LeafEntry>> {
    (0..page.count())
        .map(|i| {
            let key = page.leaf_key(i)?.to_vec();
            let value = match page.leaf_value(i)? {
                ValueRef::Inline(v) => Value::Inline(v.to_vec()),
                ValueRef::Overflow { pgno, len } => Value::Overflow { pgno, len },
            };
            Ok(LeafEntry { key, value })
        })
        .collect()
}

/// Cell 0 of a branch has no key on disk; its real lower bound comes from the parent.
fn decode_branch(page: Page, lower: &[u8]) -> io::Result<Vec<BranchEntry>> {
    (0..page.count())
        .map(|i| {
            let key = if i == 0 { lower.to_vec() } else { page.branch_key(i)?.to_vec() };
            Ok(BranchEntry { key, child: page.branch_child(i)? })
        })
        .collect()
}
