//! On-disk page layout.
//!
//! Every non-meta page starts with a 16-byte header:
//!   [kind u8][reserved u8][count u16][overflow_pages u32][next u64]
//!
//! Branch and leaf pages are slotted: `count` u16 cell offsets follow the
//! header, cells are packed from the end of the page towards the slots.
//!   leaf cell:   [key_len u16][flags u8][val_len u32][key][value | overflow pgno u64]
//!   branch cell: [child u64][key_len u16][key]   (key of cell 0 is always empty = -inf)
//!
//! An overflow value occupies a run of consecutive pages; the value bytes start
//! right after the header of the first page and continue across the run.

use std::io;

pub const PAGE_SIZE: usize = 4096;
pub const HEADER: usize = 16;
pub const CAPACITY: usize = PAGE_SIZE - HEADER;
/// A cell never takes more than half of a page, so any page split yields at least two cells per page.
pub const MAX_CELL: usize = CAPACITY / 2 - 2;
pub const MAX_KEY_SIZE: usize = 1978;

pub const KIND_BRANCH: u8 = 1;
pub const KIND_LEAF: u8 = 2;
pub const KIND_OVERFLOW: u8 = 3;
pub const KIND_FREELIST: u8 = 4;

const FLAG_OVERFLOW: u8 = 1;
const LEAF_CELL_HEADER: usize = 7;
const BRANCH_CELL_HEADER: usize = 10;

pub fn corrupted(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("mostik: database is corrupted ({what})"))
}

fn u16_at(b: &[u8], at: usize) -> usize {
    u16::from_le_bytes([b[at], b[at + 1]]) as usize
}
fn u32_at(b: &[u8], at: usize) -> usize {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap()) as usize
}
pub fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// Byte-wise key order. Compares 8 bytes at a time instead of calling libc `memcmp`,
/// which dominates lookups of short keys that share a prefix (`user:1`, `user:2`, ...).
#[inline(always)]
pub fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i + 8 <= n {
        let x = u64::from_be_bytes(a[i..i + 8].try_into().unwrap());
        let y = u64::from_be_bytes(b[i..i + 8].try_into().unwrap());
        if x != y {
            return x.cmp(&y);
        }
        i += 8;
    }
    while i < n {
        if a[i] != b[i] {
            return a[i].cmp(&b[i]);
        }
        i += 1;
    }
    a.len().cmp(&b.len())
}

/// Borrowed view of one page.
#[derive(Clone, Copy)]
pub struct Page<'a>(pub &'a [u8]);

pub enum ValueRef<'a> {
    Inline(&'a [u8]),
    Overflow { pgno: u64, len: usize },
}

impl<'a> Page<'a> {
    #[inline(always)]
    pub fn kind(&self) -> u8 {
        self.0[0]
    }
    #[inline(always)]
    pub fn count(&self) -> usize {
        u16_at(self.0, 2)
    }
    pub fn next(&self) -> u64 {
        u64_at(self.0, 8)
    }
    #[inline(always)]
    fn cell(&self, i: usize, cell_header: usize) -> io::Result<usize> {
        if i >= self.count() || HEADER + 2 * self.count() > PAGE_SIZE {
            return Err(corrupted("cell index"));
        }
        let off = u16_at(self.0, HEADER + 2 * i);
        if off < HEADER + 2 * self.count() || off + cell_header > PAGE_SIZE {
            return Err(corrupted("cell offset"));
        }
        Ok(off)
    }

    #[inline(always)]
    pub fn leaf_key(&self, i: usize) -> io::Result<&'a [u8]> {
        let c = self.cell(i, LEAF_CELL_HEADER)?;
        let klen = u16_at(self.0, c);
        self.0.get(c + LEAF_CELL_HEADER..c + LEAF_CELL_HEADER + klen).ok_or_else(|| corrupted("leaf key"))
    }

    pub fn leaf_value(&self, i: usize) -> io::Result<ValueRef<'a>> {
        let c = self.cell(i, LEAF_CELL_HEADER)?;
        let klen = u16_at(self.0, c);
        let flags = self.0[c + 2];
        let vlen = u32_at(self.0, c + 3);
        let at = c + LEAF_CELL_HEADER + klen;
        if flags & FLAG_OVERFLOW != 0 {
            let pgno = u64_at(self.0.get(at..at + 8).ok_or_else(|| corrupted("overflow pointer"))?, 0);
            Ok(ValueRef::Overflow { pgno, len: vlen })
        } else {
            Ok(ValueRef::Inline(self.0.get(at..at + vlen).ok_or_else(|| corrupted("leaf value"))?))
        }
    }

    #[inline(always)]
    pub fn branch_child(&self, i: usize) -> io::Result<u64> {
        let c = self.cell(i, BRANCH_CELL_HEADER)?;
        Ok(u64_at(self.0, c))
    }

    #[inline(always)]
    pub fn branch_key(&self, i: usize) -> io::Result<&'a [u8]> {
        let c = self.cell(i, BRANCH_CELL_HEADER)?;
        let klen = u16_at(self.0, c + 8);
        self.0.get(c + BRANCH_CELL_HEADER..c + BRANCH_CELL_HEADER + klen).ok_or_else(|| corrupted("branch key"))
    }

    /// Index of the leaf cell equal to `key`, or `Err(insert_position)`.
    pub fn leaf_search(&self, key: &[u8]) -> io::Result<Result<usize, usize>> {
        let (mut lo, mut hi) = (0, self.count());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match compare(self.leaf_key(mid)?, key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(Ok(mid)),
            }
        }
        Ok(Err(lo))
    }

    /// Index of the child whose range contains `key`.
    pub fn branch_search(&self, key: &[u8]) -> io::Result<usize> {
        // first index in 1..count whose key is > `key`, minus one
        let (mut lo, mut hi) = (1, self.count());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if compare(self.branch_key(mid)?, key) != std::cmp::Ordering::Greater {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo - 1)
    }
}

/// In-memory leaf value used while rebuilding pages.
#[derive(Clone, Debug)]
pub enum Value {
    Inline(Vec<u8>),
    Overflow { pgno: u64, len: usize },
}

#[derive(Clone, Debug)]
pub struct LeafEntry {
    pub key: Vec<u8>,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub struct BranchEntry {
    pub key: Vec<u8>,
    pub child: u64,
}

pub fn inline_fits(key_len: usize, val_len: usize) -> bool {
    LEAF_CELL_HEADER + key_len + val_len <= MAX_CELL
}

pub fn overflow_run_len(val_len: usize) -> usize {
    (HEADER + val_len).div_ceil(PAGE_SIZE)
}

/// Bytes a leaf entry takes on a page, slot included.
pub fn leaf_entry_size(e: &LeafEntry) -> usize {
    2 + LEAF_CELL_HEADER
        + e.key.len()
        + match &e.value {
            Value::Inline(v) => v.len(),
            Value::Overflow { .. } => 8,
        }
}

pub fn branch_entry_size(e: &BranchEntry) -> usize {
    2 + BRANCH_CELL_HEADER + e.key.len()
}

fn write_header(page: &mut [u8], kind: u8, count: usize) {
    page[0] = kind;
    page[2..4].copy_from_slice(&(count as u16).to_le_bytes());
}

pub fn encode_leaf(entries: &[LeafEntry]) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SIZE];
    write_header(&mut page, KIND_LEAF, entries.len());
    let mut end = PAGE_SIZE;
    for (i, e) in entries.iter().enumerate() {
        let size = leaf_entry_size(e) - 2;
        end -= size;
        let c = end;
        page[c..c + 2].copy_from_slice(&(e.key.len() as u16).to_le_bytes());
        let at = c + LEAF_CELL_HEADER + e.key.len();
        page[c + LEAF_CELL_HEADER..at].copy_from_slice(&e.key);
        match &e.value {
            Value::Inline(v) => {
                page[c + 3..c + 7].copy_from_slice(&(v.len() as u32).to_le_bytes());
                page[at..at + v.len()].copy_from_slice(v);
            }
            Value::Overflow { pgno, len } => {
                page[c + 2] = FLAG_OVERFLOW;
                page[c + 3..c + 7].copy_from_slice(&(*len as u32).to_le_bytes());
                page[at..at + 8].copy_from_slice(&pgno.to_le_bytes());
            }
        }
        page[HEADER + 2 * i..HEADER + 2 * i + 2].copy_from_slice(&(c as u16).to_le_bytes());
    }
    debug_assert!(HEADER + 2 * entries.len() <= end);
    page
}

pub fn encode_branch(entries: &[BranchEntry]) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SIZE];
    write_header(&mut page, KIND_BRANCH, entries.len());
    let mut end = PAGE_SIZE;
    for (i, e) in entries.iter().enumerate() {
        let key: &[u8] = if i == 0 { &[] } else { &e.key };
        end -= BRANCH_CELL_HEADER + key.len();
        let c = end;
        page[c..c + 8].copy_from_slice(&e.child.to_le_bytes());
        page[c + 8..c + 10].copy_from_slice(&(key.len() as u16).to_le_bytes());
        page[c + BRANCH_CELL_HEADER..c + BRANCH_CELL_HEADER + key.len()].copy_from_slice(key);
        page[HEADER + 2 * i..HEADER + 2 * i + 2].copy_from_slice(&(c as u16).to_le_bytes());
    }
    debug_assert!(HEADER + 2 * entries.len() <= end);
    page
}

pub fn encode_overflow(value: &[u8]) -> Vec<u8> {
    let pages = overflow_run_len(value.len());
    let mut buf = vec![0u8; pages * PAGE_SIZE];
    buf[0] = KIND_OVERFLOW;
    buf[4..8].copy_from_slice(&(pages as u32).to_le_bytes());
    buf[HEADER..HEADER + value.len()].copy_from_slice(value);
    buf
}

/// Freelist page: header (`next` links the chain) followed by `count` u64 page numbers.
pub const FREELIST_PER_PAGE: usize = CAPACITY / 8;

pub fn encode_freelist(pages: &[u64], next: u64) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SIZE];
    write_header(&mut page, KIND_FREELIST, pages.len());
    page[8..16].copy_from_slice(&next.to_le_bytes());
    for (i, p) in pages.iter().enumerate() {
        page[HEADER + 8 * i..HEADER + 8 * i + 8].copy_from_slice(&p.to_le_bytes());
    }
    page
}

pub fn decode_freelist(page: Page) -> Vec<u64> {
    (0..page.count().min(FREELIST_PER_PAGE)).map(|i| u64_at(page.0, HEADER + 8 * i)).collect()
}

/// Splits consecutive entries of the given sizes into chunks that each fit on one page,
/// keeping chunks roughly equal in size.
pub fn split_points(sizes: &[usize]) -> Vec<std::ops::Range<usize>> {
    let total: usize = sizes.iter().sum();
    if total <= CAPACITY {
        return vec![0..sizes.len()];
    }
    let mut chunks = Vec::new();
    let mut remaining_total = total;
    let mut remaining_chunks = total.div_ceil(CAPACITY);
    let mut start = 0;
    let mut cur = 0;
    for (i, &s) in sizes.iter().enumerate() {
        let target = remaining_total.div_ceil(remaining_chunks.max(1));
        if cur > 0 && (cur + s > target || cur + s > CAPACITY) {
            chunks.push(start..i);
            remaining_total -= cur;
            remaining_chunks = remaining_chunks.saturating_sub(1).max(1);
            start = i;
            cur = 0;
        }
        cur += s;
    }
    chunks.push(start..sizes.len());
    chunks
}

#[cfg(test)]
mod tests {
    use super::compare;
    use rand::{Rng, SeedableRng};

    #[test]
    fn compare_matches_slice_order() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        for _ in 0..200_000 {
            let len_a = rng.gen_range(0..24);
            let len_b = rng.gen_range(0..24);
            // small alphabet so long shared prefixes are common
            let a: Vec<u8> = (0..len_a).map(|_| rng.gen_range(0..3) * 127).collect();
            let b: Vec<u8> = (0..len_b).map(|_| rng.gen_range(0..3) * 127).collect();
            assert_eq!(compare(&a, &b), a.cmp(&b), "{a:?} vs {b:?}");
        }
    }
}
