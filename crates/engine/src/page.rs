//! Page layout.
//!
//! Tree pages are PAGE_SIZE bytes in memory. They start with a 16-byte header:
//!   [kind u8][reserved u8][count u16][reserved u32][next u64]
//! and are slotted: `count` u16 cell offsets follow the header, cells are packed from the end
//! of the page towards the slots.
//!   leaf cell:   [key_len u16][flags u8][val_len u32][key][value | overflow ref u64]
//!   branch cell: [child ref u64][key_len u16][key]   (key of cell 0 is always empty = -inf)
//!
//! Pages refer to each other by `Ref`: a page written by a checkpoint has a physical ref
//! (where its extent starts on disk, in BLOCK units, and how many blocks it takes); a page
//! changed since the last checkpoint lives only in memory under a temporary ref.
//!
//! On disk a page is an extent of whole blocks: [kind u8][codec u8][packed_len u16]
//! [logical_len u32][stored_len u32][crc32 u32] and the page, LZ4-compressed when that saves
//! space. A leaf is packed first (`pack_leaf`): the prefix its keys share is stored once and
//! cell headers shrink to varints; `packed_len` is its packed size. Large values ("overflow"
//! values) are extents too, holding the value bytes.

use std::io;

/// Unit of disk allocation: small, so a compressed page wastes little of its last block.
/// Blocks 0 and 1 hold the meta records.
pub const BLOCK: usize = 128;
/// Size of a tree page in memory.
pub const PAGE_SIZE: usize = 8192;
pub const HEADER: usize = 16;
pub const CAPACITY: usize = PAGE_SIZE - HEADER;
/// A cell never takes more than half of a page, so any page split yields at least two cells per page.
pub const MAX_CELL: usize = CAPACITY / 2 - 2;
pub const MAX_KEY_SIZE: usize = 1978;

pub const KIND_BRANCH: u8 = 1;
pub const KIND_LEAF: u8 = 2;
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

/// Where a page or overflow value is: on disk (physical) or in memory only (temporary).
pub type Ref = u64;

const TEMP: u64 = 1 << 63;
/// Temporary refs of overflow values carry this bit: their bytes are a value, not a tree page.
const TEMP_OVERFLOW: u64 = 1 << 62;
const BLOCK_BITS: u32 = 40;
/// enough blocks for a 4 GiB value
const BLOCKS_MASK: u64 = (1 << 23) - 1;

pub fn temp_ref(id: u64, overflow: bool) -> Ref {
    TEMP | if overflow { TEMP_OVERFLOW } else { 0 } | id
}

pub fn is_temp(r: Ref) -> bool {
    r & TEMP != 0
}

pub fn is_temp_overflow(r: Ref) -> bool {
    r & (TEMP | TEMP_OVERFLOW) == TEMP | TEMP_OVERFLOW
}

pub fn phys_ref(block: u64, blocks: u64) -> Ref {
    debug_assert!(block < 1 << BLOCK_BITS && blocks > 0 && blocks <= BLOCKS_MASK);
    block | blocks << BLOCK_BITS
}

/// First block and block count of a physical ref.
pub fn extent_of(r: Ref) -> (u64, u64) {
    (r & ((1 << BLOCK_BITS) - 1), (r >> BLOCK_BITS) & BLOCKS_MASK)
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

/// Borrowed view of one tree page (or a freelist block).
#[derive(Clone, Copy)]
pub struct Page<'a>(pub &'a [u8]);

pub enum ValueRef<'a> {
    Inline(&'a [u8]),
    Overflow { r: Ref, len: usize },
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
        let size = self.0.len();
        if i >= self.count() || HEADER + 2 * self.count() > size {
            return Err(corrupted("cell index"));
        }
        let off = u16_at(self.0, HEADER + 2 * i);
        if off < HEADER + 2 * self.count() || off + cell_header > size {
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
            let r = u64_at(self.0.get(at..at + 8).ok_or_else(|| corrupted("overflow pointer"))?, 0);
            Ok(ValueRef::Overflow { r, len: vlen })
        } else {
            Ok(ValueRef::Inline(self.0.get(at..at + vlen).ok_or_else(|| corrupted("leaf value"))?))
        }
    }

    /// The raw bytes of leaf cell `i`, as `encode_leaf` wrote them.
    #[inline(always)]
    pub fn leaf_cell(&self, i: usize) -> io::Result<&'a [u8]> {
        let c = self.cell(i, LEAF_CELL_HEADER)?;
        let klen = u16_at(self.0, c);
        let vlen = if self.0[c + 2] & FLAG_OVERFLOW != 0 { 8 } else { u32_at(self.0, c + 3) };
        self.0.get(c..c + LEAF_CELL_HEADER + klen + vlen).ok_or_else(|| corrupted("leaf cell"))
    }

    /// Bytes in use, slots included (cells are packed at the end of the page).
    pub fn used_bytes(&self) -> io::Result<usize> {
        let mut lowest = self.0.len();
        for i in 0..self.count() {
            lowest = lowest.min(self.cell(i, 0)?);
        }
        Ok(self.0.len() - lowest + 2 * self.count())
    }

    pub fn branch_child(&self, i: usize) -> io::Result<Ref> {
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

/// Whether a tree page refers to pages or values that exist only in memory (temporary refs);
/// true for what cannot be read, so that callers take the careful path.
pub fn refers_to_temp(page: &[u8]) -> bool {
    let p = Page(page);
    match p.kind() {
        KIND_BRANCH => (0..p.count()).any(|i| p.branch_child(i).map_or(true, is_temp)),
        KIND_LEAF => (0..p.count()).any(|i| match p.leaf_value(i) {
            Ok(ValueRef::Inline(_)) => false,
            Ok(ValueRef::Overflow { r, .. }) => is_temp(r),
            Err(_) => true,
        }),
        _ => true,
    }
}

/// In-memory leaf value used while rebuilding pages.
#[derive(Clone, Debug)]
pub enum Value {
    Inline(Vec<u8>),
    Overflow { r: Ref, len: usize },
}

#[derive(Clone, Debug)]
pub struct LeafEntry {
    pub key: Vec<u8>,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub struct BranchEntry {
    pub key: Vec<u8>,
    pub child: Ref,
}

pub fn inline_fits(key_len: usize, val_len: usize) -> bool {
    LEAF_CELL_HEADER + key_len + val_len <= MAX_CELL
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

/// Points branch cell `i` of `page` at `child`.
pub fn set_branch_child(page: &mut [u8], i: usize, child: Ref) -> io::Result<()> {
    let c = Page(page).cell(i, BRANCH_CELL_HEADER)?;
    page[c..c + 8].copy_from_slice(&child.to_le_bytes());
    Ok(())
}

/// Points the overflow value of leaf cell `i` at `r` (the cell must hold an overflow value).
pub fn set_leaf_overflow(page: &mut [u8], i: usize, r: Ref) -> io::Result<()> {
    let c = Page(page).cell(i, LEAF_CELL_HEADER)?;
    let at = c + LEAF_CELL_HEADER + u16_at(page, c);
    page.get_mut(at..at + 8).ok_or_else(|| corrupted("overflow pointer"))?.copy_from_slice(&r.to_le_bytes());
    Ok(())
}

/// Size of an inline leaf cell for `key` and `value`, slot included.
pub fn inline_cell_size(key: &[u8], value: &[u8]) -> usize {
    2 + LEAF_CELL_HEADER + key.len() + value.len()
}

/// A leaf page built from raw cells (`encode_leaf`'s cell format), in key order.
pub fn encode_leaf_cells<'a>(cells: impl ExactSizeIterator<Item = LeafCell<'a>>) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SIZE];
    write_header(&mut page, KIND_LEAF, cells.len());
    let mut end = PAGE_SIZE;
    for (i, cell) in cells.enumerate() {
        match cell {
            LeafCell::Raw(bytes) => {
                end -= bytes.len();
                page[end..end + bytes.len()].copy_from_slice(bytes);
            }
            LeafCell::Inline(key, value) => {
                end -= LEAF_CELL_HEADER + key.len() + value.len();
                let c = end;
                page[c..c + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
                page[c + 3..c + 7].copy_from_slice(&(value.len() as u32).to_le_bytes());
                page[c + LEAF_CELL_HEADER..c + LEAF_CELL_HEADER + key.len()].copy_from_slice(key);
                page[c + LEAF_CELL_HEADER + key.len()..c + LEAF_CELL_HEADER + key.len() + value.len()].copy_from_slice(value);
            }
        }
        page[HEADER + 2 * i..HEADER + 2 * i + 2].copy_from_slice(&(end as u16).to_le_bytes());
    }
    page
}

pub enum LeafCell<'a> {
    /// a cell copied from an existing page
    Raw(&'a [u8]),
    Inline(&'a [u8], &'a [u8]),
}

impl LeafCell<'_> {
    pub fn key(&self) -> &[u8] {
        match self {
            LeafCell::Raw(bytes) => &bytes[LEAF_CELL_HEADER..LEAF_CELL_HEADER + u16_at(bytes, 0)],
            LeafCell::Inline(key, _) => key,
        }
    }
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
            Value::Overflow { r, len } => {
                page[c + 2] = FLAG_OVERFLOW;
                page[c + 3..c + 7].copy_from_slice(&(*len as u32).to_le_bytes());
                page[at..at + 8].copy_from_slice(&r.to_le_bytes());
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

// ---- extents on disk ----

const EXTENT_HEADER: usize = 16;
const CODEC_RAW: u8 = 0;
const CODEC_LZ4: u8 = 1;
/// a leaf, packed (`pack_leaf`) then LZ4-compressed
const CODEC_LEAF_LZ4: u8 = 2;
pub const EXTENT_PAGE: u8 = 1;
pub const EXTENT_VALUE: u8 = 2;

/// The bytes to write for `logical` (a tree page or a value), padded to whole blocks.
pub fn encode_extent(kind: u8, logical: &[u8]) -> Vec<u8> {
    let (mut codec, mut packed_len) = (CODEC_LZ4, 0);
    let compressed = if kind != EXTENT_PAGE {
        None
    } else if let Some(packed) = pack_leaf(logical) {
        codec = CODEC_LEAF_LZ4;
        packed_len = packed.len();
        Some(lz4_flex::block::compress(&packed))
    } else {
        Some(lz4_flex::block::compress(logical))
    };
    let (codec, stored): (u8, &[u8]) = match &compressed {
        // keep the raw bytes unless compression saves at least a block
        Some(c) if (EXTENT_HEADER + c.len()).div_ceil(BLOCK) < (EXTENT_HEADER + logical.len()).div_ceil(BLOCK) => (codec, c),
        _ => (CODEC_RAW, logical),
    };
    let mut out = vec![0u8; (EXTENT_HEADER + stored.len()).div_ceil(BLOCK) * BLOCK];
    out[0] = kind;
    out[1] = codec;
    out[2..4].copy_from_slice(&(packed_len as u16).to_le_bytes());
    out[4..8].copy_from_slice(&(logical.len() as u32).to_le_bytes());
    out[8..12].copy_from_slice(&(stored.len() as u32).to_le_bytes());
    out[12..16].copy_from_slice(&crc32fast::hash(stored).to_le_bytes());
    out[EXTENT_HEADER..EXTENT_HEADER + stored.len()].copy_from_slice(stored);
    out
}

/// The logical bytes of an extent read from disk.
pub fn decode_extent(kind: u8, bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.len() < EXTENT_HEADER || bytes[0] != kind {
        return Err(corrupted("extent kind"));
    }
    let logical_len = u32_at(bytes, 4);
    let stored_len = u32_at(bytes, 8);
    let stored = bytes.get(EXTENT_HEADER..EXTENT_HEADER + stored_len).ok_or_else(|| corrupted("extent length"))?;
    if crc32fast::hash(stored) != u32_at(bytes, 12) as u32 {
        return Err(corrupted("extent checksum"));
    }
    match bytes[1] {
        CODEC_RAW if stored_len == logical_len => Ok(stored.to_vec()),
        CODEC_LZ4 => {
            let mut out = vec![0u8; logical_len];
            match lz4_flex::block::decompress_into(stored, &mut out) {
                Ok(n) if n == logical_len => Ok(out),
                _ => Err(corrupted("extent compression")),
            }
        }
        CODEC_LEAF_LZ4 if kind == EXTENT_PAGE => {
            let packed_len = u16_at(bytes, 2);
            let mut packed = vec![0u8; packed_len];
            match lz4_flex::block::decompress_into(stored, &mut packed) {
                Ok(n) if n == packed_len => unpack_leaf(&packed, logical_len),
                _ => Err(corrupted("extent compression")),
            }
        }
        _ => Err(corrupted("extent codec")),
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(b: &[u8], at: &mut usize) -> io::Result<usize> {
    let mut v = 0usize;
    for shift in (0..35).step_by(7) {
        let byte = *b.get(*at).ok_or_else(|| corrupted("packed leaf"))?;
        *at += 1;
        v |= ((byte & 0x7f) as usize) << shift;
        if byte < 0x80 {
            return Ok(v);
        }
    }
    Err(corrupted("packed leaf varint"))
}

/// A leaf in its on-disk form: the header as is, `[prefix_len][prefix]` (the prefix every key
/// shares), then per cell `[suffix_len][value_len << 1 | overflow][key suffix][value or ref]`,
/// lengths as varints. None for a page that is not a leaf.
pub fn pack_leaf(page: &[u8]) -> Option<Vec<u8>> {
    let leaf = Page(page);
    if page.len() < HEADER || leaf.kind() != KIND_LEAF || page.len() > u16::MAX as usize {
        return None;
    }
    let count = leaf.count();
    let (first, last) = if count == 0 { (&[][..], &[][..]) } else { (leaf.leaf_key(0).ok()?, leaf.leaf_key(count - 1).ok()?) };
    // keys are sorted: what the first and last share, all share
    let prefix = first.iter().zip(last).take_while(|(a, b)| a == b).count();
    let mut out = Vec::with_capacity(page.len() / 2);
    out.extend_from_slice(&page[..HEADER]);
    put_varint(&mut out, prefix);
    out.extend_from_slice(&first[..prefix]);
    for i in 0..count {
        let cell = leaf.leaf_cell(i).ok()?;
        let klen = u16_at(cell, 0);
        let overflow = cell[2] & FLAG_OVERFLOW != 0;
        put_varint(&mut out, klen - prefix);
        put_varint(&mut out, u32_at(cell, 3) << 1 | overflow as usize);
        out.extend_from_slice(&cell[LEAF_CELL_HEADER + prefix..]);
    }
    (out.len() <= u16::MAX as usize).then_some(out)
}

/// The leaf page `pack_leaf` packed, `len` bytes long.
fn unpack_leaf(packed: &[u8], len: usize) -> io::Result<Vec<u8>> {
    if packed.len() < HEADER || len < HEADER || packed[0] != KIND_LEAF {
        return Err(corrupted("packed leaf"));
    }
    let mut page = vec![0u8; len];
    page[..HEADER].copy_from_slice(&packed[..HEADER]);
    let count = u16_at(packed, 2);
    let mut at = HEADER;
    let prefix_len = get_varint(packed, &mut at)?;
    let prefix = packed.get(at..at + prefix_len).ok_or_else(|| corrupted("packed leaf"))?;
    at += prefix_len;
    let mut end = len;
    for i in 0..count {
        let klen = prefix_len + get_varint(packed, &mut at)?;
        let v = get_varint(packed, &mut at)?;
        let (vlen, overflow) = (v >> 1, v & 1 != 0);
        let stored = klen - prefix_len + if overflow { 8 } else { vlen };
        let rest = packed.get(at..at + stored).ok_or_else(|| corrupted("packed leaf"))?;
        at += stored;
        let size = LEAF_CELL_HEADER + prefix_len + stored;
        if klen > u16::MAX as usize || HEADER + 2 * count + size > end {
            return Err(corrupted("packed leaf"));
        }
        end -= size;
        let c = end;
        page[c..c + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        page[c + 2] = if overflow { FLAG_OVERFLOW } else { 0 };
        page[c + 3..c + 7].copy_from_slice(&(vlen as u32).to_le_bytes());
        page[c + LEAF_CELL_HEADER..c + LEAF_CELL_HEADER + prefix_len].copy_from_slice(prefix);
        page[c + LEAF_CELL_HEADER + prefix_len..c + size].copy_from_slice(rest);
        page[HEADER + 2 * i..HEADER + 2 * i + 2].copy_from_slice(&(c as u16).to_le_bytes());
    }
    if at != packed.len() {
        return Err(corrupted("packed leaf"));
    }
    Ok(page)
}

// ---- freelist blocks ----

/// A freelist node, FREELIST_BLOCKS blocks: header (`next` links the chain) followed by
/// `count` extents `[start u64][blocks u32]`.
pub const FREELIST_BLOCKS: u64 = 32;
pub const FREELIST_NODE: usize = FREELIST_BLOCKS as usize * BLOCK;
pub const FREELIST_PER_NODE: usize = (FREELIST_NODE - HEADER) / 12;

pub fn encode_freelist(extents: &[(u64, u64)], next: u64) -> Vec<u8> {
    let mut page = vec![0u8; FREELIST_NODE];
    write_header(&mut page, KIND_FREELIST, extents.len());
    page[8..16].copy_from_slice(&next.to_le_bytes());
    for (i, (start, len)) in extents.iter().enumerate() {
        let at = HEADER + 12 * i;
        page[at..at + 8].copy_from_slice(&start.to_le_bytes());
        page[at + 8..at + 12].copy_from_slice(&(*len as u32).to_le_bytes());
    }
    page
}

pub fn decode_freelist(page: Page) -> Vec<(u64, u64)> {
    (0..page.count().min(FREELIST_PER_NODE))
        .map(|i| {
            let at = HEADER + 12 * i;
            (u64_at(page.0, at), u32_at(page.0, at + 8) as u64)
        })
        .collect()
}

/// Splits consecutive entries into chunks that each fill a page completely, the last one
/// taking the rest. Used when keys arrive in ascending order: pages left behind stay full
/// instead of half empty.
pub fn split_full(sizes: &[usize]) -> Vec<std::ops::Range<usize>> {
    let mut chunks = Vec::new();
    let (mut start, mut cur) = (0, 0);
    for (i, &s) in sizes.iter().enumerate() {
        if cur > 0 && cur + s > CAPACITY {
            chunks.push(start..i);
            start = i;
            cur = 0;
        }
        cur += s;
    }
    chunks.push(start..sizes.len());
    chunks
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
    use super::*;
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

    #[test]
    fn extents_round_trip_and_detect_damage() {
        let page = encode_leaf(&[LeafEntry { key: b"k".to_vec(), value: Value::Inline(vec![7; 3000]) }]);
        let extent = encode_extent(EXTENT_PAGE, &page);
        assert!(extent.len() < PAGE_SIZE, "a mostly empty page compresses");
        assert_eq!(decode_extent(EXTENT_PAGE, &extent).unwrap(), page);
        let value: Vec<u8> = (0..10_000u32).map(|i| (i * 7919 % 251) as u8).collect();
        let extent = encode_extent(EXTENT_VALUE, &value);
        assert_eq!(decode_extent(EXTENT_VALUE, &extent).unwrap(), value);
        let mut damaged = extent.clone();
        damaged[100] ^= 1;
        assert!(decode_extent(EXTENT_VALUE, &damaged).is_err());
        assert!(decode_extent(EXTENT_PAGE, &extent).is_err(), "wrong kind");
    }

    #[test]
    fn leaves_pack_and_unpack() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(9);
        for n in [0usize, 1, 2, 40, 150] {
            let mut entries: Vec<LeafEntry> = (0..n)
                .map(|i| LeafEntry {
                    key: [&b"i\0bench\0docs\0"[..], &(i as u32 * 7).to_be_bytes(), &vec![b'x'; rng.gen_range(0..3)]].concat(),
                    value: if i % 7 == 3 {
                        Value::Overflow { r: phys_ref(1000 + i as u64, 3), len: 5000 + i }
                    } else {
                        Value::Inline((0..rng.gen_range(0..20)).map(|_| rng.gen()).collect())
                    },
                })
                .collect();
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            let page = encode_leaf(&entries);
            let extent = encode_extent(EXTENT_PAGE, &page);
            assert_eq!(decode_extent(EXTENT_PAGE, &extent).unwrap(), page, "n = {n}");
            let mut damaged = pack_leaf(&page).unwrap();
            damaged.truncate(damaged.len() - 1);
            assert!(n == 0 || unpack_leaf(&damaged, PAGE_SIZE).is_err());
        }
    }

    #[test]
    fn refs_pack_and_unpack() {
        let r = phys_ref(123_456_789, 17);
        assert!(!is_temp(r));
        assert_eq!(extent_of(r), (123_456_789, 17));
        assert!(is_temp(temp_ref(5, false)) && !is_temp_overflow(temp_ref(5, false)));
        assert!(is_temp_overflow(temp_ref(5, true)));
    }
}
