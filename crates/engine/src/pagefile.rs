//! Reading pages by `Ref`. Nothing is mapped into memory: pages written by checkpoints are
//! read with positioned reads, decompressed, and kept in a cache of bounded size, so the process
//! stays small however large the database grows.
//!
//! Pages changed since the last checkpoint ("dirty") live only in memory under temporary refs
//! (their changes are safe in the journal). Once a checkpoint has written one, its temporary ref
//! translates to the physical one until nothing can refer to it any more.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use quick_cache::sync::Cache;
use quick_cache::{OptionsBuilder, Weighter};

use crate::page::{corrupted, decode_extent, encode_extent, extent_of, is_temp, is_temp_overflow, refers_to_temp, Ref, BLOCK, EXTENT_PAGE, EXTENT_VALUE};

/// A page or value changed since the last checkpoint.
#[derive(Clone)]
pub enum Dirty {
    /// as built: pages replaced soon after (branches near the root, most of all) are never
    /// compressed
    Raw(Arc<[u8]>),
    /// compressed, as the extent a checkpoint will write, so that many scattered changes take
    /// little memory; with whether the page refers to temporary pages (a checkpoint must then
    /// rewrite it to point at where they went)
    Packed(Arc<[u8]>, bool),
}

/// Share of the cache for pages read more than once; the rest holds recent ones.
const CACHE_HOT_SHARE: f64 = 0.5;

/// A commit changing at least this many pages compresses them at once.
const PACK_AT_ONCE: usize = 64;

#[derive(Clone)]
struct ByteWeight;

impl Weighter<u64, Arc<[u8]>> for ByteWeight {
    fn weight(&self, _: &u64, page: &Arc<[u8]>) -> u64 {
        page.len() as u64
    }
}

pub struct PageFile {
    file: File,
    /// first block of an extent, or temporary ref -> logical bytes of a tree page
    cache: Cache<u64, Arc<[u8]>, ByteWeight>,
    /// temporary ref -> a page or value changed since the last checkpoint
    dirty: RwLock<HashMap<Ref, Dirty>>,
    /// bytes of the `Dirty::Raw` entries
    raw_bytes: AtomicUsize,
    /// temporary ref -> physical ref, for pages a checkpoint has written
    translation: RwLock<HashMap<Ref, Ref>>,
    /// blocks the file holds
    end: AtomicU64,
}

impl PageFile {
    pub fn new(file: File, cache_bytes: usize, end: u64) -> PageFile {
        PageFile {
            file,
            cache: Cache::with_options(
                OptionsBuilder::new()
                    .estimated_items_capacity(cache_bytes / 8192 + 16)
                    .weight_capacity(cache_bytes as u64)
                    // half for pages seen once: a page read and then changed soon after (a
                    // commit's conditions, then its writes) must still be there
                    .hot_allocation(CACHE_HOT_SHARE)
                    .build()
                    .expect("cache options"),
                ByteWeight,
                Default::default(),
                Default::default(),
            ),
            dirty: RwLock::new(HashMap::new()),
            raw_bytes: AtomicUsize::new(0),
            translation: RwLock::new(HashMap::new()),
            end: AtomicU64::new(end),
        }
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn set_end(&self, end: u64) {
        self.end.store(end, Ordering::Release);
    }

    pub fn dirty_get(&self, r: Ref) -> Option<Dirty> {
        self.dirty.read().unwrap().get(&r).cloned()
    }

    pub fn raw_bytes(&self) -> usize {
        self.raw_bytes.load(Ordering::Acquire)
    }

    /// Keeps new pages and values. Those of a small commit stay as they are; a large commit's
    /// are compressed right away, in parallel. Returns the memory each takes.
    pub fn add_dirty(&self, pages: Vec<(Ref, Vec<u8>)>) -> Vec<(Ref, usize)> {
        use rayon::prelude::*;
        if pages.len() < PACK_AT_ONCE {
            let sizes: Vec<(Ref, usize)> = pages.iter().map(|(r, page)| (*r, page.len())).collect();
            self.raw_bytes.fetch_add(sizes.iter().map(|(_, size)| size).sum(), Ordering::AcqRel);
            let pages: Vec<(Ref, Arc<[u8]>)> = pages.into_iter().map(|(r, page)| (r, Arc::from(page))).collect();
            // tree pages are read through the cache (sharded) rather than the dirty map (one lock)
            for (r, page) in &pages {
                if !is_temp_overflow(*r) {
                    self.cache.insert(*r, page.clone());
                }
            }
            self.dirty.write().unwrap().extend(pages.into_iter().map(|(r, page)| (r, Dirty::Raw(page))));
            return sizes;
        }
        let packed: Vec<(Ref, Dirty)> = pages
            .into_par_iter()
            .map(|(r, logical)| {
                let kind = if is_temp_overflow(r) { EXTENT_VALUE } else { EXTENT_PAGE };
                let extent: Arc<[u8]> = Arc::from(encode_extent(kind, &logical));
                let links = kind == EXTENT_PAGE && refers_to_temp(&logical);
                if kind == EXTENT_PAGE {
                    self.cache.insert(r, Arc::from(logical));
                }
                (r, Dirty::Packed(extent, links))
            })
            .collect();
        let sizes = packed
            .iter()
            .map(|(r, dirty)| match dirty {
                Dirty::Packed(extent, _) | Dirty::Raw(extent) => (*r, extent.len()),
            })
            .collect();
        self.dirty.write().unwrap().extend(packed);
        sizes
    }

    /// Keeps pages a commit compressed as it made them. Returns the memory each takes.
    pub fn add_packed(&self, pages: Vec<(Ref, Vec<u8>, bool)>) -> Vec<(Ref, usize)> {
        if pages.is_empty() {
            return Vec::new();
        }
        let sizes = pages.iter().map(|(r, extent, _)| (*r, extent.len())).collect();
        self.dirty.write().unwrap().extend(pages.into_iter().map(|(r, extent, links)| (r, Dirty::Packed(Arc::from(extent), links))));
        sizes
    }

    /// Compresses the pages and values kept as they are. Tree pages go to the cache as they
    /// are, since fresh pages tend to be read again soon. Returns the memory each takes now.
    pub fn pack_dirty(&self) -> Vec<(Ref, usize)> {
        use rayon::prelude::*;
        let raw: Vec<(Ref, Arc<[u8]>)> = self
            .dirty
            .read()
            .unwrap()
            .iter()
            .filter_map(|(&r, d)| match d {
                Dirty::Raw(page) => Some((r, page.clone())),
                Dirty::Packed(..) => None,
            })
            .collect();
        // compressing is the bulk of the work for large commits: spread it over the cores
        let packed: Vec<(Ref, Arc<[u8]>, bool, Arc<[u8]>)> = raw
            .into_par_iter()
            .map(|(r, page)| {
                let kind = if is_temp_overflow(r) { EXTENT_VALUE } else { EXTENT_PAGE };
                (r, Arc::from(encode_extent(kind, &page)), kind == EXTENT_PAGE && refers_to_temp(&page), page)
            })
            .collect();
        let mut sizes = Vec::with_capacity(packed.len());
        let mut dirty = self.dirty.write().unwrap();
        for (r, extent, links, page) in packed {
            let Some(entry) = dirty.get_mut(&r) else { continue };
            if !is_temp_overflow(r) {
                self.cache.insert(r, page.clone());
            }
            self.raw_bytes.fetch_sub(page.len(), Ordering::AcqRel);
            sizes.push((r, extent.len()));
            *entry = Dirty::Packed(extent, links);
        }
        sizes
    }

    pub fn remove_dirty(&self, refs: impl IntoIterator<Item = Ref>) {
        let mut dirty = self.dirty.write().unwrap();
        for r in refs {
            if let Some(Dirty::Raw(page)) = dirty.remove(&r) {
                self.raw_bytes.fetch_sub(page.len(), Ordering::AcqRel);
            }
            self.cache.remove(&r);
        }
    }

    /// The physical ref a checkpoint wrote for temporary ref `r`, if any.
    pub fn translated(&self, r: Ref) -> Option<Ref> {
        self.translation.read().unwrap().get(&r).copied()
    }

    pub fn add_translations(&self, written: impl IntoIterator<Item = (Ref, Ref)>) {
        self.translation.write().unwrap().extend(written);
    }

    pub fn remove_translations(&self, refs: impl IntoIterator<Item = Ref>) {
        let mut translation = self.translation.write().unwrap();
        for r in refs {
            translation.remove(&r);
            self.cache.remove(&r);
        }
    }

    pub fn has_translations(&self) -> bool {
        !self.translation.read().unwrap().is_empty()
    }

    /// `r` with a written temporary ref replaced by its physical ref.
    pub fn normalize(&self, r: Ref) -> Ref {
        if is_temp(r) {
            self.translated(r).unwrap_or(r)
        } else {
            r
        }
    }

    /// Drops a cached copy: a checkpoint wrote new content at this block.
    pub fn invalidate(&self, block: u64) {
        self.cache.remove(&block);
    }

    fn read_extent(&self, r: Ref, kind: u8) -> io::Result<Vec<u8>> {
        let (block, blocks) = extent_of(r);
        if block < 2 || blocks == 0 || block + blocks > self.end.load(Ordering::Acquire) {
            return Err(corrupted("extent out of bounds"));
        }
        let mut bytes = vec![0u8; (blocks as usize) * BLOCK];
        self.file.read_exact_at(&mut bytes, block * BLOCK as u64).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                corrupted("extent beyond the end of the file")
            } else {
                e
            }
        })?;
        decode_extent(kind, &bytes)
    }
}

/// The pages of one committed state.
#[derive(Clone, Copy)]
pub struct View<'a> {
    pub pages: &'a PageFile,
}

impl View<'_> {
    /// A tree page.
    pub fn page(&self, r: Ref) -> io::Result<Arc<[u8]>> {
        if !is_temp(r) {
            return self.written_page(r);
        }
        // the cache first: it is sharded, while the dirty and translation maps have one lock
        if let Some(page) = self.pages.cache.get(&r) {
            return Ok(page);
        }
        let page = match self.pages.dirty_get(r) {
            Some(Dirty::Raw(page)) => page,
            Some(Dirty::Packed(extent, _)) => Arc::from(decode_extent(EXTENT_PAGE, &extent)?),
            None => self.written_page(self.pages.translated(r).ok_or_else(|| corrupted("unknown temporary page"))?)?,
        };
        self.pages.cache.insert(r, page.clone());
        Ok(page)
    }

    /// A tree page, read without keeping it in the cache (pages about to be let go).
    pub fn page_uncached(&self, r: Ref) -> io::Result<Arc<[u8]>> {
        let r = if is_temp(r) {
            if let Some(page) = self.pages.cache.get(&r) {
                return Ok(page);
            }
            match self.pages.dirty_get(r) {
                Some(Dirty::Raw(page)) => return Ok(page),
                Some(Dirty::Packed(extent, _)) => return Ok(Arc::from(decode_extent(EXTENT_PAGE, &extent)?)),
                None => self.pages.translated(r).ok_or_else(|| corrupted("unknown temporary page"))?,
            }
        } else {
            r
        };
        match self.pages.cache.get(&extent_of(r).0) {
            Some(page) => Ok(page),
            None => Ok(Arc::from(self.pages.read_extent(r, EXTENT_PAGE)?)),
        }
    }

    /// A tree page a checkpoint wrote at physical ref `r`.
    fn written_page(&self, r: Ref) -> io::Result<Arc<[u8]>> {
        let block = extent_of(r).0;
        if let Some(page) = self.pages.cache.get(&block) {
            return Ok(page);
        }
        let page: Arc<[u8]> = Arc::from(self.pages.read_extent(r, EXTENT_PAGE)?);
        self.pages.cache.insert(block, page.clone());
        Ok(page)
    }

    /// The `len` bytes of an overflow value.
    pub fn overflow(&self, r: Ref, len: usize) -> io::Result<Vec<u8>> {
        let bytes = if is_temp(r) {
            if !is_temp_overflow(r) {
                return Err(corrupted("overflow ref"));
            }
            match self.pages.dirty_get(r) {
                Some(Dirty::Raw(value)) => value.to_vec(),
                Some(Dirty::Packed(extent, _)) => decode_extent(EXTENT_VALUE, &extent)?,
                None => {
                    let r = self.pages.translated(r).ok_or_else(|| corrupted("unknown temporary value"))?;
                    self.pages.read_extent(r, EXTENT_VALUE)?
                }
            }
        } else {
            self.pages.read_extent(r, EXTENT_VALUE)?
        };
        if bytes.len() != len {
            return Err(corrupted("overflow value length"));
        }
        Ok(bytes)
    }
}
