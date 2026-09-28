//! Free space on disk, as extents of blocks. Only checkpoints allocate (commits keep their pages
//! in memory); commits only release the extents of pages they replaced.
//!
//! Extents released by commit `T` are still reachable from snapshot `T - 1`, so they wait in
//! `pending` until no reader holds a snapshot older than `T`. Extents that are part of the last
//! checkpoint wait in `awaiting` first: recovery after a crash starts from that checkpoint, so
//! they must stay intact until the next checkpoint is on disk.
//! On disk the freelist is a flat list of extents: after a restart nobody reads old snapshots.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub type Extent = (u64, u64);

/// Free extents ready for use, coalesced, with a size index for best-fit allocation.
#[derive(Clone, Debug, Default)]
pub struct Space {
    by_start: BTreeMap<u64, u64>,
    by_size: BTreeSet<(u64, u64)>,
    /// blocks `[0, end)` exist; allocation past the free extents grows the file from here
    pub end: u64,
}

impl Space {
    pub fn new(extents: impl IntoIterator<Item = Extent>, end: u64) -> Space {
        let mut space = Space { end, ..Space::default() };
        for extent in extents {
            space.free(extent);
        }
        space
    }

    /// Adds an extent, merging it with free neighbours.
    pub fn free(&mut self, (mut start, mut len): Extent) {
        if len == 0 {
            return;
        }
        if let Some((&before, &before_len)) = self.by_start.range(..start).next_back() {
            if before + before_len == start {
                self.remove(before, before_len);
                start = before;
                len += before_len;
            }
        }
        if let Some(&after_len) = self.by_start.get(&(start + len)) {
            self.remove(start + len, after_len);
            len += after_len;
        }
        self.by_start.insert(start, len);
        self.by_size.insert((len, start));
    }

    fn remove(&mut self, start: u64, len: u64) {
        self.by_start.remove(&start);
        self.by_size.remove(&(len, start));
    }

    /// `blocks` consecutive blocks: the smallest free extent that fits, else the end of the file.
    pub fn alloc(&mut self, blocks: u64) -> u64 {
        if let Some(&(len, start)) = self.by_size.range((blocks, 0)..).next() {
            self.remove(start, len);
            if len > blocks {
                self.by_start.insert(start + blocks, len - blocks);
                self.by_size.insert((len - blocks, start + blocks));
            }
            return start;
        }
        let start = self.end;
        self.end += blocks;
        start
    }

    pub fn extents(&self) -> impl Iterator<Item = Extent> + '_ {
        self.by_start.iter().map(|(&s, &l)| (s, l))
    }
}

#[derive(Clone, Debug, Default)]
pub struct FreeList {
    pub space: Space,
    pending: VecDeque<(u64, Vec<Extent>)>,
    awaiting: Vec<(u64, Vec<Extent>)>,
}

impl FreeList {
    pub fn new(free: Vec<Extent>, end: u64) -> FreeList {
        FreeList { space: Space::new(free, end), pending: VecDeque::new(), awaiting: Vec::new() }
    }

    /// Makes extents reusable once no reader can reach them. `oldest_reader` is the oldest
    /// snapshot in use.
    pub fn release(&mut self, oldest_reader: Option<u64>) {
        while let Some((tag, _)) = self.pending.front() {
            if oldest_reader.is_some_and(|oldest| oldest < *tag) {
                break;
            }
            let (_, extents) = self.pending.pop_front().unwrap();
            for extent in extents {
                self.space.free(extent);
            }
        }
    }

    /// Extents released by commit `txn` that are in no checkpoint: they only wait for readers.
    pub fn defer(&mut self, txn: u64, extents: Vec<Extent>) {
        if !extents.is_empty() {
            self.pending.push_back((txn, extents));
        }
    }

    /// Extents of a checkpoint released by commit `txn`: they wait for the next checkpoint.
    pub fn defer_until_checkpoint(&mut self, txn: u64, extents: Vec<Extent>) {
        if !extents.is_empty() {
            self.awaiting.push((txn, extents));
        }
    }

    /// A checkpoint of the tree as of commit `txn` is on disk: extents freed up to that commit
    /// now only wait for readers. Extents freed later may be in that checkpoint: they keep waiting.
    pub fn checkpoint_done(&mut self, txn: u64) {
        let (done, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.awaiting).into_iter().partition(|(tag, _)| *tag <= txn);
        self.awaiting = waiting;
        let mut pending: Vec<_> = self.pending.drain(..).chain(done).collect();
        pending.sort_by_key(|(tag, _)| *tag);
        self.pending = pending.into();
    }

    /// Hands the ready space to a checkpoint, which allocates from it without the writer lock.
    pub fn take_space(&mut self) -> Space {
        let end = self.space.end;
        std::mem::replace(&mut self.space, Space { end, ..Space::default() })
    }

    /// Takes back what a checkpoint left of its space; extents that became ready meanwhile stay.
    pub fn return_space(&mut self, space: Space) {
        let mine = std::mem::replace(&mut self.space, space);
        for extent in mine.extents() {
            self.space.free(extent);
        }
    }

    /// Every extent waiting to become free: with the ready space, what a checkpoint persists.
    pub fn waiting(&self) -> Vec<Extent> {
        self.pending.iter().chain(&self.awaiting).flat_map(|(_, extents)| extents.iter().copied()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extents_merge_and_allocate_best_fit() {
        let mut space = Space::new([(10, 2), (12, 3), (20, 1), (30, 8)], 100);
        assert_eq!(space.extents().collect::<Vec<_>>(), vec![(10, 5), (20, 1), (30, 8)]);
        assert_eq!(space.alloc(1), 20, "the smallest extent that fits");
        assert_eq!(space.alloc(5), 10);
        assert_eq!(space.alloc(3), 30);
        assert_eq!(space.alloc(6), 100, "nothing fits: grow");
        assert_eq!(space.end, 106);
        space.free((10, 5));
        space.free((15, 5));
        assert_eq!(space.extents().collect::<Vec<_>>(), vec![(10, 10), (33, 5)]);
    }

    #[test]
    fn pending_extents_wait_for_readers_and_checkpoints() {
        let mut f = FreeList::new(vec![], 10);
        f.defer(5, vec![(3, 1)]);
        f.defer_until_checkpoint(6, vec![(4, 2)]);
        f.release(Some(4));
        assert_eq!(f.space.alloc(1), 10, "a reader on snapshot 4 still reaches blocks freed by txn 5");
        f.release(None);
        assert_eq!(f.space.alloc(1), 3);
        f.checkpoint_done(5);
        f.release(None);
        assert_eq!(f.space.alloc(2), 11, "freed by txn 6, after the checkpoint of txn 5");
        f.checkpoint_done(6);
        f.release(None);
        assert_eq!(f.space.alloc(2), 4);
    }

    #[test]
    fn a_checkpoint_borrows_the_space_and_gives_back_the_rest() {
        let mut f = FreeList::new(vec![(2, 4)], 10);
        let mut space = f.take_space();
        f.defer(1, vec![(8, 2)]);
        f.release(None); // becomes ready while the checkpoint runs
        assert_eq!(space.alloc(3), 2);
        assert_eq!(space.alloc(4), 10);
        f.return_space(space);
        assert_eq!(f.space.extents().collect::<Vec<_>>(), vec![(5, 1), (8, 2)]);
        assert_eq!(f.space.end, 14);
    }
}
