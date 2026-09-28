//! Page allocation. Pages released by commit `T` are still reachable from snapshot `T - 1`,
//! so they wait in `pending` until no reader holds a snapshot older than `T`.
//! On disk the freelist is a flat list of page numbers: after a restart nobody reads old snapshots.

use std::collections::{BTreeSet, VecDeque};

#[derive(Clone, Debug, Default)]
pub struct FreeList {
    ready: BTreeSet<u64>,
    pending: VecDeque<(u64, Vec<u64>)>,
    /// High-water mark: pages `[0, page_count)` exist, new pages come from here.
    pub page_count: u64,
}

impl FreeList {
    pub fn new(free: Vec<u64>, page_count: u64) -> FreeList {
        FreeList { ready: free.into_iter().collect(), pending: VecDeque::new(), page_count }
    }

    /// Makes pages reusable once no reader can reach them. `oldest_reader` is the oldest snapshot in use.
    pub fn release(&mut self, oldest_reader: Option<u64>) {
        while let Some((tag, _)) = self.pending.front() {
            if oldest_reader.is_some_and(|oldest| oldest < *tag) {
                break;
            }
            let (_, pages) = self.pending.pop_front().unwrap();
            self.ready.extend(pages);
        }
    }

    /// Allocates `n` consecutive pages.
    pub fn alloc(&mut self, n: usize) -> u64 {
        if n == 1 {
            if let Some(p) = self.ready.pop_first() {
                return p;
            }
        } else if let Some(start) = self.find_run(n) {
            for p in start..start + n as u64 {
                self.ready.remove(&p);
            }
            return start;
        }
        let p = self.page_count;
        self.page_count += n as u64;
        p
    }

    fn find_run(&self, n: usize) -> Option<u64> {
        let (mut start, mut len) = (0, 0);
        for &p in &self.ready {
            if len > 0 && p == start + len as u64 {
                len += 1;
            } else {
                start = p;
                len = 1;
            }
            if len == n {
                return Some(start);
            }
        }
        None
    }

    /// Returns pages that were allocated and dropped within the same, unpublished commit.
    pub fn give_back(&mut self, pages: impl IntoIterator<Item = u64>) {
        self.ready.extend(pages);
    }

    /// Pages released by commit `txn`.
    pub fn defer(&mut self, txn: u64, pages: Vec<u64>) {
        if !pages.is_empty() {
            self.pending.push_back((txn, pages));
        }
    }

    /// Takes one page that is free right now, without growing the file.
    pub fn take_ready(&mut self) -> Option<u64> {
        self.ready.pop_first()
    }

    pub fn len(&self) -> usize {
        self.ready.len() + self.pending.iter().map(|(_, pages)| pages.len()).sum::<usize>()
    }

    /// Every free page, ready or pending; what gets persisted.
    pub fn all(&self) -> Vec<u64> {
        let mut all: Vec<u64> = self.ready.iter().copied().collect();
        for (_, pages) in &self.pending {
            all.extend_from_slice(pages);
        }
        all.sort_unstable();
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_pages_wait_for_readers() {
        let mut f = FreeList::new(vec![], 10);
        f.defer(5, vec![3, 4]);
        f.release(Some(4));
        assert_eq!(f.alloc(1), 10, "reader on snapshot 4 still reaches pages freed by txn 5");
        f.release(Some(5));
        assert_eq!(f.alloc(1), 3);
    }

    #[test]
    fn runs_are_contiguous() {
        let mut f = FreeList::new(vec![2, 3, 5, 6, 7], 10);
        assert_eq!(f.alloc(3), 5);
        assert_eq!(f.alloc(3), 10);
        assert_eq!(f.page_count, 13);
        assert_eq!(f.all(), vec![2, 3]);
    }
}
