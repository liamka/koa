//! Tracks which committed snapshots readers are using, so the writer never
//! reuses a page that an in-flight read may still reach.
//!
//! A reader holds an `Arc<Snapshot>`; the writer keeps every replaced snapshot in
//! `history` and treats one as in use while anybody but `history` references it.

use std::collections::VecDeque;
use std::sync::atomic::{fence, Ordering};
use std::sync::{Arc, Mutex};

use memmap2::Mmap;

pub struct Snapshot {
    pub txn: u64,
    pub root: u64,
    pub map: Arc<Mmap>,
}

pub struct Readers {
    current: Mutex<Arc<Snapshot>>,
    /// replaced snapshots, oldest first; only the writer touches it
    history: Mutex<VecDeque<Arc<Snapshot>>>,
}

impl Readers {
    pub fn new(current: Snapshot) -> Readers {
        Readers { current: Mutex::new(Arc::new(current)), history: Mutex::new(VecDeque::new()) }
    }

    /// The latest committed snapshot; readers keep it alive for as long as they hold it.
    pub fn begin(&self) -> Arc<Snapshot> {
        self.current.lock().unwrap().clone()
    }

    pub fn publish(&self, snapshot: Snapshot) {
        let old = std::mem::replace(&mut *self.current.lock().unwrap(), Arc::new(snapshot));
        self.history.lock().unwrap().push_back(old);
    }

    /// Oldest replaced snapshot still being read, if any. Readers of the current
    /// snapshot never matter: pages are only freed once they left the current tree.
    pub fn oldest(&self) -> Option<u64> {
        // taking the lock orders us after any `begin` that cloned a now-replaced snapshot
        drop(self.current.lock().unwrap());
        let mut history = self.history.lock().unwrap();
        while history.front().is_some_and(|s| Arc::strong_count(s) == 1) {
            history.pop_front();
        }
        // pairs with the release of readers dropping their Arc: their page reads are done
        fence(Ordering::Acquire);
        history.front().map(|s| s.txn)
    }
}
