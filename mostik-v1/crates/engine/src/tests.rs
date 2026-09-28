use std::collections::BTreeMap;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::*;

fn open_in(dir: &tempfile::TempDir) -> Arc<Env> {
    Env::open(&dir.path().join("data.mostik"), &dir.path().join("lock.mostik")).unwrap()
}

fn put(k: &[u8], v: &[u8]) -> Op {
    Op::Put(k.to_vec(), v.to_vec())
}

fn remove(k: &[u8]) -> Op {
    Op::Remove(k.to_vec())
}

/// Walks the whole file: keys are sorted and inside their parent's range, all leaves are at the
/// same depth, and every page is used by exactly one owner (tree, overflow value, freelist) or is free.
fn check_invariants(env: &Env) -> usize {
    use crate::page::*;
    let w = env.writer.lock().unwrap();
    let map: &[u8] = &w.map;
    let mut owner = vec![0u8; w.free.page_count as usize];
    owner[0] = 1;
    owner[1] = 1;
    let mut claim = |pgno: u64, n: usize| {
        for p in pgno..pgno + n as u64 {
            assert!(p < w.free.page_count, "page {p} beyond page_count");
            assert_eq!(owner[p as usize], 0, "page {p} used twice");
            owner[p as usize] = 1;
        }
    };
    let mut leaf_depth = None;
    let mut entries = 0;
    let mut stack = vec![(w.meta.root, Vec::<u8>::new(), None::<Vec<u8>>, 0usize)];
    while let Some((pgno, lower, upper, depth)) = stack.pop() {
        if pgno == 0 {
            continue;
        }
        claim(pgno, 1);
        let page = btree::page_at(map, pgno).unwrap();
        let in_range = |k: &[u8]| k >= lower.as_slice() && upper.as_deref().map_or(true, |u| k < u);
        match page.kind() {
            KIND_LEAF => {
                assert!(page.count() > 0, "empty leaf");
                assert_eq!(*leaf_depth.get_or_insert(depth), depth, "unbalanced tree");
                for i in 0..page.count() {
                    let k = page.leaf_key(i).unwrap();
                    assert!(in_range(k), "leaf key out of range");
                    if i > 0 {
                        assert!(page.leaf_key(i - 1).unwrap() < k, "leaf keys not sorted");
                    }
                    if let ValueRef::Overflow { pgno, len } = page.leaf_value(i).unwrap() {
                        claim(pgno, overflow_run_len(len));
                    }
                    entries += 1;
                }
            }
            KIND_BRANCH => {
                assert!(page.count() > 0, "empty branch");
                for i in 0..page.count() {
                    let lo = if i == 0 { lower.clone() } else { page.branch_key(i).unwrap().to_vec() };
                    let hi = if i + 1 < page.count() { Some(page.branch_key(i + 1).unwrap().to_vec()) } else { upper.clone() };
                    assert!(i == 0 || in_range(&lo), "separator out of range");
                    stack.push((page.branch_child(i).unwrap(), lo, hi, depth + 1));
                }
            }
            k => panic!("unexpected page kind {k}"),
        }
    }
    for &p in &w.freelist_pages {
        claim(p, 1);
    }
    for p in w.free.all() {
        claim(p, 1);
    }
    let lost: Vec<usize> = owner.iter().enumerate().filter(|(_, &o)| o == 0).map(|(p, _)| p).collect();
    assert!(lost.is_empty(), "leaked pages: {lost:?}");
    entries
}

fn check_all(env: &Env, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    assert_eq!(check_invariants(env), model.len(), "entry count");
    for (k, v) in model {
        assert_eq!(env.get(k).unwrap().as_deref(), Some(v.as_slice()), "key {k:?}");
    }
}

#[test]
fn put_get_remove() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    assert_eq!(env.get(b"a").unwrap(), None);
    env.commit(vec![put(b"a", b"1"), put(b"b", b"2")]).unwrap();
    assert_eq!(env.get(b"a").unwrap(), Some(b"1".to_vec()));
    env.commit(vec![remove(b"a"), remove(b"missing")]).unwrap();
    assert_eq!(env.get(b"a").unwrap(), None);
    assert_eq!(env.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn last_op_in_a_batch_wins() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit(vec![put(b"k", b"1"), remove(b"k"), put(b"k", b"2")]).unwrap();
    assert_eq!(env.get(b"k").unwrap(), Some(b"2".to_vec()));
    env.commit(vec![put(b"k", b"3"), remove(b"k")]).unwrap();
    assert_eq!(env.get(b"k").unwrap(), None);
}

#[test]
fn rejects_bad_keys() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    assert!(env.commit(vec![put(b"", b"x")]).is_err());
    assert!(env.commit(vec![put(&[1; MAX_KEY_SIZE + 1], b"x")]).is_err());
    env.commit(vec![put(&[1; MAX_KEY_SIZE], b"x")]).unwrap();
    assert_eq!(env.get(&[1; MAX_KEY_SIZE]).unwrap(), Some(b"x".to_vec()));
}

#[test]
fn large_values_and_max_keys() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut model = BTreeMap::new();
    let mut ops = Vec::new();
    for i in 0..200u32 {
        let mut key = vec![b'k'; MAX_KEY_SIZE];
        key[..4].copy_from_slice(&i.to_be_bytes());
        let value = vec![(i % 251) as u8; (i as usize * 997) % 50_000];
        ops.push(put(&key, &value));
        model.insert(key, value);
    }
    env.commit(ops).unwrap();
    check_all(&env, &model);
    // replace every big value with a small one: the overflow pages must be reused, not leaked
    let len_before = std::fs::metadata(dir.path().join("data.mostik")).unwrap().len();
    for _ in 0..5 {
        let ops = model.keys().map(|k| put(k, &vec![7; 20_000])).collect();
        env.commit(ops).unwrap();
    }
    let len_after = std::fs::metadata(dir.path().join("data.mostik")).unwrap().len();
    assert!(len_after < len_before * 3, "file grew from {len_before} to {len_after}");
}

#[test]
fn random_ops_match_btreemap() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut model = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(7);
    for round in 0..300 {
        let mut ops = Vec::new();
        for _ in 0..rng.gen_range(1..400) {
            let key = format!("key{:06}", rng.gen_range(0..20_000)).into_bytes();
            if rng.gen_bool(0.3) {
                model.remove(&key);
                ops.push(remove(&key));
            } else {
                let len = if rng.gen_bool(0.02) { rng.gen_range(2000..10_000) } else { rng.gen_range(0..100) };
                let value: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
                model.insert(key.clone(), value.clone());
                ops.push(put(&key, &value));
            }
        }
        env.commit(ops).unwrap();
        if round % 50 == 0 {
            check_all(&env, &model);
        }
    }
    check_all(&env, &model);
    for i in 0..20_000 {
        let key = format!("key{i:06}").into_bytes();
        assert_eq!(env.get(&key).unwrap(), model.get(&key).cloned());
    }
}

#[test]
fn hundred_thousand_keys_then_remove_all() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let keys: Vec<Vec<u8>> = (0..100_000u32).map(|i| format!("{i}").into_bytes()).collect();
    for chunk in keys.chunks(10_000) {
        env.commit(chunk.iter().map(|k| put(k, k)).collect()).unwrap();
    }
    for k in keys.iter().step_by(97) {
        assert_eq!(env.get(k).unwrap().as_ref(), Some(k));
    }
    for chunk in keys.chunks(7_000) {
        env.commit(chunk.iter().map(|k| remove(k)).collect()).unwrap();
    }
    assert_eq!(env.writer.lock().unwrap().meta.root, 0, "tree must be empty");
    for k in keys.iter().step_by(97) {
        assert_eq!(env.get(k).unwrap(), None);
    }
    // everything is free again, so refilling must not grow the file
    let len = std::fs::metadata(dir.path().join("data.mostik")).unwrap().len();
    for chunk in keys.chunks(10_000) {
        env.commit(chunk.iter().map(|k| put(k, k)).collect()).unwrap();
    }
    assert_eq!(std::fs::metadata(dir.path().join("data.mostik")).unwrap().len(), len);
}

#[test]
fn survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = BTreeMap::new();
    {
        let env = open_in(&dir);
        for i in 0..50u32 {
            let ops = (0..100u32)
                .map(|j| {
                    let (k, v) = (format!("{j}").into_bytes(), format!("{i}-{j}").into_bytes());
                    model.insert(k.clone(), v.clone());
                    put(&k, &v)
                })
                .collect();
            env.commit(ops).unwrap();
        }
    }
    let env = open_in(&dir);
    assert_eq!(env.txn_id(), 50);
    check_all(&env, &model);
}

#[test]
fn same_file_same_env_in_one_process() {
    let dir = tempfile::tempdir().unwrap();
    let a = open_in(&dir);
    let b = open_in(&dir);
    assert!(Arc::ptr_eq(&a, &b));
}

#[test]
fn torn_meta_falls_back_to_previous_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.mostik");
    {
        let env = open_in(&dir);
        env.commit(vec![put(b"k", b"old")]).unwrap(); // txn 1 -> slot 1
        env.commit(vec![put(b"k", b"new")]).unwrap(); // txn 2 -> slot 0
    }
    // simulate a crash in the middle of writing meta of txn 2
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.write_all_at(&[0xAB; 20], 20).unwrap();
    drop(file);
    let env = open_in(&dir);
    assert_eq!(env.txn_id(), 1);
    assert_eq!(env.get(b"k").unwrap(), Some(b"old".to_vec()));
    env.commit(vec![put(b"k", b"again")]).unwrap();
    assert_eq!(env.get(b"k").unwrap(), Some(b"again".to_vec()));
}

#[test]
fn crash_before_meta_keeps_previous_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.mostik");
    let before;
    {
        let env = open_in(&dir);
        env.commit((0..2000u32).map(|i| put(&i.to_be_bytes(), b"v1")).collect()).unwrap();
        before = std::fs::read(&path).unwrap();
        env.commit((0..2000u32).map(|i| put(&i.to_be_bytes(), b"v2")).collect()).unwrap();
    }
    // keep every page written by the second commit, but not its meta
    let mut after = std::fs::read(&path).unwrap();
    after[..2 * PAGE_SIZE].copy_from_slice(&before[..2 * PAGE_SIZE]);
    std::fs::write(&path, &after).unwrap();
    let env = open_in(&dir);
    for i in 0..2000u32 {
        assert_eq!(env.get(&i.to_be_bytes()).unwrap(), Some(b"v1".to_vec()));
    }
}

#[test]
fn reader_snapshot_is_not_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), b"first")).collect()).unwrap();
    let guard = env.readers.begin();
    for round in 0..20 {
        let value = format!("round{round}").into_bytes();
        env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), &value)).collect()).unwrap();
    }
    // the old snapshot still reads its own data: none of its pages were reused
    for i in 0..3000u32 {
        let v = btree::get(&guard.map, guard.root, &i.to_be_bytes()).unwrap();
        assert_eq!(v, Some(&b"first"[..]));
    }
    drop(guard);
    assert_eq!(env.get(&7u32.to_be_bytes()).unwrap(), Some(b"round19".to_vec()));
}

#[test]
fn concurrent_readers_during_writes() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit((0..1000u32).map(|i| put(&i.to_be_bytes(), &0u32.to_be_bytes())).collect()).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (env, stop) = (env.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // every commit writes one round number to all keys, so one snapshot never mixes rounds
                    let snap = env.readers.begin();
                    let first = btree::get(&snap.map, snap.root, &0u32.to_be_bytes()).unwrap().unwrap().to_vec();
                    for i in (0..1000u32).step_by(37) {
                        let v = btree::get(&snap.map, snap.root, &i.to_be_bytes()).unwrap().unwrap();
                        assert_eq!(v, first.as_slice());
                    }
                }
            })
        })
        .collect();
    for round in 1..200u32 {
        env.commit((0..1000u32).map(|i| put(&i.to_be_bytes(), &round.to_be_bytes())).collect()).unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
}

#[test]
fn reopen_after_churn_has_no_leaked_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(11);
    for session in 0..10 {
        let env = open_in(&dir);
        check_all(&env, &model);
        for _ in 0..30 {
            let mut ops = Vec::new();
            for _ in 0..rng.gen_range(1..800) {
                let key = format!("{}", rng.gen_range(0..5000)).into_bytes();
                if rng.gen_bool(0.4) {
                    model.remove(&key);
                    ops.push(remove(&key));
                } else {
                    let len = if rng.gen_bool(0.05) { rng.gen_range(3000..20_000) } else { rng.gen_range(0..300) };
                    let value = vec![session as u8; len];
                    model.insert(key.clone(), value.clone());
                    ops.push(put(&key, &value));
                }
            }
            env.commit(ops).unwrap();
        }
        check_all(&env, &model);
    }
}

#[test]
fn freelist_chain_accounts_for_every_page() {
    use crate::page::FREELIST_PER_PAGE;
    for n in [0, 1, FREELIST_PER_PAGE, FREELIST_PER_PAGE + 1, FREELIST_PER_PAGE + 2, 3 * FREELIST_PER_PAGE + 5] {
        let original: Vec<u64> = (10..10 + n as u64).collect();
        let mut free = FreeList::new(original.clone(), 10_000);
        let mut dirty = HashMap::new();
        let (head, chain) = write_freelist(&mut free, &mut dirty);
        let mut file = vec![0u8; 10_000 * PAGE_SIZE];
        for (pgno, bytes) in dirty {
            file[pgno as usize * PAGE_SIZE..][..PAGE_SIZE].copy_from_slice(&bytes);
        }
        let (listed, read_chain) = read_freelist(&file, head).unwrap();
        assert_eq!(read_chain, chain);
        let mut all: Vec<u64> = listed.into_iter().chain(chain).collect();
        all.sort_unstable();
        assert_eq!(all, original, "n = {n}");
    }
}

#[test]
fn empty_values_fill_pages_to_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut model = BTreeMap::new();
    let ops = (0..5000u32)
        .map(|i| {
            let key = vec![b'a' + (i % 26) as u8; 1 + (i % 3) as usize].into_iter().chain(i.to_be_bytes()).collect::<Vec<_>>();
            model.insert(key.clone(), Vec::new());
            put(&key, b"")
        })
        .collect();
    env.commit(ops).unwrap();
    check_all(&env, &model);
}

#[test]
fn corrupted_page_is_an_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.mostik");
    let root;
    {
        let env = open_in(&dir);
        env.commit((0..10u32).map(|i| put(&i.to_be_bytes(), b"v")).collect()).unwrap();
        root = env.writer.lock().unwrap().meta.root;
    }
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    // point every slot of the root leaf past the end of the page
    let mut garbage = vec![0u8; 2 * 10];
    for slot in garbage.chunks_mut(2) {
        slot.copy_from_slice(&4095u16.to_le_bytes());
    }
    file.write_all_at(&garbage, root * PAGE_SIZE as u64 + 16).unwrap();
    drop(file);
    let env = open_in(&dir);
    let err = env.get(&3u32.to_be_bytes()).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(env.commit(vec![put(b"x", b"y")]).is_err());
}
