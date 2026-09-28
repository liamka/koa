use std::collections::BTreeMap;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
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

/// Checkpoints, then walks the whole file: keys are sorted and inside their parent's range, all
/// leaves are at the same depth, and every block is used by exactly one owner (meta, tree page,
/// overflow value, freelist) or is free.
fn check_invariants(env: &Env) -> usize {
    use crate::page::*;
    env.checkpoint().unwrap();
    let w = env.writer.lock().unwrap();
    let view = env.latest_view();
    let end = w.meta.end;
    assert_eq!(w.free.space.end, end);
    let mut owner = vec![0u8; end as usize];
    owner[0] = 1;
    owner[1] = 1;
    let mut claim = |(block, blocks): (u64, u64)| {
        for b in block..block + blocks {
            assert!(b < end, "block {b} beyond the end {end}");
            assert_eq!(owner[b as usize], 0, "block {b} used twice");
            owner[b as usize] = 1;
        }
    };
    let mut leaf_depth = None;
    let mut entries = 0;
    let root = w.meta.root;
    let mut stack = vec![(root, Vec::<u8>::new(), None::<Vec<u8>>, 0usize)];
    while let Some((r, lower, upper, depth)) = stack.pop() {
        if r == 0 {
            continue;
        }
        assert!(!is_temp(r), "a checkpointed tree points at a temporary page");
        claim(extent_of(r));
        let bytes = view.page(r).unwrap();
        let page = Page(&bytes);
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
                    if let ValueRef::Overflow { r, .. } = page.leaf_value(i).unwrap() {
                        assert!(!is_temp(r), "a checkpointed leaf points at a temporary value");
                        claim(extent_of(r));
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
    for &b in &w.freelist_blocks {
        claim((b, FREELIST_BLOCKS));
    }
    for extent in w.free.space.extents().chain(w.free.waiting()) {
        claim(extent);
    }
    let lost: Vec<usize> = owner.iter().enumerate().filter(|(_, &o)| o == 0).map(|(b, _)| b).collect();
    assert!(lost.is_empty(), "leaked blocks: {lost:?}");
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
    // replace every big value again and again: freed overflow runs must be reused, not leaked
    env.checkpoint().unwrap();
    let pages_before = env.writer.lock().unwrap().free.space.end;
    for _ in 0..5 {
        let ops = model.keys().map(|k| put(k, &vec![7; 20_000])).collect();
        env.commit(ops).unwrap();
        env.checkpoint().unwrap();
    }
    let pages_after = env.writer.lock().unwrap().free.space.end;
    assert!(pages_after < pages_before * 3, "grew from {pages_before} to {pages_after} pages");
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
    assert_eq!(env.writer.lock().unwrap().root, 0, "tree must be empty");
    for k in keys.iter().step_by(97) {
        assert_eq!(env.get(k).unwrap(), None);
    }
    // everything is free again once checkpointed, so refilling must not need new pages
    env.checkpoint().unwrap();
    let pages = env.writer.lock().unwrap().free.space.end;
    for chunk in keys.chunks(10_000) {
        env.commit(chunk.iter().map(|k| put(k, k)).collect()).unwrap();
    }
    assert_eq!(env.writer.lock().unwrap().free.space.end, pages);
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
        env.commit(vec![put(b"k", b"old")]).unwrap();
        env.checkpoint().unwrap(); // txn 1 -> slot 1
        env.commit(vec![put(b"k", b"new")]).unwrap();
    } // closing checkpoints txn 2 -> slot 0
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

/// The files as a crash would leave them right now: whatever the process wrote so far.
fn crash_copy(dir: &tempfile::TempDir) -> tempfile::TempDir {
    let copy = tempfile::tempdir().unwrap();
    for name in ["data.mostik", "data.mostik-journal0", "data.mostik-journal1"] {
        std::fs::copy(dir.path().join(name), copy.path().join(name)).unwrap();
    }
    copy
}

#[test]
fn a_crash_keeps_every_commit_in_the_journal() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit((0..2000u32).map(|i| put(&i.to_be_bytes(), b"checkpointed")).collect()).unwrap();
    env.checkpoint().unwrap();
    for round in 0..5u32 {
        env.commit((0..2000u32).map(|i| put(&i.to_be_bytes(), &round.to_be_bytes())).collect()).unwrap();
    }
    env.commit(vec![remove(&7u32.to_be_bytes())]).unwrap();
    let copy = crash_copy(&dir);
    let recovered = open_in(&copy);
    assert_eq!(recovered.get(&7u32.to_be_bytes()).unwrap(), None);
    for i in (0..2000u32).filter(|&i| i != 7) {
        assert_eq!(recovered.get(&i.to_be_bytes()).unwrap(), Some(4u32.to_be_bytes().to_vec()));
    }
    check_invariants(&recovered);
}

#[test]
fn a_torn_journal_tail_loses_only_that_commit() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit(vec![put(b"a", b"1")]).unwrap();
    env.commit((0..500u32).map(|i| put(&i.to_be_bytes(), b"last")).collect()).unwrap();
    let copy = crash_copy(&dir);
    let journal = copy.path().join("data.mostik-journal0");
    let len = std::fs::metadata(&journal).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&journal).unwrap().set_len(len - 10).unwrap();
    let recovered = open_in(&copy);
    assert_eq!(recovered.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(recovered.get(&3u32.to_be_bytes()).unwrap(), None, "the torn commit is gone as a whole");
    check_invariants(&recovered);
}

#[test]
fn a_crash_during_a_checkpoint_recovers_from_the_previous_one() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), b"v1")).collect()).unwrap();
    env.checkpoint().unwrap();
    for round in 0..3u32 {
        env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), &round.to_be_bytes())).collect()).unwrap();
    }
    let before = crash_copy(&dir);
    env.checkpoint().unwrap();
    // pages of the new checkpoint reached the file, its meta did not: old meta + full journal
    let copy = crash_copy(&dir);
    let old = std::fs::read(before.path().join("data.mostik")).unwrap();
    let mut data = std::fs::read(copy.path().join("data.mostik")).unwrap();
    data[..2 * BLOCK].copy_from_slice(&old[..2 * BLOCK]);
    std::fs::write(copy.path().join("data.mostik"), &data).unwrap();
    for name in ["data.mostik-journal0", "data.mostik-journal1"] {
        std::fs::copy(before.path().join(name), copy.path().join(name)).unwrap();
    }
    let recovered = open_in(&copy);
    for i in 0..3000u32 {
        assert_eq!(recovered.get(&i.to_be_bytes()).unwrap(), Some(2u32.to_be_bytes().to_vec()));
    }
    check_invariants(&recovered);
}

#[test]
fn old_checkpoint_pages_are_not_reused_before_the_next_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), b"v1")).collect()).unwrap();
    env.checkpoint().unwrap();
    let checkpointed = std::fs::read(dir.path().join("data.mostik")).unwrap();
    // many commits, no checkpoint: every page of the checkpoint must stay as it is on disk
    for round in 0..20u32 {
        env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), &round.to_be_bytes())).collect()).unwrap();
    }
    let now = std::fs::read(dir.path().join("data.mostik")).unwrap();
    assert_eq!(&now[..checkpointed.len()], &checkpointed[..]);
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
        let v = guard.get_with(&i.to_be_bytes(), <[u8]>::to_vec).unwrap();
        assert_eq!(v.as_deref(), Some(&b"first"[..]));
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
                    let first = snap.get_with(&0u32.to_be_bytes(), <[u8]>::to_vec).unwrap().unwrap();
                    for i in (0..1000u32).step_by(37) {
                        let v = snap.get_with(&i.to_be_bytes(), <[u8]>::to_vec).unwrap().unwrap();
                        assert_eq!(v, first);
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
fn freelist_chain_accounts_for_every_extent() {
    use crate::page::FREELIST_PER_NODE;
    for n in [0, 1, FREELIST_PER_NODE, FREELIST_PER_NODE + 1, FREELIST_PER_NODE + 2, 3 * FREELIST_PER_NODE + 5] {
        // n separate extents, so none merge
        let original: Vec<(u64, u64)> = (0..n as u64).map(|i| (10 + 3 * i, 2)).collect();
        let (listed_elsewhere, in_space) = original.split_at(n / 3);
        let mut space = Space::new(in_space.iter().copied(), 10_000);
        let (head, chain, blocks) = build_freelist(&mut space, listed_elsewhere);
        let file = tempfile::tempfile().unwrap();
        file.set_len(space.end * BLOCK as u64).unwrap();
        for (block, bytes) in blocks {
            file.write_all_at(&bytes, block * BLOCK as u64).unwrap();
        }
        let (listed, read_chain) = read_freelist(&file, head, space.end).unwrap();
        assert_eq!(read_chain, chain);
        // what is listed plus the chain blocks is exactly the original free space
        let mut blocks: Vec<u64> = listed.iter().flat_map(|&(s, l)| s..s + l).chain(chain.iter().flat_map(|&b| b..b + FREELIST_BLOCKS)).collect();
        blocks.sort_unstable();
        let mut expected: Vec<u64> = original.iter().flat_map(|&(s, l)| s..s + l).collect();
        expected.extend(chain.iter().filter(|&&b| b >= 10_000).flat_map(|&b| b..b + FREELIST_BLOCKS));
        expected.sort_unstable();
        assert_eq!(blocks, expected, "n = {n}");
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
        env.checkpoint().unwrap();
        root = env.writer.lock().unwrap().meta.root;
    }
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    // damage the stored bytes of the root leaf
    file.write_all_at(&[0xAB; 20], crate::page::extent_of(root).0 * BLOCK as u64 + 40).unwrap();
    drop(file);
    let env = open_in(&dir);
    let err = env.get(&3u32.to_be_bytes()).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(env.commit(vec![put(b"x", b"y")]).is_err());
}

fn collect_range(env: &Env, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    env.snapshot()
        .range(start, end, |k, v| {
            out.push((k.to_vec(), v.to_vec()));
            true
        })
        .unwrap();
    out
}

#[test]
fn range_matches_btreemap() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut model = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(5);
    for _ in 0..20 {
        let ops = (0..2000)
            .map(|_| {
                let key = format!("{:05}", rng.gen_range(0..50_000)).into_bytes();
                let len = if rng.gen_bool(0.01) { 9000 } else { rng.gen_range(0..40) };
                let value = vec![rng.gen(); len];
                model.insert(key.clone(), value.clone());
                put(&key, &value)
            })
            .collect();
        env.commit(ops).unwrap();
    }
    for _ in 0..300 {
        let a = format!("{:05}", rng.gen_range(0..50_000)).into_bytes();
        let b = format!("{:05}", rng.gen_range(0..50_000)).into_bytes();
        let expected: Vec<_> = if a < b { model.range(a.clone()..b.clone()).map(|(k, v)| (k.clone(), v.clone())).collect() } else { vec![] };
        assert_eq!(collect_range(&env, &a, &b), expected);
    }
    assert_eq!(collect_range(&env, b"", &[0xff]).len(), model.len());
    // stopping early
    let mut seen = 0;
    env.snapshot().range(b"", &[0xff], |_, _| { seen += 1; seen < 10 }).unwrap();
    assert_eq!(seen, 10);
}

#[test]
fn range_on_empty_tree_and_empty_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    assert!(collect_range(&env, b"", &[0xff]).is_empty());
    env.commit(vec![put(b"b", b"1")]).unwrap();
    assert!(collect_range(&env, b"b", b"b").is_empty());
    assert!(collect_range(&env, b"c", b"a").is_empty());
    assert_eq!(collect_range(&env, b"b", b"b\0").len(), 1);
}

#[test]
fn snapshot_range_is_stable_while_writing() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), b"old")).collect()).unwrap();
    let snap = env.snapshot();
    for _ in 0..10 {
        env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), b"new")).collect()).unwrap();
    }
    let mut n = 0;
    snap.range(b"", &[0xff; 5], |_, v| { assert_eq!(v, b"old"); n += 1; true }).unwrap();
    assert_eq!(n, 3000);
}

fn group(absent: &[&[u8]], ops: Vec<Op>) -> Group {
    Group { conditions: absent.iter().map(|k| Condition::Absent(k.to_vec())).collect(), ops, chain: 0 }
}

fn identity(v: &[u8]) -> io::Result<Vec<u8>> {
    Ok(v.to_vec())
}

#[test]
fn groups_apply_only_when_keys_are_absent() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit(vec![put(b"taken", b"1")]).unwrap();
    let applied = env
        .commit_groups(
            vec![
            group(&[b"taken"], vec![put(b"taken", b"2"), put(b"idx-a", b"")]),
            group(&[b"new"], vec![put(b"new", b"1"), put(b"idx-b", b"")]),
            // sees the group above: "new" now exists
            group(&[b"new"], vec![put(b"new", b"2"), put(b"idx-c", b"")]),
            group(&[], vec![remove(b"taken")]),
            // sees the removal above
            group(&[b"taken"], vec![put(b"taken", b"3")]),
            ],
            &identity,
        )
        .unwrap();
    assert_eq!(applied, vec![false, true, false, true, true]);
    assert_eq!(env.get(b"taken").unwrap(), Some(b"3".to_vec()));
    assert_eq!(env.get(b"new").unwrap(), Some(b"1".to_vec()));
    assert_eq!(env.get(b"idx-a").unwrap(), None);
    assert_eq!(env.get(b"idx-b").unwrap(), Some(vec![]));
    assert_eq!(env.get(b"idx-c").unwrap(), None);
    // nothing applied: no commit, nothing changes
    let txn = env.txn_id();
    assert_eq!(env.commit_groups(vec![group(&[b"new"], vec![put(b"new", b"x")])], &identity).unwrap(), vec![false]);
    assert_eq!(env.txn_id(), txn);
}

fn open_tiny_cache(dir: &tempfile::TempDir) -> Arc<Env> {
    // 16 pages: almost every read misses, pages are evicted and reused constantly
    Env::open_with_cache(&dir.path().join("data.mostik"), &dir.path().join("lock.mostik"), 16 * PAGE_SIZE).unwrap()
}

#[test]
fn tiny_cache_random_ops_match_btreemap() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_tiny_cache(&dir);
    let mut model = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(21);
    for _ in 0..100 {
        let mut ops = Vec::new();
        for _ in 0..rng.gen_range(1..400) {
            let key = format!("k{:05}", rng.gen_range(0..10_000)).into_bytes();
            if rng.gen_bool(0.3) {
                model.remove(&key);
                ops.push(remove(&key));
            } else {
                let len = if rng.gen_bool(0.02) { rng.gen_range(3000..12_000) } else { rng.gen_range(0..80) };
                let value: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
                model.insert(key.clone(), value.clone());
                ops.push(put(&key, &value));
            }
        }
        env.commit(ops).unwrap();
    }
    check_all(&env, &model);
    assert_eq!(collect_range(&env, b"", &[0xff]).len(), model.len());
}

#[test]
fn tiny_cache_old_snapshot_survives_page_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_tiny_cache(&dir);
    env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), b"first")).collect()).unwrap();
    let old = env.snapshot();
    for round in 0..20 {
        let value = format!("round{round}").into_bytes();
        env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), &value)).collect()).unwrap();
    }
    for i in 0..3000u32 {
        assert_eq!(old.get_with(&i.to_be_bytes(), <[u8]>::to_vec).unwrap().as_deref(), Some(&b"first"[..]));
    }
    drop(old);
    // pages of the old snapshot are reused now; the cache must not serve their old contents
    for round in 20..30 {
        let value = format!("round{round}").into_bytes();
        env.commit((0..3000u32).map(|i| put(&i.to_be_bytes(), &value)).collect()).unwrap();
    }
    for i in 0..3000u32 {
        assert_eq!(env.get(&i.to_be_bytes()).unwrap().as_deref(), Some(&b"round29"[..]));
    }
}

/// Average share of leaf page capacity in use.
fn leaf_fill(env: &Env) -> f64 {
    use crate::page::*;
    let w = env.writer.lock().unwrap();
    let view = env.latest_view();
    let (mut used, mut leaves) = (0usize, 0usize);
    let mut stack = vec![w.root];
    while let Some(pgno) = stack.pop() {
        let bytes = view.page(pgno).unwrap();
        let page = Page(&bytes);
        if page.kind() == KIND_BRANCH {
            stack.extend((0..page.count()).map(|i| page.branch_child(i).unwrap()));
        } else {
            leaves += 1;
            used += (0..page.count())
                .map(|i| {
                    let value = match page.leaf_value(i).unwrap() {
                        ValueRef::Inline(v) => v.len(),
                        ValueRef::Overflow { .. } => 8,
                    };
                    2 + 7 + page.leaf_key(i).unwrap().len() + value
                })
                .sum::<usize>();
        }
    }
    used as f64 / (leaves * CAPACITY) as f64
}

#[test]
fn ascending_inserts_fill_pages() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut model = BTreeMap::new();
    for batch in 0..100u32 {
        let ops = (0..1000u32)
            .map(|i| {
                let key = (batch * 1000 + i).to_be_bytes().to_vec();
                model.insert(key.clone(), vec![7u8; 40]);
                put(&key, &[7; 40])
            })
            .collect();
        env.commit(ops).unwrap();
    }
    check_all(&env, &model);
    let fill = leaf_fill(&env);
    assert!(fill > 0.9, "ascending inserts left leaves {:.0}% full", fill * 100.0);

    // random order still splits evenly, leaving room on both sides
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut keys: Vec<u32> = (0..100_000).collect();
    keys.shuffle(&mut StdRng::seed_from_u64(1));
    for chunk in keys.chunks(1000) {
        env.commit(chunk.iter().map(|k| put(&k.to_be_bytes(), &[7; 40])).collect()).unwrap();
    }
    check_all(&env, &model);
    let fill = leaf_fill(&env);
    assert!((0.5..0.9).contains(&fill), "random inserts: leaves {:.0}% full", fill * 100.0);
}

#[test]
fn present_and_equals_conditions() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit(vec![put(b"doc", b"v1")]).unwrap();
    let cond = |c: Vec<Condition>, ops: Vec<Op>| Group { conditions: c, ops, chain: 0 };
    let applied = env
        .commit_groups(
            vec![
                cond(vec![Condition::Equals(b"doc".to_vec(), b"v0".to_vec())], vec![remove(b"doc")]),
                cond(vec![Condition::Present(b"missing".to_vec())], vec![put(b"x", b"1")]),
                // deleted and re-inserted with other content: a stale delete must not apply
                cond(vec![Condition::Equals(b"doc".to_vec(), b"v1".to_vec())], vec![remove(b"doc")]),
                cond(vec![Condition::Absent(b"doc".to_vec())], vec![put(b"doc", b"v2")]),
                cond(vec![Condition::Equals(b"doc".to_vec(), b"v1".to_vec())], vec![remove(b"doc")]),
                cond(vec![Condition::Equals(b"doc".to_vec(), b"v2".to_vec()), Condition::Present(b"doc".to_vec())], vec![put(b"seen", b"")]),
            ],
            &identity,
        )
        .unwrap();
    assert_eq!(applied, vec![false, false, true, true, false, true]);
    assert_eq!(env.get(b"doc").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(env.get(b"x").unwrap(), None);
    // Equals decodes stored values before comparing
    let upper = |v: &[u8]| Ok(v.to_ascii_uppercase());
    let applied = env
        .commit_groups(vec![cond(vec![Condition::Equals(b"doc".to_vec(), b"V2".to_vec())], vec![remove(b"doc")])], &upper)
        .unwrap();
    assert_eq!(applied, vec![true]);
    assert_eq!(env.get(b"doc").unwrap(), None);
}

#[test]
fn chains_stop_at_the_first_rejected_group() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit(vec![put(b"b", b"taken")]).unwrap();
    let insert = |k: &[u8], chain| Group { conditions: vec![Condition::Absent(k.to_vec())], ops: vec![put(k, b"new")], chain };
    let applied = env
        .commit_groups(vec![insert(b"a", 1), insert(b"b", 1), insert(b"c", 1), insert(b"d", 2), insert(b"b", 3), insert(b"e", 3), insert(b"f", 0)], &identity)
        .unwrap();
    assert_eq!(applied, vec![true, false, false, true, false, false, true]);
    assert_eq!(env.get(b"c").unwrap(), None);
    assert_eq!(env.get(b"b").unwrap(), Some(b"taken".to_vec()));
}

fn counter(env: &Env, key: &[u8]) -> Option<i64> {
    env.get(key).unwrap().map(|v| {
        assert_eq!(v.len(), 9);
        i64::from_le_bytes(v[1..].try_into().unwrap())
    })
}

#[test]
fn counters_add_up_across_groups_and_commits() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let add = |d| Op::Add(b"n".to_vec(), d);
    assert_eq!(counter(&env, b"n"), None);
    let applied = env
        .commit_groups(
            vec![
                Group { conditions: vec![], ops: vec![add(5), add(1)], chain: 0 },
                Group { conditions: vec![Condition::Present(b"nope".to_vec())], ops: vec![add(1000)], chain: 0 },
                Group { conditions: vec![Condition::Present(b"n".to_vec())], ops: vec![add(-2)], chain: 0 },
            ],
            &identity,
        )
        .unwrap();
    assert_eq!(applied, vec![true, false, true]);
    assert_eq!(counter(&env, b"n"), Some(4));
    for _ in 0..50 {
        env.commit(vec![add(3), put(b"other", b"x"), add(-1)]).unwrap();
    }
    assert_eq!(counter(&env, b"n"), Some(104));
    drop(env);
    let env = open_in(&dir);
    assert_eq!(counter(&env, b"n"), Some(104));
}

#[test]
fn readers_stay_consistent_across_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let env = Env::open_with_cache(&dir.path().join("data.mostik"), &dir.path().join("lock.mostik"), 32 * PAGE_SIZE).unwrap();
    env.commit((0..2000u32).map(|i| put(&i.to_be_bytes(), &0u32.to_be_bytes())).collect()).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (env, stop) = (env.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let snap = env.snapshot();
                    let first = snap.get_with(&0u32.to_be_bytes(), <[u8]>::to_vec).unwrap().unwrap();
                    for i in (0..2000u32).step_by(13) {
                        assert_eq!(snap.get_with(&i.to_be_bytes(), <[u8]>::to_vec).unwrap().unwrap(), first);
                    }
                }
            })
        })
        .collect();
    for round in 1..300u32 {
        env.commit((0..2000u32).map(|i| put(&i.to_be_bytes(), &round.to_be_bytes())).collect()).unwrap();
        if round % 7 == 0 {
            env.checkpoint().unwrap();
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
    check_invariants(&env);
}

#[test]
fn crash_copies_at_random_moments_recover_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut model = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(99);
    for step in 0..120 {
        let mut ops = Vec::new();
        for _ in 0..rng.gen_range(1..300) {
            let key = format!("k{:04}", rng.gen_range(0..3000)).into_bytes();
            if rng.gen_bool(0.3) {
                model.remove(&key);
                ops.push(remove(&key));
            } else {
                let len = if rng.gen_bool(0.03) { rng.gen_range(3000..9000) } else { rng.gen_range(0..60) };
                let value = vec![step as u8; len];
                model.insert(key.clone(), value.clone());
                ops.push(put(&key, &value));
            }
        }
        env.commit(ops).unwrap();
        if rng.gen_bool(0.1) {
            env.checkpoint().unwrap();
        }
        if step % 15 == 14 {
            let copy = crash_copy(&dir);
            let recovered = open_in(&copy);
            check_all(&recovered, &model);
        }
    }
}

#[test]
fn commits_run_during_checkpoints_and_everything_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let checkpointer = {
        let (env, stop) = (env.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut n = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                env.checkpoint().unwrap();
                n += 1;
            }
            n
        })
    };
    let reader = {
        let (env, stop) = (env.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let snap = env.snapshot();
                let first = snap.get_with(b"round", <[u8]>::to_vec).unwrap();
                if let Some(round) = first {
                    for i in (0..3000u32).step_by(101) {
                        assert_eq!(snap.get_with(&i.to_be_bytes(), <[u8]>::to_vec).unwrap().unwrap(), round);
                    }
                }
            }
        })
    };
    let mut model = BTreeMap::new();
    for round in 0..400u32 {
        let value = round.to_be_bytes().to_vec();
        let mut ops: Vec<Op> = (0..3000u32).map(|i| put(&i.to_be_bytes(), &value)).collect();
        ops.push(put(b"round", &value));
        if round % 3 == 0 {
            ops.push(put(&[b'x'; 10], &vec![round as u8; 9000])); // overflow runs too
        }
        env.commit(ops).unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let checkpoints = checkpointer.join().unwrap();
    reader.join().unwrap();
    assert!(checkpoints > 10, "only {checkpoints} checkpoints ran");
    for i in 0..3000u32 {
        model.insert(i.to_be_bytes().to_vec(), 399u32.to_be_bytes().to_vec());
    }
    model.insert(b"round".to_vec(), 399u32.to_be_bytes().to_vec());
    model.insert(vec![b'x'; 10], vec![399u32 as u8; 9000]);
    check_all(&env, &model);
    drop(env);
    let env = open_in(&dir);
    check_all(&env, &model);
}

#[test]
fn commits_inside_the_writing_phase_of_checkpoints() {
    use std::sync::mpsc;
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut model = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(4);
    let mut commit_random = |env: &Env, model: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
        let mut ops = Vec::new();
        for _ in 0..rng.gen_range(1..200) {
            let key = format!("k{:04}", rng.gen_range(0..4000)).into_bytes();
            if rng.gen_bool(0.2) {
                model.remove(&key);
                ops.push(remove(&key));
            } else {
                let len = if rng.gen_bool(0.05) { rng.gen_range(9000..20_000) } else { rng.gen_range(0..80) };
                let value = vec![rng.gen(); len];
                model.insert(key.clone(), value.clone());
                ops.push(put(&key, &value));
            }
        }
        env.commit(ops).unwrap();
    };
    for _ in 0..3 {
        commit_random(&env, &mut model);
    }
    for _round in 0..8 {
        // pause the checkpoint after it planned what to write; commit meanwhile
        let (entered_tx, entered) = mpsc::channel::<()>();
        let (go, go_rx) = mpsc::channel::<()>();
        let (entered_tx, go_rx) = (Mutex::new(entered_tx), Mutex::new(go_rx));
        *env.before_write.lock().unwrap() = Some(Box::new(move || {
            entered_tx.lock().unwrap().send(()).unwrap();
            go_rx.lock().unwrap().recv().unwrap();
        }));
        let checkpointer = {
            let env = env.clone();
            std::thread::spawn(move || env.checkpoint().unwrap())
        };
        entered.recv().unwrap();
        for _ in 0..5 {
            commit_random(&env, &mut model);
        }
        go.send(()).unwrap();
        checkpointer.join().unwrap();
        *env.before_write.lock().unwrap() = None;
        for _ in 0..3 {
            commit_random(&env, &mut model);
        }
        for (k, v) in &model {
            assert_eq!(env.get(k).unwrap().as_deref(), Some(v.as_slice()));
        }
    }
    check_all(&env, &model);
    drop(env);
    let env = open_in(&dir);
    check_all(&env, &model);
}

#[test]
fn parallel_ranges_match_sequential_ones() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut rng = StdRng::seed_from_u64(21);
    let mut model = BTreeMap::new();
    let key = |i: u32| format!("k{i:06}").into_bytes();
    for round in 0..3 {
        let mut ops = Vec::new();
        for _ in 0..20_000 {
            let i: u32 = rng.gen_range(0..60_000);
            // a few values large enough to be stored apart from their leaf
            let v = if rng.gen_ratio(1, 500) { vec![round as u8; 20_000] } else { i.to_le_bytes().repeat(rng.gen_range(1..8)) };
            model.insert(key(i), v.clone());
            ops.push(Op::Put(key(i), v));
        }
        env.commit(ops).unwrap();
        if round == 1 {
            env.checkpoint().unwrap();
        }
    }
    let snapshot = env.snapshot();
    // keeps keys whose value starts with an even byte, as a filter would
    let map = |k: &[u8], v: &[u8]| -> io::Result<Option<(Vec<u8>, Vec<u8>)>> { Ok((v[0] % 2 == 0).then(|| (k.to_vec(), v.to_vec()))) };
    for _ in 0..200 {
        let (a, b): (u32, u32) = (rng.gen_range(0..62_000), rng.gen_range(0..62_000));
        let (start, end) = (key(a.min(b)), key(a.max(b)));
        let stop_after = rng.gen_range(1..40);
        let expected: Vec<(Vec<u8>, Vec<u8>)> =
            model.range(start.clone()..end.clone()).filter(|(_, v)| v[0] % 2 == 0).map(|(k, v)| (k.clone(), v.clone())).collect();
        let (mut got, mut last_seen, mut calls) = (Vec::new(), Vec::new(), 0);
        snapshot
            .par_range(&start, &end, rng.gen_range(1..8), &map, |found, last| {
                assert!(last >= &start[..] && last < &end[..] && last > &last_seen[..]);
                assert!(found.iter().all(|(k, _)| k.as_slice() <= last));
                last_seen = last.to_vec();
                got.extend(found);
                calls += 1;
                calls < stop_after
            })
            .unwrap();
        if calls < stop_after {
            assert_eq!(got, expected, "{a}..{b}");
            let in_range = model.range(start.clone()..end.clone()).next_back().map(|(k, _)| k.clone()).unwrap_or_default();
            assert_eq!(last_seen, in_range, "the last key visited is the last in range");
        } else {
            // stopped early: a prefix of the result, up to the last key visited
            let prefix: Vec<_> = expected.iter().take_while(|(k, _)| k <= &last_seen).cloned().collect();
            assert_eq!(got, prefix);
        }
    }
}

#[test]
fn large_batches_check_conditions_like_one_group_after_another() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut rng = StdRng::seed_from_u64(33);
    let key = |i: u32| format!("key{i:03}").into_bytes();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for round in 0..20 {
        // few keys, many groups: later groups often see what earlier ones of the batch wrote
        let groups: Vec<Group> = (0..rng.gen_range(1..300))
            .map(|_| {
                let conditions = (0..rng.gen_range(0..3))
                    .map(|_| {
                        let k = key(rng.gen_range(0..60));
                        match rng.gen_range(0..3) {
                            0 => Condition::Absent(k),
                            1 => Condition::Present(k),
                            _ => Condition::Equals(k, vec![rng.gen_range(0..4)]),
                        }
                    })
                    .collect();
                let ops = (0..rng.gen_range(1..3))
                    .map(|_| {
                        let k = key(rng.gen_range(0..60));
                        if rng.gen_ratio(1, 4) { Op::Remove(k) } else { Op::Put(k, vec![rng.gen_range(0..4)]) }
                    })
                    .collect();
                Group { conditions, ops, chain: rng.gen_range(0..3) }
            })
            .collect();
        // the model applies the groups one after another
        let mut expected = Vec::new();
        let mut broken = std::collections::HashSet::new();
        for g in &groups {
            let ok = (g.chain == 0 || !broken.contains(&g.chain))
                && g.conditions.iter().all(|c| match c {
                    Condition::Absent(k) => !model.contains_key(k),
                    Condition::Present(k) => model.contains_key(k),
                    Condition::Equals(k, v) => model.get(k) == Some(v),
                    Condition::Unique { .. } => unreachable!(),
                });
            if ok {
                for op in &g.ops {
                    match op {
                        Op::Put(k, v) => {
                            model.insert(k.clone(), v.clone());
                        }
                        Op::Remove(k) => {
                            model.remove(k);
                        }
                        Op::Add(..) | Op::RemoveRange(..) => unreachable!(),
                    }
                }
            } else if g.chain != 0 {
                broken.insert(g.chain);
            }
            expected.push(ok);
        }
        assert_eq!(env.commit_groups(groups, &identity).unwrap(), expected, "round {round}");
    }
    check_all(&env, &model);
}

#[test]
fn lookups_that_reuse_a_leaf_answer_like_fresh_ones() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut rng = StdRng::seed_from_u64(44);
    let key = |i: u32| format!("k{:07}", i).into_bytes();
    let mut model = BTreeMap::new();
    // every third key, so lookups between existing keys are misses
    for chunk in (0..60_000u32).step_by(3).collect::<Vec<_>>().chunks(5000) {
        let ops: Vec<Op> = chunk.iter().map(|&i| put(&key(i), &i.to_le_bytes().repeat(1 + i as usize % 20))).collect();
        for &i in chunk {
            model.insert(key(i), i.to_le_bytes().repeat(1 + i as usize % 20));
        }
        env.commit(ops).unwrap();
    }
    let snapshot = env.snapshot();
    let mut lookup = btree::Lookup::new(snapshot.view(), snapshot.root);
    let mut probes: Vec<Vec<u8>> = (0..62_000u32).step_by(7).map(key).collect(); // in order
    probes.extend((0..20_000).map(|_| key(rng.gen_range(0..62_000)))); // anywhere
    probes.extend([b"".to_vec(), b"a".to_vec(), b"z".to_vec(), key(0), key(59_997), key(59_998)]);
    for probe in &probes {
        assert_eq!(lookup.get_with(probe, |v| v.to_vec()).unwrap(), model.get(probe).cloned(), "{:?}", String::from_utf8_lossy(probe));
    }
}

fn counter_of(env: &Env, key: &[u8]) -> i64 {
    env.get(key).unwrap().map_or(0, |v| i64::from_le_bytes(v[1..9].try_into().unwrap()))
}

#[test]
fn ranges_are_removed_like_their_keys_one_by_one() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut rng = StdRng::seed_from_u64(55);
    let key = |i: u32| format!("r{i:07}").into_bytes();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let count_key = b"~count".to_vec();
    let mut counted: i64 = 0;
    for round in 0..40 {
        // refill: small and large values (large ones are stored apart from their leaf)
        let mut ops = Vec::new();
        for _ in 0..rng.gen_range(1..20_000) {
            let i: u32 = rng.gen_range(0..200_000);
            let v = if rng.gen_ratio(1, 300) { vec![round as u8; 9000] } else { i.to_le_bytes().repeat(rng.gen_range(1..10)) };
            if model.insert(key(i), v.clone()).is_none() {
                counted += 1;
            }
            ops.push(Op::Put(key(i), v));
        }
        let n = ops.len() as i64;
        env.commit(ops).unwrap();
        env.commit(vec![Op::Add(count_key.clone(), counted - counter_of(&env, &count_key))]).unwrap();
        let _ = n;
        // one to three ranges, sometimes empty, sometimes the whole tree, sometimes a sliver
        let ranges: Vec<(Vec<u8>, Vec<u8>, Option<Vec<u8>>)> = (0..rng.gen_range(1..4))
            .map(|_| {
                let (a, b) = (rng.gen_range(0..210_000u32), rng.gen_range(0..210_000u32));
                let (a, b) = match rng.gen_range(0..6) {
                    0 => (0, 210_000),
                    1 => (a, a + rng.gen_range(0..5)),
                    _ => (a.min(b), a.max(b)),
                };
                let start = if rng.gen_ratio(1, 8) { b"".to_vec() } else { key(a) };
                (start, key(b), Some(count_key.clone()))
            })
            .collect();
        let extra = vec![Op::Put(b"~marker".to_vec(), vec![round as u8])];
        let counts = env.delete_ranges(&ranges, extra).unwrap();
        for ((start, end, _), &n) in ranges.iter().zip(&counts) {
            let gone: Vec<Vec<u8>> = model.range(start.clone()..end.clone()).map(|(k, _)| k.clone()).collect();
            assert_eq!(n, gone.len() as u64, "round {round}: keys removed from {:?}", String::from_utf8_lossy(start));
            for k in gone {
                model.remove(&k);
            }
            counted -= n as i64;
        }
        model.insert(b"~marker".to_vec(), vec![round as u8]);
        assert_eq!(counter_of(&env, &count_key), counted, "the counter follows the removals");
        if round % 7 == 3 {
            env.checkpoint().unwrap();
        }
        if round % 10 == 9 {
            // every block owned exactly once: removed subtrees freed, nothing freed twice
            let mut with_counter = model.clone();
            with_counter.insert(count_key.clone(), env.get(&count_key).unwrap().unwrap());
            check_all(&env, &with_counter);
        }
    }
    let snapshot = env.snapshot();
    let mut all = Vec::new();
    snapshot.range(b"", &[0xff], |k, v| {
        all.push((k.to_vec(), v.to_vec()));
        true
    }).unwrap();
    let mut expected: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
    expected.push((count_key.clone(), env.get(&count_key).unwrap().unwrap()));
    expected.sort();
    assert_eq!(all, expected);
}

#[test]
fn a_crash_after_removing_ranges_replays_them() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    env.commit((0..30_000u32).map(|i| put(&i.to_be_bytes(), &[7; 40])).collect()).unwrap();
    env.checkpoint().unwrap();
    env.commit(vec![Op::Add(b"n".to_vec(), 30_000)]).unwrap();
    let counts = env
        .delete_ranges(
            &[(1000u32.to_be_bytes().to_vec(), 25_000u32.to_be_bytes().to_vec(), Some(b"n".to_vec())), (29_999u32.to_be_bytes().to_vec(), 30_000u32.to_be_bytes().to_vec(), None)],
            vec![put(b"after", b"1")],
        )
        .unwrap();
    assert_eq!(counts, vec![24_000, 1]);
    let copy = crash_copy(&dir);
    let recovered = open_in(&copy);
    for (i, present) in [(999u32, true), (1000, false), (24_999, false), (25_000, true), (29_998, true), (29_999, false)] {
        assert_eq!(recovered.get(&i.to_be_bytes()).unwrap().is_some(), present, "key {i}");
    }
    assert_eq!(recovered.get(b"after").unwrap(), Some(b"1".to_vec()));
    assert_eq!(counter_of(&recovered, b"n"), 6000);
    check_invariants(&recovered);
}

#[test]
fn descending_ranges_mirror_ascending_ones() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut rng = StdRng::seed_from_u64(66);
    let key = |i: u32| format!("d{i:06}").into_bytes();
    let mut model = BTreeMap::new();
    for round in 0..3 {
        let mut ops = Vec::new();
        for _ in 0..15_000 {
            let i: u32 = rng.gen_range(0..50_000);
            let v = if rng.gen_ratio(1, 400) { vec![round as u8; 9000] } else { i.to_le_bytes().to_vec() };
            model.insert(key(i), v.clone());
            ops.push(Op::Put(key(i), v));
        }
        env.commit(ops).unwrap();
        if round == 1 {
            env.checkpoint().unwrap();
        }
    }
    let snapshot = env.snapshot();
    for _ in 0..300 {
        let (a, b): (u32, u32) = (rng.gen_range(0..52_000), rng.gen_range(0..52_000));
        let (start, end) = (key(a.min(b)), key(a.max(b)));
        let stop = rng.gen_range(1..2000);
        let expected: Vec<(Vec<u8>, Vec<u8>)> = model.range(start.clone()..end.clone()).rev().take(stop).map(|(k, v)| (k.clone(), v.clone())).collect();
        let mut got = Vec::new();
        snapshot
            .range_rev(&start, &end, |k, v| {
                got.push((k.to_vec(), v.to_vec()));
                got.len() < stop
            })
            .unwrap();
        assert_eq!(got, expected, "{a}..{b} stop {stop}");
    }
    // bounds that fall between keys, and the whole tree
    let mut all = Vec::new();
    snapshot.range_rev(b"", &[0xff], |k, _| {
        all.push(k.to_vec());
        true
    }).unwrap();
    assert_eq!(all, model.keys().rev().cloned().collect::<Vec<_>>());
}

#[test]
fn unique_conditions_admit_one_key_per_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_in(&dir);
    let mut rng = StdRng::seed_from_u64(77);
    // entries "u<value>" 0 "<id>": at most one id per value
    let entry = |value: u32, id: u32| [format!("u{value:03}").into_bytes(), vec![0], format!("{id:04}").into_bytes()].concat();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for round in 0..30 {
        let mut groups = Vec::new();
        let mut expected = Vec::new();
        for _ in 0..rng.gen_range(1..200) {
            let (value, id) = (rng.gen_range(0..40), rng.gen_range(0..60));
            let key = entry(value, id);
            if rng.gen_ratio(1, 3) {
                // remove some entry of that value, whoever holds it
                let held: Vec<Vec<u8>> = model.keys().filter(|k| k.starts_with(format!("u{value:03}").as_bytes())).cloned().collect();
                if let Some(k) = held.first() {
                    model.remove(k);
                    groups.push(Group { conditions: vec![], ops: vec![remove(k)], chain: 0 });
                    expected.push(true);
                    continue;
                }
            }
            let prefix = format!("u{value:03}").into_bytes();
            let ok = !model.keys().any(|k| k.starts_with(&[prefix.clone(), vec![0]].concat()) && *k != key);
            if ok {
                model.insert(key.clone(), vec![round as u8]);
            }
            expected.push(ok);
            groups.push(Group { conditions: vec![Condition::Unique { key: key.clone(), prefix_len: prefix.len() }], ops: vec![put(&key, &[round as u8])], chain: 0 });
        }
        assert_eq!(env.commit_groups(groups, &identity).unwrap(), expected, "round {round}");
        if round % 10 == 5 {
            env.checkpoint().unwrap();
        }
    }
    check_all(&env, &model);
}
