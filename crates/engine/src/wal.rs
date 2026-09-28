//! The journal: every commit appends its final operations here before it is acknowledged;
//! the tree pages it changed stay in memory until a checkpoint writes them. After a crash the
//! database is the last checkpoint plus the journal records that follow it.
//!
//! Record: `[len u32][crc32 u32][txn u64][ops]`, `len` counting txn and ops, the crc covering
//! them. Ops: `[kind u8][key_len u32][key]` then, for a put, `[value_len u32][value]`, for an
//! add, `[delta i64]`, for a range removal (key: its start), `[end_len u32][end]`. A record that is cut short or fails its crc ends the journal.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

use crate::btree::Op;

const PUT: u8 = 1;
const REMOVE: u8 = 2;
const ADD: u8 = 3;
const REMOVE_RANGE: u8 = 4;
const HEADER: usize = 8;

pub struct Wal {
    pub file: File,
    /// bytes of valid records
    pub len: u64,
}

impl Wal {
    /// Opens the journal and returns the valid records it holds, in order.
    pub fn open(file: File) -> io::Result<(Wal, Vec<(u64, Vec<Op>)>)> {
        let size = file.metadata()?.len();
        let mut bytes = vec![0u8; size as usize];
        file.read_exact_at(&mut bytes, 0)?;
        let (mut records, mut at) = (Vec::new(), 0usize);
        while let Some((txn, ops, next)) = decode_record(&bytes, at) {
            records.push((txn, ops));
            at = next;
        }
        if (at as u64) < size {
            // a torn tail from a crash: drop it so new records follow the valid ones
            file.set_len(at as u64)?;
        }
        Ok((Wal { file, len: at as u64 }, records))
    }

    pub fn append(&mut self, txn: u64, ops: &[Op]) -> io::Result<()> {
        let record = encode_record(txn, ops);
        self.file.write_all_at(&record, self.len)?;
        self.len += record.len() as u64;
        Ok(())
    }

    /// Everything up to here is in a checkpoint: start over.
    pub fn reset(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.len = 0;
        Ok(())
    }
}

fn encode_record(txn: u64, ops: &[Op]) -> Vec<u8> {
    let size: usize = ops
        .iter()
        .map(|op| {
            1 + 4
                + op.key().len()
                + match op {
                    Op::Put(_, v) => 4 + v.len(),
                    Op::Remove(_) => 0,
                    Op::Add(..) => 8,
                    Op::RemoveRange(_, end) => 4 + end.len(),
                }
        })
        .sum();
    let mut out = Vec::with_capacity(HEADER + 8 + size);
    out.extend_from_slice(&[0; HEADER]);
    out.extend_from_slice(&txn.to_le_bytes());
    for op in ops {
        let kind = match op {
            Op::Put(..) => PUT,
            Op::Remove(_) => REMOVE,
            Op::Add(..) => ADD,
            Op::RemoveRange(..) => REMOVE_RANGE,
        };
        out.push(kind);
        out.extend_from_slice(&(op.key().len() as u32).to_le_bytes());
        out.extend_from_slice(op.key());
        match op {
            Op::Put(_, v) => {
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                out.extend_from_slice(v);
            }
            Op::Remove(_) => {}
            Op::Add(_, delta) => out.extend_from_slice(&delta.to_le_bytes()),
            Op::RemoveRange(_, end) => {
                out.extend_from_slice(&(end.len() as u32).to_le_bytes());
                out.extend_from_slice(end);
            }
        }
    }
    let body_len = (out.len() - HEADER) as u32;
    let crc = crc32fast::hash(&out[HEADER..]);
    out[..4].copy_from_slice(&body_len.to_le_bytes());
    out[4..8].copy_from_slice(&crc.to_le_bytes());
    out
}

fn decode_record(bytes: &[u8], at: usize) -> Option<(u64, Vec<Op>, usize)> {
    let len = u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?) as usize;
    let crc = u32::from_le_bytes(bytes.get(at + 4..at + 8)?.try_into().ok()?);
    let body = bytes.get(at + HEADER..at + HEADER + len)?;
    if len < 8 || crc32fast::hash(body) != crc {
        return None;
    }
    let txn = u64::from_le_bytes(body[..8].try_into().ok()?);
    let (mut ops, mut i) = (Vec::new(), 8);
    let take = |i: &mut usize, n: usize| -> Option<&[u8]> {
        let b = body.get(*i..*i + n)?;
        *i += n;
        Some(b)
    };
    while i < body.len() {
        let kind = take(&mut i, 1)?[0];
        let key_len = u32::from_le_bytes(take(&mut i, 4)?.try_into().ok()?) as usize;
        let key = take(&mut i, key_len)?.to_vec();
        ops.push(match kind {
            PUT => {
                let value_len = u32::from_le_bytes(take(&mut i, 4)?.try_into().ok()?) as usize;
                Op::Put(key, take(&mut i, value_len)?.to_vec())
            }
            REMOVE => Op::Remove(key),
            ADD => Op::Add(key, i64::from_le_bytes(take(&mut i, 8)?.try_into().ok()?)),
            REMOVE_RANGE => {
                let end_len = u32::from_le_bytes(take(&mut i, 4)?.try_into().ok()?) as usize;
                Op::RemoveRange(key, take(&mut i, end_len)?.to_vec())
            }
            _ => return None,
        });
    }
    Some((txn, ops, at + HEADER + len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_torn_tails_are_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let open = || std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path).unwrap();
        let (mut wal, records) = Wal::open(open()).unwrap();
        assert!(records.is_empty());
        wal.append(
            1,
            &[Op::Put(b"k".to_vec(), b"v".to_vec()), Op::Remove(b"r".to_vec()), Op::Add(b"n".to_vec(), -3), Op::RemoveRange(b"a".to_vec(), b"c".to_vec())],
        )
        .unwrap();
        wal.append(2, &[Op::Put(b"k2".to_vec(), vec![7; 10_000])]).unwrap();
        let full = wal.len;
        wal.append(3, &[Op::Put(b"k3".to_vec(), b"v3".to_vec())]).unwrap();
        // cut the last record short, as a crash in the middle of an append would
        wal.file.set_len(wal.len - 3).unwrap();
        let (wal, records) = Wal::open(open()).unwrap();
        assert_eq!(records.iter().map(|(txn, ops)| (*txn, ops.len())).collect::<Vec<_>>(), vec![(1, 4), (2, 1)]);
        assert!(matches!(&records[0].1[3], Op::RemoveRange(s, e) if s == b"a" && e == b"c"));
        assert_eq!(wal.len, full);
        assert_eq!(wal.file.metadata().unwrap().len(), full);
        // a flipped byte fails the crc: the journal ends before that record
        wal.file.write_all_at(&[0xff], 20).unwrap();
        let (_, records) = Wal::open(open()).unwrap();
        assert!(records.is_empty());
    }
}
