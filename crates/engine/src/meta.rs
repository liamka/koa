//! Pages 0 and 1 hold two copies of the meta record. Commit `txn` writes slot `txn % 2`,
//! so the previous meta always survives a torn write; on open the valid meta with the
//! highest txn id wins.
//!
//! Layout: [magic u64][version u32][block u32][txn u64][root ref u64][freelist block u64][end u64][crc32 u32]

use crate::page::{u64_at, BLOCK};

const MAGIC: u64 = u64::from_le_bytes(*b"MOSTIK\0\x01");
const VERSION: u32 = 5;
const BODY: usize = 48;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Meta {
    pub txn: u64,
    /// 0 = empty tree
    pub root: u64,
    /// first block of the freelist chain; 0 = none
    pub freelist: u64,
    /// Blocks in use, meta blocks included; the file grows from here.
    pub end: u64,
}

impl Meta {
    pub fn empty(txn: u64) -> Meta {
        Meta { txn, root: 0, freelist: 0, end: 2 }
    }

    pub fn slot(&self) -> u64 {
        self.txn % 2
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut page = vec![0u8; BLOCK];
        page[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        page[8..12].copy_from_slice(&VERSION.to_le_bytes());
        page[12..16].copy_from_slice(&(BLOCK as u32).to_le_bytes());
        page[16..24].copy_from_slice(&self.txn.to_le_bytes());
        page[24..32].copy_from_slice(&self.root.to_le_bytes());
        page[32..40].copy_from_slice(&self.freelist.to_le_bytes());
        page[40..48].copy_from_slice(&self.end.to_le_bytes());
        let crc = crc32fast::hash(&page[..BODY]);
        page[BODY..BODY + 4].copy_from_slice(&crc.to_le_bytes());
        page
    }

    pub fn decode(page: &[u8]) -> Option<Meta> {
        if page.len() < BODY + 4 || u64_at(page, 0) != MAGIC {
            return None;
        }
        let crc = u32::from_le_bytes(page[BODY..BODY + 4].try_into().unwrap());
        if crc != crc32fast::hash(&page[..BODY]) {
            return None;
        }
        let version = u32::from_le_bytes(page[8..12].try_into().unwrap());
        let page_size = u32::from_le_bytes(page[12..16].try_into().unwrap());
        if version != VERSION || page_size as usize != BLOCK {
            return None;
        }
        Some(Meta {
            txn: u64_at(page, 16),
            root: u64_at(page, 24),
            freelist: u64_at(page, 32),
            end: u64_at(page, 40),
        })
    }

    /// Picks the newest valid meta out of the two slots.
    pub fn newest(file: &[u8]) -> Option<Meta> {
        let a = file.get(..BLOCK).and_then(Meta::decode);
        let b = file.get(BLOCK..2 * BLOCK).and_then(Meta::decode);
        match (a, b) {
            (Some(a), Some(b)) => Some(if a.txn >= b.txn { a } else { b }),
            (a, b) => a.or(b),
        }
    }
}
