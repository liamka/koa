//! Node.js binding. Keys and values arrive already encoded by the JS layer;
//! here values get a one-byte header and optional LZ4 compression.

use std::path::Path;
use std::sync::Arc;

use mostik_engine::{Env as Store, Op};
use napi::bindgen_prelude::*;
use napi_derive::napi;

const RAW: u8 = 0;
const LZ4: u8 = 1;

const OP_PUT: u8 = 1;
const OP_REMOVE: u8 = 2;

fn to_napi(e: std::io::Error) -> Error {
    Error::from_reason(e.to_string())
}

#[napi]
pub struct NativeEnv {
    store: Option<Arc<Store>>,
}

#[napi]
impl NativeEnv {
    #[napi(factory)]
    pub fn open(path: String, lock_path: String) -> Result<NativeEnv> {
        let store = Store::open(Path::new(&path), Path::new(&lock_path)).map_err(to_napi)?;
        Ok(NativeEnv { store: Some(store) })
    }

    fn store(&self) -> Result<&Arc<Store>> {
        self.store.as_ref().ok_or_else(|| Error::from_reason("The database has been closed"))
    }

    /// Reads the value of `key[..key_len]` into `out`, reusing JS buffers instead of allocating
    /// one per call. Returns -1 when the key is missing, otherwise the value length; a length
    /// larger than `out` means nothing was written and the caller should retry with a bigger buffer.
    #[napi]
    pub fn get_into(&self, key: BufferSlice, key_len: u32, mut out: BufferSlice) -> Result<i64> {
        let key = key.get(..key_len as usize).ok_or_else(|| Error::from_reason("key_len out of range"))?;
        let read = self.store()?.get_with(key, |stored| decode_value_into(stored, &mut out)).map_err(to_napi)?;
        read.map_or(Ok(-1), |len| len.map(|len| len as i64))
    }

    /// Commits a batch encoded as `[op u8][key_len u32][key]([value_len u32][value])` records.
    #[napi]
    pub fn commit(&self, batch: Buffer, compression_threshold: u32) -> Result<AsyncTask<Commit>> {
        Ok(AsyncTask::new(Commit {
            store: self.store()?.clone(),
            batch: batch.to_vec(),
            threshold: compression_threshold as usize,
        }))
    }

    #[napi]
    pub fn close(&mut self) {
        self.store = None;
    }
}

fn decode_value_into(stored: &[u8], out: &mut [u8]) -> Result<usize> {
    let corrupted = || Error::from_reason("mostik: database is corrupted (value)");
    match stored.split_first() {
        Some((&RAW, v)) => {
            if let Some(dst) = out.get_mut(..v.len()) {
                dst.copy_from_slice(v);
            }
            Ok(v.len())
        }
        Some((&LZ4, v)) => {
            let (size, block) = lz4_flex::block::uncompressed_size(v).map_err(|_| corrupted())?;
            if let Some(dst) = out.get_mut(..size) {
                if lz4_flex::block::decompress_into(block, dst).map_err(|_| corrupted())? != size {
                    return Err(corrupted());
                }
            }
            Ok(size)
        }
        _ => Err(corrupted()),
    }
}

fn encode_value(value: &[u8], threshold: usize) -> Vec<u8> {
    if threshold > 0 && value.len() > threshold {
        let compressed = lz4_flex::compress_prepend_size(value);
        if compressed.len() < value.len() {
            let mut out = Vec::with_capacity(compressed.len() + 1);
            out.push(LZ4);
            out.extend_from_slice(&compressed);
            return out;
        }
    }
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(RAW);
    out.extend_from_slice(value);
    out
}

pub struct Commit {
    store: Arc<Store>,
    batch: Vec<u8>,
    threshold: usize,
}

fn parse_batch(batch: &[u8], threshold: usize) -> Result<Vec<Op>> {
    let bad = || Error::from_reason("mostik: malformed write batch");
    let mut ops = Vec::new();
    let mut at = 0;
    let chunk = |at: &mut usize| -> Result<&[u8]> {
        let len = u32::from_le_bytes(batch.get(*at..*at + 4).ok_or_else(bad)?.try_into().unwrap()) as usize;
        let bytes = batch.get(*at + 4..*at + 4 + len).ok_or_else(bad)?;
        *at += 4 + len;
        Ok(bytes)
    };
    while at < batch.len() {
        let op = batch[at];
        at += 1;
        let key = chunk(&mut at)?.to_vec();
        ops.push(match op {
            OP_PUT => Op::Put(key, encode_value(chunk(&mut at)?, threshold)),
            OP_REMOVE => Op::Remove(key),
            _ => return Err(bad()),
        });
    }
    Ok(ops)
}

impl Task for Commit {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        let ops = parse_batch(&self.batch, self.threshold)?;
        self.store.commit(ops).map_err(to_napi)
    }

    fn resolve(&mut self, _env: napi::Env, _output: ()) -> Result<()> {
        Ok(())
    }
}
