use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError};
use crowdb_protocol::common::ChunkId;
use sha2::{Digest, Sha256};

#[derive(Default)]
pub struct TestBlocks {
    pub values: ArcSwap<BTreeMap<u64, Arc<Vec<u8>>>>,
    pub writes: AtomicUsize,
    pub reads: AtomicUsize,
    pub max_input: AtomicUsize,
    pub fail_after: AtomicUsize,
    pub corrupt_reads: AtomicBool,
    pub pause_reads: AtomicBool,
    pub read_entered: tokio::sync::Notify,
    pub read_release: tokio::sync::Notify,
}

#[async_trait]
impl FileBlockStore for TestBlocks {
    async fn put(&self, _owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        self.max_input.fetch_max(bytes.len(), Ordering::SeqCst);
        let index = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        self.values.rcu(|values| {
            let mut next = (**values).clone();
            next.insert(index as u64, Arc::new(bytes.to_vec()));
            next
        });
        if self.fail_after.load(Ordering::SeqCst) == index {
            return Err(FileIoError::Bounds);
        }
        Ok(ChunkRoot {
            chunk: ChunkId {
                high: 1,
                low: index as u64,
            },
            offset: 0,
            physical_length: bytes.len() as u64 + 64,
            logical_offset: 0,
            logical_length: bytes.len() as u64,
            height,
            digest: Sha256::digest(bytes).into(),
        })
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        if self.pause_reads.load(Ordering::SeqCst) {
            self.read_entered.notify_one();
            self.read_release.notified().await;
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        let mut bytes = self
            .values
            .load()
            .get(&root.chunk.low)
            .ok_or(FileIoError::Bounds)?
            .as_ref()
            .clone();
        if self.corrupt_reads.load(Ordering::SeqCst) {
            bytes[0] ^= 1;
        }
        Ok(bytes)
    }
}
