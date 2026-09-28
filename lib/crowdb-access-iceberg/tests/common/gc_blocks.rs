use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use crowdb_access_iceberg::file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError};
use crowdb_chunk_client::ReclaimOutcome;

#[derive(Default)]
pub struct TestReclaimBlocks {
    pub blocks: crate::blocks::TestBlocks,
    pub deferred: AtomicBool,
    pub deletes: AtomicUsize,
    pub reply_loss: AtomicBool,
    pub delay_ms: AtomicUsize,
}

#[async_trait]
impl FileBlockStore for TestReclaimBlocks {
    async fn put(&self, owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        self.blocks.put(owner, height, bytes).await
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.blocks.read(root).await
    }

    async fn reclaim(&self, root: &ChunkRoot) -> Result<ReclaimOutcome, FileIoError> {
        let delay = self.delay_ms.load(Ordering::Relaxed);
        if delay != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay as u64)).await;
        }
        if self.deferred.load(Ordering::Relaxed) {
            return Ok(ReclaimOutcome::Deferred);
        }
        self.blocks.values.rcu(|values| {
            let mut next = (**values).clone();
            next.remove(&root.chunk.low);
            next
        });
        self.deletes.fetch_add(1, Ordering::Relaxed);
        if self.reply_loss.swap(false, Ordering::Relaxed) {
            return Err(crowdb_chunk_client::IoError::Internal("lost deletion response".into()).into());
        }
        Ok(ReclaimOutcome::Reclaimed)
    }
}
