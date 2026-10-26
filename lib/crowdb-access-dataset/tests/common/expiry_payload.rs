use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{PayloadDurability, PayloadStorage, UploadError};
use crowdb_chunk_client::SmallWriteIntent;
use crowdb_protocol::chunkdb::rpc::Location;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

#[derive(Default)]
pub struct TestExpiryPayload {
    inner: crate::upload::TestPayload,
    pub deferred_through: AtomicU64,
    pub attempted_ranges: AtomicU64,
    pub stall: AtomicBool,
}

#[async_trait]
impl PayloadStorage for TestExpiryPayload {
    async fn write(
        &self,
        key: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        self.inner.write(key, data, intent).await
    }
    async fn read(&self, locations: &[Location]) -> Result<Bytes, UploadError> {
        self.inner.read(locations).await
    }
    async fn durability(&self, locations: &[Location]) -> Result<PayloadDurability, UploadError> {
        self.inner.durability(locations).await
    }
    async fn discard(&self, locations: &[Location]) -> Result<bool, UploadError> {
        self.attempted_ranges
            .fetch_add(locations.len() as u64, Ordering::SeqCst);
        if self.stall.load(Ordering::SeqCst) {
            futures::future::pending::<()>().await;
        }
        if locations
            .iter()
            .any(|location| location.chunk_id.unwrap().low <= self.deferred_through.load(Ordering::SeqCst))
        {
            return Ok(false);
        }
        self.inner.discard(locations).await
    }
}
