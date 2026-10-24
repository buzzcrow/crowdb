use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError, CatalogStore, StoreError, StoredValue},
    error::ValidationError,
    file::DeletedFile,
    gc::{GcCandidate, GcLimits, GcRepository, GcScan, GcStore, GcSystemScan},
    record::StorageRecord,
};

use super::FileRepository;

impl FileRepository {
    pub(super) async fn retain_deleted(
        &self,
        context: CatalogContext,
        deleted: &DeletedFile,
    ) -> Result<(), CatalogError> {
        let key = GcCandidate::deleted_key(&deleted.file);
        let candidate = if let Some(value) = self.store.get(&key.encode()?).await? {
            let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            *candidate
        } else {
            // Replacing the filename removes its deletion marker. Transfer the
            // complete old resource to both durable GC indexes first.
            let gc = GcRepository::new(Arc::new(PublicationGcStore(self.store.clone())));
            Box::pin(gc.claim_deleted_file(context, deleted, deleted.deleted_ms, GcLimits::default())).await?
        };
        if candidate.file != deleted.file || candidate.part.is_some() || candidate.assembly.is_some() {
            return Err(CatalogError::Conflict);
        }
        Ok(())
    }
}

// Publication only needs existing GC admission point operations. This adapter
// deliberately cannot scan or delete resources through a CatalogStore handle.
struct PublicationGcStore(Arc<dyn CatalogStore>);

#[async_trait]
impl CatalogStore for PublicationGcStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.0.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.0.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl GcStore for PublicationGcStore {
    async fn scan_gc(&self, _request: GcScan) -> Result<MultiScanPage, StoreError> {
        Err(StoreError::Response)
    }

    async fn scan_gc_system(&self, _request: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        Err(StoreError::Response)
    }

    async fn delete_gc_record(
        &self,
        _key: &[u8],
        _expected: &[u8],
        _identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        Err(StoreError::Response)
    }
}
