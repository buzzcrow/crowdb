use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{CasOutcome, CatalogStore, RootState, StoreError, StoredValue};
use crowdb_access_iceberg::key::IcebergKey;
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_protocol::chunk_kv::ClientRequestId;

pub struct TestFaultStore {
    pub inner: Arc<dyn CatalogStore>,
    pub mode: AtomicU8,
}

#[async_trait]
impl CatalogStore for TestFaultStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let record = StorageRecord::decode(&IcebergKey::decode(key)?, value)?;
        let intercept = matches!(&record, StorageRecord::Active(root) if root.state == RootState::Fencing)
            || (self.mode.load(Ordering::SeqCst) == 3
                && expected.is_some()
                && matches!(&record, StorageRecord::NamespaceAuthority(authority) if authority.pending_operation.is_some()));
        let mode = if intercept {
            self.mode.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if mode == 1 {
            return Err(StoreError::Response);
        }
        let result = self
            .inner
            .compare_exchange(key, expected, value, identity)
            .await?;
        if mode == 2 || mode == 3 {
            return Err(StoreError::Response);
        }
        Ok(result)
    }
}
