use std::sync::Arc;

use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{CasOutcome, CatalogStore, StoreError, StoredValue};
use crowdb_access_iceberg::file::{MultipartPartScan, MultipartPartStore, MultipartSession};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

use crate::common::TestStore;

pub struct TestPartRace {
    pub inner: Arc<TestStore>,
    pub session: MultipartSession,
}

#[async_trait]
impl CatalogStore for TestPartRace {
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
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl MultipartPartStore for TestPartRace {
    async fn scan_multipart_parts(&self, scan: MultipartPartScan) -> Result<MultiScanPage, StoreError> {
        let page = self.inner.scan_multipart_parts(scan).await?;
        let key = self.session.key().encode()?;
        let expected = StorageRecord::MultipartSession(Box::new(self.session.clone())).encode()?;
        let mut next = self.session.clone();
        next.revision += 1;
        let value = StorageRecord::MultipartSession(Box::new(next)).encode()?;
        assert!(matches!(
            self.inner
                .compare_exchange(
                    &key,
                    Some(&expected),
                    &value,
                    mutation_identity(&key, Some(&expected), &value)
                )
                .await?,
            CasOutcome::Applied(_)
        ));
        Ok(page)
    }
}
