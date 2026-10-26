use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    key::IcebergKey,
    namespace::{ChildScan, NamespaceStore},
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

pub struct TestRenameConflicts {
    pub inner: Arc<crate::common::TestStore>,
    pub remaining: AtomicUsize,
    pub injected: AtomicUsize,
    awaiting_retry: AtomicBool,
}

impl TestRenameConflicts {
    pub fn new(inner: Arc<crate::common::TestStore>) -> Self {
        Self {
            inner,
            remaining: AtomicUsize::new(4),
            injected: AtomicUsize::new(0),
            awaiting_retry: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl CatalogStore for TestRenameConflicts {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(
            !self.awaiting_retry.load(Ordering::SeqCst),
            "CAS observed value must be used before another GET"
        );
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let decoded = IcebergKey::decode(key)?;
        if let (Some(expected), StorageRecord::NamespaceAuthority(candidate)) =
            (expected, StorageRecord::decode(&decoded, value)?)
        {
            if candidate.rename.is_some() && self.remaining.load(Ordering::SeqCst) > 0 {
                let StorageRecord::NamespaceAuthority(mut competitor) =
                    StorageRecord::decode(&decoded, expected)?
                else {
                    panic!()
                };
                self.remaining.fetch_sub(1, Ordering::SeqCst);
                self.injected.fetch_add(1, Ordering::SeqCst);
                competitor.mutation_revision += 1;
                let bytes = StorageRecord::NamespaceAuthority(competitor).encode()?;
                assert!(matches!(
                    self.inner
                        .compare_exchange(
                            key,
                            Some(expected),
                            &bytes,
                            mutation_identity(key, Some(expected), &bytes)
                        )
                        .await?,
                    CasOutcome::Applied(_)
                ));
                let observed = self.inner.get(key).await?;
                self.awaiting_retry.store(true, Ordering::SeqCst);
                return Ok(CasOutcome::Conflict(observed));
            }
        }
        let result = self.inner.compare_exchange(key, expected, value, identity).await;
        self.awaiting_retry.store(false, Ordering::SeqCst);
        result
    }
}

#[async_trait]
impl NamespaceStore for TestRenameConflicts {
    async fn scan_children(
        &self,
        scan: ChildScan,
    ) -> Result<crowdb_chunk_kv_client::MultiScanPage, StoreError> {
        self.inner.scan_children(scan).await
    }
    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete_mapping(key, expected, identity).await
    }
}
