#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
#[allow(dead_code)]
mod fixture;

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogError, CatalogStore, StoreError, StoredValue},
    file::{FileBlockStore, FileIdentity, FileWriteIntent},
    key::{FileId, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct TestChecks {
    store: Arc<common::TestStore>,
    active: AtomicUsize,
    peak: AtomicUsize,
    completed: AtomicUsize,
    mutations: AtomicUsize,
    fence_after: Option<(Vec<u8>, Vec<u8>)>,
}

struct TestRead<'a>(&'a AtomicUsize);
impl Drop for TestRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl CatalogStore for TestChecks {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _read = TestRead(&self.active);
        self.peak.fetch_max(active, Ordering::SeqCst);
        tokio::task::yield_now().await;
        let value = self.store.get(key).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        value
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        assert_eq!(
            self.active.load(Ordering::SeqCst),
            0,
            "the admission read group must finish before its mutation"
        );
        assert_eq!(self.completed.load(Ordering::SeqCst), 3);
        assert_eq!(self.mutations.fetch_add(1, Ordering::SeqCst), 0);
        let result = self
            .store
            .compare_exchange(key, expected, value, identity)
            .await?;
        if let Some((key, bytes)) = &self.fence_after {
            assert!(matches!(
                self.store
                    .compare_exchange(key, None, bytes, mutation_identity(key, None, bytes))
                    .await?,
                CasOutcome::Applied(_)
            ));
        }
        Ok(result)
    }
}

#[tokio::test]
async fn range_registration_checks_have_two_bounded_parallel_groups_around_one_cas() {
    for failure in 0..3 {
        let fixture = fixture::TestFile::new(common::TestStore::default()).await;
        let owner = FileIdentity {
            table: fixture.table,
            file: FileId::random(),
        };
        let root = blocks::TestBlocks::default()
            .put(owner, 0, b"block")
            .await
            .unwrap();
        let intent = FileWriteIntent {
            identity: OperationId::random(),
            owner,
            root,
            created_ms: 10,
            not_before_ms: 0,
            deleting: false,
        };
        let fence_after = if failure == 2 {
            let mut fence = intent.clone();
            fence.not_before_ms = 20;
            fence.deleting = true;
            Some((
                FileWriteIntent::fence_key(owner).encode().unwrap(),
                StorageRecord::FileWriteIntent(Box::new(fence)).encode().unwrap(),
            ))
        } else {
            None
        };
        if failure == 1 {
            fixture
                .store
                .fail_after
                .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        }
        let checks = Arc::new(TestChecks {
            store: fixture.store,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
            fence_after,
        });
        let result = intent.register(checks.clone()).await;
        match failure {
            0 => {
                result.unwrap();
                assert_eq!(checks.completed.load(Ordering::SeqCst), 6);
            }
            1 => {
                assert!(matches!(result, Err(CatalogError::Store(StoreError::Response))));
                assert_eq!(checks.completed.load(Ordering::SeqCst), 3);
            }
            2 => assert!(matches!(result, Err(CatalogError::Busy))),
            _ => unreachable!(),
        }
        assert_eq!(checks.peak.load(Ordering::SeqCst), 3);
        assert_eq!(checks.active.load(Ordering::SeqCst), 0);
        assert_eq!(checks.mutations.load(Ordering::SeqCst), 1);
    }
}
