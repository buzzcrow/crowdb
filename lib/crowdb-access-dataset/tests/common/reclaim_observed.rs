use async_trait::async_trait;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Copy)]
pub enum TestSnapshotReply {
    Conflict,
    Missing,
    Unknown,
}

pub struct TestReclaimObserved {
    pub inner: Arc<crate::access_store::TestAccessStore>,
    pub snapshot: Vec<u8>,
    pub head: HeadRecord,
    pub head_conflicts: AtomicUsize,
    pub snapshot_conflicts: AtomicUsize,
    pub reply: TestSnapshotReply,
    head_retry: AtomicBool,
    snapshot_retry: AtomicBool,
}

impl TestReclaimObserved {
    pub fn new(
        inner: Arc<crate::access_store::TestAccessStore>,
        snapshot: Vec<u8>,
        head: HeadRecord,
        reply: TestSnapshotReply,
    ) -> Self {
        Self {
            inner,
            snapshot,
            head,
            head_conflicts: AtomicUsize::new(4),
            snapshot_conflicts: AtomicUsize::new(0),
            reply,
            head_retry: AtomicBool::new(false),
            snapshot_retry: AtomicBool::new(false),
        }
    }
    fn check_read(&self, key: &[u8]) {
        assert!(
            !(key.starts_with(b"DS\0H\0") && self.head_retry.load(Ordering::SeqCst)),
            "head conflict must reuse the observed value"
        );
        assert!(
            !(key == self.snapshot && self.snapshot_retry.load(Ordering::SeqCst)),
            "snapshot mutation must not issue a confirmation GET"
        );
    }
}

#[async_trait]
impl DatasetStore for TestReclaimObserved {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.check_read(key);
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.check_read(key);
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        scan: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.inner.scan_page(scan, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        assert!(
            ![b"DS\0CA\0".as_slice(), b"DS\0CR\0", b"DS\0SC\0"]
                .iter()
                .any(|prefix| key.starts_with(prefix)),
            "reclaim wrote a publication ledger"
        );
        if key.starts_with(b"DS\0H\0")
            && self
                .head_conflicts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            // Another reclaimer advances only the physical head nonce.
            let bytes = bincode::serialize(&(2_u16, &self.head, OperationId::random())).unwrap();
            assert!(matches!(
                self.inner.compare_exchange(key, expected, &bytes).await?,
                CasOutcome::Applied(_)
            ));
            let observed = self.inner.get(key).await?;
            self.head_retry.store(true, Ordering::SeqCst);
            return Ok(CasOutcome::Conflict(observed));
        }
        if key == self.snapshot {
            self.snapshot_conflicts.fetch_add(1, Ordering::SeqCst);
            self.snapshot_retry.store(true, Ordering::SeqCst);
            return match self.reply {
                TestSnapshotReply::Conflict => {
                    assert!(matches!(
                        self.inner.compare_exchange(key, expected, value).await?,
                        CasOutcome::Applied(_)
                    ));
                    Ok(CasOutcome::Conflict(self.inner.get(key).await?))
                }
                TestSnapshotReply::Missing => {
                    self.inner.delete(key).await?;
                    Ok(CasOutcome::Conflict(None))
                }
                TestSnapshotReply::Unknown => Err(StoreError::Rejected),
            };
        }
        let result = self.inner.compare_exchange(key, expected, value).await;
        if key.starts_with(b"DS\0H\0") && matches!(result, Ok(CasOutcome::Applied(_))) {
            self.head_retry.store(false, Ordering::SeqCst);
        }
        result
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}
