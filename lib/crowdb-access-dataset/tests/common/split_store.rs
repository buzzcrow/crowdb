// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(super) struct TestDraftStore {
    inner: dataset_store::TestDatasetStore,
    pub lose_edit_reply: AtomicBool,
    pub lose_data_reply: AtomicBool,
    pub reject_cleanup_finish: AtomicBool,
    pub lose_split_reply: AtomicBool,
    pub reject_receipts: AtomicBool,
    pub inspect_only: AtomicBool,
    pub metadata_only: AtomicBool,
    pub cancel_scan: AtomicBool,
}
#[async_trait::async_trait]
impl DatasetStore for TestDraftStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(
            !self.inspect_only.load(Ordering::Relaxed),
            "Split inspection used unbounded get"
        );
        if self.metadata_only.load(Ordering::Relaxed) {
            assert!(
                !key.starts_with(b"DS\0S\0") && !key.starts_with(b"DS\0M\0"),
                "metadata planning loaded Manifest"
            );
        }
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        if self.inspect_only.load(Ordering::Relaxed) || self.metadata_only.load(Ordering::Relaxed) {
            assert!(
                !key.starts_with(b"DS\0S\0") && !key.starts_with(b"DS\0M\0"),
                "Split inspection loaded data Manifest"
            );
        }
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        if self.cancel_scan.swap(false, Ordering::Relaxed) {
            cancel.cancel();
        }
        self.inner.scan_page(request, cancel).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        assert!(
            !self.inspect_only.load(Ordering::Relaxed),
            "Split inspection wrote metadata"
        );
        if key.starts_with(b"DS\0CR\0")
            && self.reject_receipts.load(Ordering::Relaxed)
            && self.inner.get(key).await?.is_none()
        {
            return Err(StoreError::Rejected);
        }
        if key.starts_with(b"DS\0G\0") && self.reject_cleanup_finish.swap(false, Ordering::Relaxed) {
            return Err(StoreError::Rejected);
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if key.starts_with(b"DS\0H\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_data_reply.swap(false, Ordering::Relaxed)
        {
            return Err(StoreError::Rejected);
        }
        if key.starts_with(b"DS\0SH\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_split_reply.swap(false, Ordering::Relaxed)
        {
            return Err(StoreError::Rejected);
        }
        if (key.starts_with(b"DS\0SD\0") || key.starts_with(b"DS\0CD\0"))
            && expected.is_some()
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_edit_reply.swap(false, Ordering::Relaxed)
        {
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }
}

pub(super) async fn fixture() -> (
    std::sync::Arc<TestDraftStore>,
    DatasetAuthority,
    DatasetIdentity,
    SnapshotId,
) {
    let store = std::sync::Arc::new(TestDraftStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "drafts").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, crate::publication::publication_manifest(1))
        .await
        .unwrap();
    (store, authority, identity, snapshot)
}

#[allow(dead_code)]
pub(super) fn partition_splits() -> Vec<crowdb_access_dataset::split::DraftSplit> {
    use crowdb_access_dataset::split::*;
    let weights: Vec<_> = ["train", "test"]
        .into_iter()
        .map(|name| PartitionWeight {
            name: SplitName::new(name).unwrap(),
            weight: 1,
        })
        .collect();
    let splits = weights
        .iter()
        .enumerate()
        .map(|(index, weight)| DraftSplit {
            name: weight.name.clone(),
            definition: SplitDefinition::WeightedPartition {
                seed: 42,
                weights: weights.clone(),
                index: u32::try_from(index).unwrap(),
            },
            origin: MembershipOrigin::Evaluated,
        })
        .collect();
    splits
}

#[allow(dead_code)]
pub(super) async fn prepared(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    expected: Option<SplitRevisionId>,
) -> crowdb_access_dataset::split::SplitRevisionRecord {
    let operation = OperationId::random();
    authority
        .begin_split_draft(
            identity,
            operation,
            snapshot,
            expected,
            crowdb_access_dataset::split::SplitPolicy::default(),
            partition_splits(),
        )
        .await
        .unwrap();
    let mut page = 0;
    loop {
        let preview = authority
            .evaluate_split_draft(identity, operation, 1, page, &ReadCancellation::new())
            .await
            .unwrap();
        if preview.complete {
            break;
        }
        page = preview.next_page;
    }
    authority
        .prepare_split_draft(identity, operation, 1)
        .await
        .unwrap()
}
#[allow(dead_code)]
pub(super) async fn attempt(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    record: &crowdb_access_dataset::split::SplitRevisionRecord,
) -> CommitAttempt {
    CommitAttempt {
        version: 1,
        id: OperationId::random(),
        target: CommitTarget::SplitRevision {
            snapshot: record.snapshot,
            revision: record.revision,
        },
        expected_head: authority
            .inspect_split_head(identity, record.snapshot)
            .await
            .unwrap()
            .generation,
    }
}

#[allow(dead_code)]
pub(super) async fn publish_binary_samples(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    parent: SnapshotId,
) -> SnapshotId {
    let mut manifest: ManifestRecord =
        bincode::deserialize(&crate::publication::publication_manifest(1)).unwrap();
    manifest.samples = (0..45u8)
        .map(|id| SampleRecord {
            sample_id: vec![id, 0, 255],
            fields: manifest.samples[0].fields.clone(),
        })
        .collect();
    let snapshot = authority
        .publish_snapshot(identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    snapshot
}

#[allow(dead_code)]
pub(super) async fn publish_revision(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    record: &crowdb_access_dataset::split::SplitRevisionRecord,
) -> SuccessfulCommit {
    authority
        .publish_split_draft(
            identity,
            record.operation,
            &attempt(authority, identity, record).await,
        )
        .await
        .unwrap()
}
