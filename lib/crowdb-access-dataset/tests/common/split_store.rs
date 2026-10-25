// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
    pub revision_reads: AtomicUsize,
    pub bounded_reads: AtomicUsize,
    pub unbounded_reads: AtomicUsize,
    pub metadata_scans: AtomicUsize,
    pub mutations: AtomicUsize,
    pub split_head_reads: AtomicUsize,
    pub candidate_reads: AtomicUsize,
    pub data_head_reads: AtomicUsize,
    pub data_head_mutations: AtomicUsize,
    pub data_head_conflicts: AtomicUsize,
    pub keep_data_conflict_target: AtomicBool,
    pub split_head_mutations: AtomicUsize,
    pub split_head_conflicts: AtomicUsize,
    pub clear_split_conflict_target: AtomicBool,
    pub reject_publishing_ledgers: AtomicBool,
    pub hide_revision_once: AtomicBool,
    pub lose_revision_reply: AtomicBool,
    pub lose_member_reply: AtomicBool,
    pub reject_split_proof_reads: AtomicBool,
    pub reject_draft_writes: AtomicBool,
    pub reject_workflow_writes: AtomicBool,
    pub lose_complete_reply: AtomicBool,
    pub lose_claim_reply: AtomicBool,
    pub reject_after_unknown: AtomicBool,
}
impl TestDraftStore {
    fn check_unknown_reply(&self, key: &[u8], value: &[u8], result: &CasOutcome) -> Result<(), StoreError> {
        if key.starts_with(b"DS\0CB\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_complete_reply.load(Ordering::Relaxed)
            && serde_json::from_slice::<serde_json::Value>(value).is_ok_and(|v| v["version"] == 2)
        {
            self.lose_complete_reply.store(false, Ordering::Relaxed);
            self.reject_after_unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        if key.starts_with(b"DS\0U\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_claim_reply.load(Ordering::Relaxed)
            && UploadRecord::decode_stored_for_tests(value)
                .is_ok_and(|record| record.state == UploadState::Claimed)
        {
            self.lose_claim_reply.store(false, Ordering::Relaxed);
            self.reject_after_unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        Ok(())
    }
    fn check_result_write(&self, key: &[u8]) {
        assert!(
            !self.reject_after_unknown.load(Ordering::Relaxed),
            "mutation after unknown completion"
        );
        if self.reject_workflow_writes.load(Ordering::Relaxed) {
            assert!(
                ![
                    b"DS\0CBG\0".as_slice(),
                    b"DS\0CD\0",
                    b"DS\0CE\0",
                    b"DS\0CX\0",
                    b"DS\0CW\0",
                    b"DS\0CM\0",
                    b"DS\0SB\0",
                    b"DS\0SD\0",
                    b"DS\0SX\0",
                    b"DS\0SW\0",
                    b"DS\0O\0",
                    b"DS\0CA\0",
                    b"DS\0CR\0",
                    b"DS\0SC\0",
                    b"DS\0SRC\0"
                ]
                .iter()
                .any(|prefix| key.starts_with(prefix)),
                "stateless composition wrote process metadata"
            );
        }
        if self.reject_draft_writes.load(Ordering::Relaxed) {
            assert!(
                [b"DS\0SR\0".as_slice(), b"DS\0SRI\0", b"DS\0SM\0"]
                    .iter()
                    .any(|prefix| key.starts_with(prefix)),
                "stateless preparation wrote workflow state"
            );
        }
    }
}
#[async_trait::async_trait]
impl DatasetStore for TestDraftStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(
            !self.reject_after_unknown.load(Ordering::Relaxed),
            "GET after unknown completion"
        );
        self.unbounded_reads.fetch_add(1, Ordering::Relaxed);
        if key.starts_with(b"DS\0H\0") {
            self.data_head_reads.fetch_add(1, Ordering::Relaxed);
        }
        if key.starts_with(b"DS\0CB\0") {
            self.candidate_reads.fetch_add(1, Ordering::Relaxed);
        }
        if key.starts_with(b"DS\0SH\0") {
            self.split_head_reads.fetch_add(1, Ordering::Relaxed);
        }
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
        assert!(
            !self.reject_after_unknown.load(Ordering::Relaxed),
            "bounded GET after unknown completion"
        );
        self.bounded_reads.fetch_add(1, Ordering::Relaxed);
        if key.starts_with(b"DS\0H\0") {
            self.data_head_reads.fetch_add(1, Ordering::Relaxed);
        }
        if key.starts_with(b"DS\0SH\0") {
            self.split_head_reads.fetch_add(1, Ordering::Relaxed);
        }
        if self.reject_split_proof_reads.load(Ordering::Relaxed) {
            assert!(
                !key.starts_with(b"DS\0SRC\0") && !key.starts_with(b"DS\0SH\0"),
                "explicit Split reads queried publication evidence"
            );
        }
        if key.starts_with(b"DS\0SR\0") {
            self.revision_reads.fetch_add(1, Ordering::Relaxed);
            if self.hide_revision_once.swap(false, Ordering::Relaxed) {
                return Ok(None);
            }
        }
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
        self.metadata_scans.fetch_add(1, Ordering::Relaxed);
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
        self.mutations.fetch_add(1, Ordering::Relaxed);
        self.check_result_write(key);
        if self.reject_publishing_ledgers.load(Ordering::Relaxed) {
            assert!(
                ![b"DS\0CA\0".as_slice(), b"DS\0CR\0", b"DS\0SC\0", b"DS\0SRC\0"]
                    .iter()
                    .any(|prefix| key.starts_with(prefix)),
                "Split publication wrote a request ledger"
            );
        }
        if key.starts_with(b"DS\0H\0") {
            self.data_head_mutations.fetch_add(1, Ordering::Relaxed);
            if self
                .data_head_conflicts
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                let (version, (mut head, _)): (u16, (HeadRecord, OperationId)) =
                    bincode::deserialize(value).unwrap();
                if !self.keep_data_conflict_target.load(Ordering::Relaxed) {
                    head.latest = None;
                }
                let competing = bincode::serialize(&(version, (head, OperationId::random()))).unwrap();
                assert!(matches!(
                    self.inner.compare_exchange(key, expected, &competing).await?,
                    CasOutcome::Applied(_)
                ));
            }
        }
        if key.starts_with(b"DS\0SH\0") {
            self.split_head_mutations.fetch_add(1, Ordering::Relaxed);
            if self
                .split_head_conflicts
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                let mut head = split::SplitHead::decode(value).unwrap();
                head.fence = OperationId::random();
                if self.clear_split_conflict_target.load(Ordering::Relaxed) {
                    head.revision = None;
                }
                assert!(matches!(
                    self.inner
                        .compare_exchange(key, expected, &head.encode().unwrap())
                        .await?,
                    CasOutcome::Applied(_)
                ));
            }
        }
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
        self.check_unknown_reply(key, value, &result)?;
        if key.starts_with(b"DS\0SR\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_revision_reply.swap(false, Ordering::Relaxed)
        {
            return Err(StoreError::Rejected);
        }
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
        if key.starts_with(b"DS\0SM\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_member_reply.swap(false, Ordering::Relaxed)
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
    let _ = expected;
    authority
        .prepare_split_revision(
            identity,
            snapshot,
            &crowdb_access_dataset::split::SplitPreparation {
                revision: SplitRevisionId::random(),
                policy: crowdb_access_dataset::split::SplitPolicy::default(),
                splits: partition_splits(),
            },
            &ReadCancellation::new(),
        )
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
    let current = authority
        .inspect_split_head(identity, record.snapshot)
        .await
        .unwrap();
    let head = authority
        .update_split_head(
            identity,
            record.snapshot,
            &crowdb_access_dataset::split::SplitHeadUpdate {
                revision: record.revision,
                expected_revision: current.revision,
                expected_head: current.generation,
            },
        )
        .await
        .unwrap();
    SuccessfulCommit {
        version: 1,
        attempt: CommitAttempt {
            version: 1,
            id: OperationId::random(),
            target: CommitTarget::SplitRevision {
                snapshot: record.snapshot,
                revision: record.revision,
            },
            expected_head: current.generation,
        },
        committed_head: head.generation.unwrap(),
    }
}

#[allow(dead_code)]
pub(super) fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: std::time::Duration::from_secs(2),
    }
}

#[allow(dead_code)]
pub(super) async fn publish_snapshot_head(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) {
    let head = authority.inspect_head(identity).await.unwrap();
    authority
        .update_snapshot_head(
            identity,
            &SnapshotHeadUpdate {
                snapshot,
                expected_head: head.generation,
            },
        )
        .await
        .unwrap();
}
