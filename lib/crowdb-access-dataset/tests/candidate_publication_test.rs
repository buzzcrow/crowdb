// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::*;
use publication::publication_manifest;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

struct TestStore {
    inner: dataset_store::TestDatasetStore,
    lose_head_reply: AtomicBool,
    reject_receipts: AtomicBool,
    race_head: AtomicBool,
}
#[async_trait::async_trait]
impl DatasetStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.inner.scan_page(request, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        if key.starts_with(b"DS\0CR\0")
            && self.reject_receipts.load(Ordering::Relaxed)
            && self.inner.get(key).await?.is_none()
        {
            return Err(StoreError::Rejected);
        }
        if key.starts_with(b"DS\0H\0") && self.race_head.swap(false, Ordering::Relaxed) {
            let competing = bincode::serialize(&(
                1u16,
                HeadRecord {
                    latest: None,
                    stable: None,
                },
            ))
            .unwrap();
            self.inner.compare_exchange(key, expected, &competing).await?;
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if key.starts_with(b"DS\0H\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_head_reply.swap(false, Ordering::Relaxed)
        {
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}
async fn fixture() -> (Arc<TestStore>, DatasetAuthority, DatasetIdentity) {
    let store = Arc::new(TestStore {
        inner: dataset_store::TestDatasetStore::default(),
        lose_head_reply: false.into(),
        reject_receipts: false.into(),
        race_head: false.into(),
    });
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "candidates").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    (store, authority, identity)
}
async fn prepare(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    parent: Option<SnapshotId>,
    marker: u8,
) -> SnapshotId {
    authority
        .prepare_snapshot_candidate(
            identity,
            parent,
            publication_manifest(marker),
            OperationId::random(),
        )
        .await
        .unwrap()
}
async fn attempt(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) -> CommitAttempt {
    CommitAttempt {
        version: 1,
        id: OperationId::random(),
        target: CommitTarget::Snapshot { snapshot },
        expected_head: authority.inspect_head(identity).await.unwrap().generation,
    }
}
#[tokio::test]
async fn historical_parent_and_new_roots_use_independent_head_preconditions() {
    let (_, authority, identity) = fixture().await;
    let root = prepare(&authority, &identity, None, 1).await;
    authority
        .publish_snapshot_candidate(&identity, &attempt(&authority, &identity, root).await)
        .await
        .unwrap();
    let first = prepare(&authority, &identity, Some(root), 2).await;
    let second = prepare(&authority, &identity, Some(root), 3).await;
    let first_attempt = attempt(&authority, &identity, first).await;
    let stale = attempt(&authority, &identity, second).await;
    authority
        .publish_snapshot_candidate(&identity, &first_attempt)
        .await
        .unwrap();
    assert!(matches!(
        authority.publish_snapshot_candidate(&identity, &stale).await,
        Err(AuthorityError::HeadConflict)
    ));
    assert!(authority
        .snapshot_commit(&identity, second)
        .await
        .unwrap()
        .is_none());
    let retry = attempt(&authority, &identity, second).await;
    authority
        .publish_snapshot_candidate(&identity, &retry)
        .await
        .unwrap();
    assert_eq!(
        authority
            .get_snapshot(&identity, second)
            .await
            .unwrap()
            .publication
            .parent,
        Some(root)
    );
    let new_root = prepare(&authority, &identity, None, 4).await;
    authority
        .publish_snapshot_candidate(&identity, &attempt(&authority, &identity, new_root).await)
        .await
        .unwrap();
    let head = authority.inspect_head(&identity).await.unwrap();
    assert_eq!(
        authority
            .publish_snapshot_candidate(&identity, &first_attempt)
            .await
            .unwrap()
            .attempt,
        first_attempt
    );
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), head);
    assert_eq!(head.latest, Some(new_root));
    assert!(authority
        .commit_status(&identity, stale.id)
        .await
        .unwrap()
        .is_none());
}
#[tokio::test]
async fn lost_head_reply_recovers_receipt_without_republishing() {
    let (store, authority, identity) = fixture().await;
    let snapshot = prepare(&authority, &identity, None, 5).await;
    let attempt = attempt(&authority, &identity, snapshot).await;
    store.lose_head_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .is_err());
    let committed_head = authority.inspect_head(&identity).await.unwrap();
    assert_eq!(committed_head.latest, Some(snapshot));
    let restarted = DatasetAuthority::new(store);
    let receipt = restarted
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
    assert_eq!(receipt.committed_head, committed_head.generation.unwrap());
    assert_eq!(restarted.inspect_head(&identity).await.unwrap(), committed_head);
    assert_eq!(
        restarted.commit_status(&identity, attempt.id).await.unwrap(),
        Some(receipt)
    );
}
#[tokio::test]
async fn head_mutators_help_pending_success_before_replacing_its_marker() {
    let (store, authority, identity) = fixture().await;
    let snapshot = prepare(&authority, &identity, None, 6).await;
    let committed = attempt(&authority, &identity, snapshot).await;
    store.reject_receipts.store(true, Ordering::Relaxed);
    assert!(authority
        .publish_snapshot_candidate(&identity, &committed)
        .await
        .is_err());
    let head = authority.inspect_head(&identity).await.unwrap();
    assert!(authority.set_stable(&identity, snapshot).await.is_err());
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), head);
    store.reject_receipts.store(false, Ordering::Relaxed);
    authority.set_stable(&identity, snapshot).await.unwrap();
    assert!(authority
        .commit_status(&identity, committed.id)
        .await
        .unwrap()
        .is_some());
    let changed = authority.inspect_head(&identity).await.unwrap();
    assert_ne!(changed.generation, head.generation);
    assert_eq!(changed.stable, Some(snapshot));
    authority
        .publish_snapshot_candidate(&identity, &committed)
        .await
        .unwrap();
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), changed);
}
#[tokio::test]
async fn uncommitted_parent_and_divergent_attempt_cannot_publish() {
    let (_, authority, identity) = fixture().await;
    let parent = prepare(&authority, &identity, None, 7).await;
    assert!(matches!(
        authority
            .prepare_snapshot_candidate(
                &identity,
                Some(parent),
                publication_manifest(8),
                OperationId::random()
            )
            .await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    let committed = attempt(&authority, &identity, parent).await;
    authority
        .publish_snapshot_candidate(&identity, &committed)
        .await
        .unwrap();
    let child = prepare(&authority, &identity, Some(parent), 9).await;
    let divergent = CommitAttempt {
        target: CommitTarget::Snapshot { snapshot: child },
        ..committed
    };
    assert!(matches!(
        authority.publish_snapshot_candidate(&identity, &divergent).await,
        Err(AuthorityError::AlreadyExists)
    ));
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(parent));
}

#[tokio::test]
async fn lost_cas_candidate_is_not_a_committed_parent_or_stable_target() {
    let (store, authority, identity) = fixture().await;
    let snapshot = prepare(&authority, &identity, None, 10).await;
    let stale = attempt(&authority, &identity, snapshot).await;
    store.race_head.store(true, Ordering::Relaxed);
    assert!(matches!(
        authority.publish_snapshot_candidate(&identity, &stale).await,
        Err(AuthorityError::HeadConflict)
    ));
    assert_eq!(
        authority
            .get_snapshot(&identity, snapshot)
            .await
            .unwrap()
            .publication
            .state,
        PublicationState::Published
    );
    assert!(authority
        .snapshot_commit(&identity, snapshot)
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        authority.set_stable(&identity, snapshot).await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    assert!(matches!(
        authority
            .prepare_snapshot_candidate(
                &identity,
                Some(snapshot),
                publication_manifest(11),
                OperationId::random()
            )
            .await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    let retry = attempt(&authority, &identity, snapshot).await;
    authority
        .publish_snapshot_candidate(&identity, &retry)
        .await
        .unwrap();
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(snapshot));
    assert!(authority
        .commit_status(&identity, stale.id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn reclaim_head_fence_preserves_pending_commit_evidence() {
    let (store, authority, identity) = fixture().await;
    let old = prepare(&authority, &identity, None, 12).await;
    authority
        .publish_snapshot_candidate(&identity, &attempt(&authority, &identity, old).await)
        .await
        .unwrap();
    let latest = prepare(&authority, &identity, None, 13).await;
    let publication = attempt(&authority, &identity, latest).await;
    store.reject_receipts.store(true, Ordering::Relaxed);
    assert!(authority
        .publish_snapshot_candidate(&identity, &publication)
        .await
        .is_err());
    let head = authority.inspect_head(&identity).await.unwrap();
    assert!(authority.begin_reclaim(&identity, old).await.is_err());
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), head);
    store.reject_receipts.store(false, Ordering::Relaxed);
    authority.begin_reclaim(&identity, old).await.unwrap();
    let receipt = authority
        .commit_status(&identity, publication.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.committed_head, head.generation.unwrap());
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(latest));
}
