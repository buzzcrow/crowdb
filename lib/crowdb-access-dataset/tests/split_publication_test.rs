// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::DraftProgress, split::*, *};
use split_store::{attempt, fixture, prepared};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn independent_revision_cas_and_historical_retries_never_move_data_head() {
    let (_, authority, identity, snapshot) = fixture().await;
    let data_head = authority.inspect_head(&identity).await.unwrap();
    let first = prepared(&authority, &identity, snapshot, None).await;
    let competing = prepared(&authority, &identity, snapshot, None).await;
    let first_attempt = attempt(&authority, &identity, &first).await;
    let stale = attempt(&authority, &identity, &competing).await;
    let receipt = authority
        .publish_split_draft(&identity, first.operation, &first_attempt)
        .await
        .unwrap();
    assert!(matches!(
        authority
            .publish_split_draft(&identity, competing.operation, &stale)
            .await,
        Err(SplitLifecycleError::Conflict)
    ));
    assert!(matches!(
        authority
            .split_draft_status(&identity, competing.operation)
            .await
            .unwrap()
            .progress,
        DraftProgress::Prepared { .. }
    ));
    assert!(authority
        .commit_status(&identity, stale.id)
        .await
        .unwrap()
        .is_none());
    let next = prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    let next_attempt = attempt(&authority, &identity, &next).await;
    authority
        .publish_split_draft(&identity, next.operation, &next_attempt)
        .await
        .unwrap();
    let current = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    assert_eq!(current.revision, Some(next.revision));
    assert_eq!(
        authority
            .publish_split_draft(&identity, first.operation, &first_attempt)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        authority.inspect_split_head(&identity, snapshot).await.unwrap(),
        current
    );
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), data_head);
}

#[tokio::test]
async fn lost_split_cas_reply_is_recoverable_and_pending_commit_cannot_be_abandoned() {
    let (store, authority, identity, snapshot) = fixture().await;
    let record = prepared(&authority, &identity, snapshot, None).await;
    let attempt = attempt(&authority, &identity, &record).await;
    store.lose_split_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .publish_split_draft(&identity, record.operation, &attempt)
        .await
        .is_err());
    store.inspect_only.store(true, Ordering::Relaxed);
    store.reject_split_proof_reads.store(true, Ordering::Relaxed);
    assert_eq!(
        authority
            .get_split_revision(
                &identity,
                snapshot,
                record.revision,
                MetadataScanBudget {
                    items: 1,
                    bytes: 1024 * 1024,
                    timeout: std::time::Duration::from_secs(2)
                },
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        record
    );
    store.inspect_only.store(false, Ordering::Relaxed);
    store.reject_split_proof_reads.store(false, Ordering::Relaxed);
    assert!(matches!(
        authority
            .abandon_split_draft(&identity, record.operation, 1)
            .await,
        Err(SplitLifecycleError::Frozen)
    ));
    let head = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    let restarted = DatasetAuthority::new(store);
    let status = restarted
        .commit_status(&identity, attempt.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.committed_head, head.generation.unwrap());
    assert_eq!(
        restarted
            .publish_split_draft(&identity, record.operation, &attempt)
            .await
            .unwrap(),
        status
    );
    assert_eq!(
        restarted.inspect_split_head(&identity, snapshot).await.unwrap(),
        head
    );
    assert!(matches!(
        restarted
            .split_draft_status(&identity, record.operation)
            .await
            .unwrap()
            .progress,
        DraftProgress::Published { .. }
    ));
}

#[tokio::test]
async fn receipt_failure_blocks_replacement_until_pending_success_is_helped() {
    let (store, authority, identity, snapshot) = fixture().await;
    let first = prepared(&authority, &identity, snapshot, None).await;
    let first_attempt = attempt(&authority, &identity, &first).await;
    store.reject_receipts.store(true, Ordering::Relaxed);
    assert!(authority
        .publish_split_draft(&identity, first.operation, &first_attempt)
        .await
        .is_err());
    let old = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    let next = prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    let next_attempt = attempt(&authority, &identity, &next).await;
    assert!(authority
        .publish_split_draft(&identity, next.operation, &next_attempt)
        .await
        .is_err());
    assert_eq!(
        authority.inspect_split_head(&identity, snapshot).await.unwrap(),
        old
    );
    store.reject_receipts.store(false, Ordering::Relaxed);
    authority
        .publish_split_draft(&identity, next.operation, &next_attempt)
        .await
        .unwrap();
    assert_eq!(
        authority
            .commit_status(&identity, first_attempt.id)
            .await
            .unwrap()
            .unwrap()
            .committed_head,
        old.generation.unwrap()
    );
    authority
        .publish_split_draft(&identity, first.operation, &first_attempt)
        .await
        .unwrap();
    assert_eq!(
        authority
            .inspect_split_head(&identity, snapshot)
            .await
            .unwrap()
            .revision,
        Some(next.revision)
    );
}
