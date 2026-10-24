// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::*, *};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn composition_retry_observes_current_effect_without_published_process_state() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    common::evaluate(&authority, &identity, operation, 1).await;
    let prepared = common::prepare(&authority, &identity, operation, 1).await;
    let snapshot = prepared.preparation.unwrap().candidate;
    let attempt = common::attempt(&authority, &identity, snapshot).await;
    store.lose_data_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .publish_composition(&identity, operation, &attempt)
        .await
        .is_err());
    let restarted = DatasetAuthority::new(store);
    let status = restarted.composition_status(&identity, operation).await.unwrap();
    assert_eq!(
        status.record.progress,
        DraftProgress::Prepared {
            target: attempt.target
        }
    );
    assert_eq!(
        restarted
            .publish_composition(&identity, operation, &attempt)
            .await
            .unwrap()
            .attempt,
        attempt
    );
    restarted
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    assert!(restarted.get_snapshot(&identity, snapshot).await.is_ok());
}

#[tokio::test]
async fn composition_head_conflict_preserves_candidate_for_explicit_new_attempt() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    common::evaluate(&authority, &identity, operation, 1).await;
    let prepared = common::prepare(&authority, &identity, operation, 1).await;
    let snapshot = prepared.preparation.unwrap().candidate;
    let stale = common::attempt(&authority, &identity, snapshot).await;
    let other = authority
        .publish_snapshot(&identity, Some(parent), publication::publication_manifest(2))
        .await
        .unwrap();
    let error = authority
        .publish_composition(&identity, operation, &stale)
        .await
        .unwrap_err();
    let CompositionError::Authority(AuthorityError::SnapshotHeadConflict { observed }) = error else {
        panic!("{error:?}")
    };
    assert_eq!(observed.latest, Some(other));
    assert_eq!(
        authority
            .composition_status(&identity, operation)
            .await
            .unwrap()
            .record
            .progress,
        DraftProgress::Prepared { target: stale.target }
    );
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(other)
    );
    let next = CommitAttempt {
        id: OperationId::random(),
        expected_head: observed.generation,
        ..stale
    };
    authority
        .publish_composition(&identity, operation, &next)
        .await
        .unwrap();
    assert_eq!(
        authority
            .get_snapshot(&identity, snapshot)
            .await
            .unwrap()
            .publication
            .parent,
        Some(parent)
    );
}
