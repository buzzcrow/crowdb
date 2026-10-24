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

#[tokio::test]
async fn abandoned_complete_composition_preserves_raw_candidate_and_initial_split() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    common::begin_evaluated(
        &authority,
        &identity,
        operation,
        &CompositionInput {
            splits: split_store::partition_splits(),
            ..common::input(parent)
        },
    )
    .await;
    let prepared = common::prepare(&authority, &identity, operation, 1).await;
    let preparation = prepared.preparation.unwrap();
    let attempt = common::attempt(&authority, &identity, preparation.candidate).await;
    assert_eq!(
        authority
            .abandon_composition(&identity, operation, 1)
            .await
            .unwrap()
            .record
            .progress,
        DraftProgress::Abandoned
    );
    assert!(authority
        .prepare_split_draft(&identity, preparation.split_operation.unwrap(), 1)
        .await
        .is_ok());
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(parent)
    );
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    let split_operation = preparation.split_operation.unwrap();
    let mut split_cleaned = false;
    for _ in 0..100 {
        if authority
            .cleanup_split_scratch_step(&identity, split_operation, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            split_cleaned = true;
            break;
        }
    }
    assert!(split_cleaned);
    authority
        .get_snapshot(&identity, preparation.candidate)
        .await
        .unwrap();
    assert!(authority
        .inspect_split_head(&identity, preparation.candidate)
        .await
        .unwrap()
        .revision
        .is_some());
    authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
}

#[tokio::test]
async fn abandon_recognizes_a_successful_historical_candidate_without_deleting_it() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    common::begin_evaluated(&authority, &identity, operation, &common::input(parent)).await;
    let prepared = common::prepare(&authority, &identity, operation, 1).await;
    let snapshot = prepared.preparation.unwrap().candidate;
    let attempt = common::attempt(&authority, &identity, snapshot).await;
    authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
    let root = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(&identity, &common::attempt(&authority, &identity, root).await)
        .await
        .unwrap();
    assert!(matches!(
        authority.abandon_composition(&identity, operation, 1).await,
        Err(CompositionError::Frozen)
    ));
    assert_eq!(
        authority
            .composition_status(&identity, operation)
            .await
            .unwrap()
            .record
            .progress,
        DraftProgress::Published {
            target: attempt.target,
            attempt: attempt.id
        }
    );
    assert!(authority
        .snapshot_inspection_summary(&identity, snapshot, common::budget(), &ReadCancellation::new())
        .await
        .is_ok());
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(root)
    );
}
