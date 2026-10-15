// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::*;
use sha2::{Digest, Sha256};
fn candidate(operation: OperationId, revision: u64) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-composition-candidate-v1\0");
    hash.update(operation.as_bytes());
    hash.update(revision.to_be_bytes());
    OperationId::from_bytes(&hash.finalize()[..16]).unwrap()
}
async fn finish(authority: &DatasetAuthority, identity: &DatasetIdentity, operation: OperationId) {
    for _ in 0..100 {
        if authority
            .cleanup_composition_candidates_step(identity, operation, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("candidate cleanup did not finish");
}
#[tokio::test]
async fn abandoned_draft_fences_preparation_that_has_not_started() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    finish(&authority, &identity, operation).await;
    assert!(authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            publication::publication_manifest(2),
            candidate(operation, 1)
        )
        .await
        .is_err());
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(parent)
    );
}
#[tokio::test]
async fn cleanup_finds_prepared_candidate_without_a_draft_preparation_pointer() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    let snapshot = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            publication::publication_manifest(2),
            candidate(operation, 1),
        )
        .await
        .unwrap();
    // Simulate interruption after the Snapshot record but before its binding/index writes.
    let mut binding = b"DS\0CB\0".to_vec();
    binding.extend(identity.key().unwrap());
    binding.extend(snapshot.as_bytes());
    store.delete(&binding).await.unwrap();
    let mut locator = b"DS\0I\0".to_vec();
    locator.extend(identity.key().unwrap());
    locator.extend(snapshot.as_bytes());
    locator.extend(b"Vsample-2\0\0marker\0\0");
    let delayed = store.get(&locator).await.unwrap().unwrap();
    store.delete(&locator).await.unwrap();
    let attempt = common::attempt(&authority, &identity, snapshot).await;
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    finish(&authority, &identity, operation).await;
    authority
        .reclaim_snapshot_metadata(&identity, snapshot)
        .await
        .unwrap();
    assert!(matches!(
        store
            .compare_exchange(&locator, None, &delayed.bytes)
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert!(authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .is_err());
    assert!(authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            publication::publication_manifest(2),
            candidate(operation, 1)
        )
        .await
        .is_err());
    finish(&DatasetAuthority::new(store), &identity, operation).await;
}
#[tokio::test]
async fn obsolete_candidate_cleanup_preserves_a_successful_historical_publication() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    let snapshot = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            publication::publication_manifest(2),
            candidate(operation, 1),
        )
        .await
        .unwrap();
    let attempt = common::attempt(&authority, &identity, snapshot).await;
    authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
    authority
        .edit_composition(&identity, operation, &common::edit(1, vec![]))
        .await
        .unwrap();
    finish(&authority, &identity, operation).await;
    assert!(authority.get_manifest(&identity, snapshot).await.is_ok());
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(snapshot)
    );
}

#[tokio::test]
async fn superseding_partial_initial_split_preparation_retires_the_old_candidate() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let parent = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let operation = OperationId::random();
    let input = crowdb_access_dataset::composition::CompositionInput {
        splits: split_store::partition_splits(),
        ..common::input(parent)
    };
    authority
        .begin_composition(&identity, operation, &input)
        .await
        .unwrap();
    common::evaluate(&authority, &identity, operation, 1).await;
    let draft = authority
        .prepare_composition(&identity, operation, 1, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(matches!(
        draft.record.progress,
        crowdb_access_dataset::composition::DraftProgress::Evaluated { .. }
    ));
    let old = draft.preparation.unwrap().candidate;
    let attempt = common::attempt(&authority, &identity, old).await;
    authority
        .edit_composition(&identity, operation, &common::edit(1, vec![]))
        .await
        .unwrap();
    finish(&authority, &identity, operation).await;
    assert!(authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .is_err());
    assert_eq!(
        common::evaluate(&authority, &identity, operation, 2)
            .await
            .members,
        45
    );
    assert!(common::prepare(&authority, &identity, operation, 2)
        .await
        .preparation
        .is_some());
}
