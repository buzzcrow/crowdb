// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/legacy_drafts.rs"]
#[allow(dead_code)]
mod legacy;
#[path = "common/legacy_split.rs"]
mod legacy_split;
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
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
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
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
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
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
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
    legacy::composition_edit(store.as_ref(), &identity, operation, vec![], true).await;
    finish(&authority, &identity, operation).await;
    assert!(authority.get_manifest(&identity, snapshot).await.is_ok());
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(snapshot)
    );
}

#[tokio::test]
async fn superseding_partial_initial_split_preparation_retires_the_old_candidate() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let parent = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let operation = OperationId::random();
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
    let old = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            bincode::serialize(&authority.get_manifest(&identity, parent).await.unwrap()).unwrap(),
            candidate(operation, 1),
        )
        .await
        .unwrap();
    let split_operation = candidate(candidate(operation, 1), 1);
    let revision = legacy_split::revision(split_operation, 1);
    let cb = [b"DS\0CB\0".as_slice(), &identity.key().unwrap(), old.as_bytes()].concat();
    let stored = store.get(&cb).await.unwrap().unwrap();
    let mut binding: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    binding["version"] = 3.into();
    binding["initial_split"] = serde_json::json!(revision);
    legacy::replace(store.as_ref(), &cb, &stored.bytes, &binding).await;
    let splits = split_store::partition_splits();
    let record = split::SplitDraftRecord {
        version: 1,
        operation: split_operation,
        snapshot: old,
        expected_revision: None,
        revision: 1,
        policy: split::SplitPolicy::default(),
        splits: splits.clone(),
        progress: composition::DraftProgress::Editing,
    };
    legacy_split::state(store.as_ref(), &identity, &record).await;
    let sd = legacy::key(b"DS\0SD\0", &identity, split_operation);
    let stored = store.get(&sd).await.unwrap().unwrap();
    let mut draft: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    draft["initial"] = true.into();
    legacy::replace(store.as_ref(), &sd, &stored.bytes, &draft).await;
    let membership = split::MembershipPage {
        version: 1,
        snapshot: old,
        revision,
        name: splits[0].name.clone(),
        page: 0,
        members: vec![b"sample-1".to_vec()],
    };
    let member_key = split::key::membership_page(&identity, old, revision, &membership.name, 0).unwrap();
    let bytes = membership.encode().unwrap();
    legacy::install(store.as_ref(), &member_key, &bytes).await;
    let mut sw = legacy::key(b"DS\0SW\0", &identity, split_operation);
    sw.extend(1u64.to_be_bytes());
    sw.extend(0u64.to_be_bytes());
    legacy::install(
        store.as_ref(),
        &sw,
        &serde_json::to_vec(&serde_json::json!({
            "version": 1, "pages": [{"key": member_key, "digest": <[u8;32]>::from(Sha256::digest(&bytes))}],
        }))
        .unwrap(),
    )
    .await;
    assert!(authority.get_snapshot(&identity, old).await.is_err());
    let attempt = common::attempt(&authority, &identity, old).await;
    legacy::composition_edit(store.as_ref(), &identity, operation, vec![], true).await;
    finish(&authority, &identity, operation).await;
    assert!(authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .is_err());
    assert!(matches!(
        store.compare_exchange(&member_key, None, &bytes).await.unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert!(matches!(
        store
            .compare_exchange(&member_key, Some(&bytes), &bytes)
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    let result = authority
        .prepare_composition_resource(
            &identity,
            SnapshotId::random(),
            &composition::CompositionRequest {
                splits,
                ..common::request(parent)
            },
            None,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.sample_count, 45);
    assert!(result.initial_split.is_some());
}
