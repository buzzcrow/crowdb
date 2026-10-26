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
use std::sync::atomic::Ordering;

fn key(prefix: &[u8], identity: &DatasetIdentity, suffix: &[u8]) -> Vec<u8> {
    [prefix, identity.key().unwrap().as_slice(), suffix].concat()
}
async fn legacy_binding(store: &dyn DatasetStore, identity: &DatasetIdentity, snapshot: SnapshotId) {
    let key = key(b"DS\0CB\0", identity, snapshot.as_bytes());
    let stored = store.get(&key).await.unwrap().unwrap();
    let mut binding: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    binding["version"] = 1.into();
    assert!(matches!(
        store
            .compare_exchange(&key, Some(&stored.bytes), &serde_json::to_vec(&binding).unwrap())
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn stored_legacy_receipts_remain_readable_but_do_not_control_new_publication() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let old = common::attempt(&authority, &identity, snapshot).await;
    let receipt = SuccessfulCommit {
        version: 1,
        attempt: old.clone(),
        committed_head: old.expected_head.unwrap(),
    };
    for (prefix, suffix) in [
        (b"DS\0CR\0".as_slice(), old.id.as_bytes()),
        (b"DS\0SC\0", snapshot.as_bytes()),
    ] {
        store
            .compare_exchange(&key(prefix, &identity, suffix), None, &receipt.encode().unwrap())
            .await
            .unwrap();
    }
    store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
    let writes = store.mutations.load(Ordering::Relaxed);
    assert_eq!(
        authority.commit_status(&identity, old.id).await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        authority.snapshot_commit(&identity, snapshot).await.unwrap(),
        Some(receipt)
    );
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    let next = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(&identity, &common::attempt(&authority, &identity, next).await)
        .await
        .unwrap();
    assert!(matches!(
        authority.publish_snapshot_candidate(&identity, &old).await,
        Err(AuthorityError::SnapshotHeadConflict { .. })
    ));
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(next));
}

#[tokio::test]
async fn a_legacy_head_marker_is_observed_without_backfill_and_its_complete_resource_survives_replacement() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    legacy_binding(store.as_ref(), &identity, snapshot).await;
    let old = CommitAttempt {
        version: 1,
        id: OperationId::random(),
        target: CommitTarget::Snapshot { snapshot },
        expected_head: None,
    };
    let head_key = key(b"DS\0H\0", &identity, &[]);
    let current = store.get(&head_key).await.unwrap().unwrap();
    let bytes = bincode::serialize(&(
        3u16,
        (
            HeadRecord {
                latest: Some(snapshot),
                stable: None,
            },
            OperationId::random(),
            old.clone(),
        ),
    ))
    .unwrap();
    store
        .compare_exchange(&head_key, Some(&current.bytes), &bytes)
        .await
        .unwrap();
    let generation = CommitGeneration::from_head(&bytes);
    store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
    let writes = store.mutations.load(Ordering::Relaxed);
    assert_eq!(
        authority
            .commit_status(&identity, old.id)
            .await
            .unwrap()
            .unwrap()
            .committed_head,
        generation
    );
    assert_eq!(
        authority
            .snapshot_commit(&identity, snapshot)
            .await
            .unwrap()
            .unwrap()
            .attempt,
        old
    );
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    let next = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .update_snapshot_head(
            &identity,
            &SnapshotHeadUpdate {
                snapshot: next,
                expected_head: Some(generation),
            },
        )
        .await
        .unwrap();
    let writes = store.mutations.load(Ordering::Relaxed);
    assert!(authority.get_snapshot(&identity, snapshot).await.is_ok());
    assert!(authority
        .snapshot_inspection_summary(&identity, snapshot, common::budget(), &ReadCancellation::new())
        .await
        .is_ok());
    assert!(authority
        .commit_status(&identity, old.id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    for (prefix, suffix) in [
        (b"DS\0CR\0".as_slice(), old.id.as_bytes()),
        (b"DS\0SC\0", snapshot.as_bytes()),
    ] {
        assert!(store
            .get(&key(prefix, &identity, suffix))
            .await
            .unwrap()
            .is_none());
    }
    authority.set_stable(&identity, snapshot).await.unwrap();
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes + 1);
}

#[tokio::test]
async fn legacy_preparation_and_missing_initial_split_are_not_complete_resources() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let incomplete = authority
        .prepare_snapshot_with_operation(
            &identity,
            Some(parent),
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    legacy_binding(store.as_ref(), &identity, incomplete).await;
    assert!(matches!(
        authority.require_committed_snapshot(&identity, incomplete).await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    let binding_key = key(b"DS\0CB\0", &identity, parent.as_bytes());
    let stored = store.get(&binding_key).await.unwrap().unwrap();
    let mut binding: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    binding["version"] = 1.into();
    binding["initial_split"] =
        serde_json::to_value(SplitRevisionId::from_bytes(OperationId::random().as_bytes()).unwrap()).unwrap();
    store
        .compare_exchange(
            &binding_key,
            Some(&stored.bytes),
            &serde_json::to_vec(&binding).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        authority.require_committed_snapshot(&identity, parent).await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    assert!(matches!(
        authority
            .update_snapshot_head(
                &identity,
                &SnapshotHeadUpdate {
                    snapshot: parent,
                    expected_head: authority.inspect_head(&identity).await.unwrap().generation
                }
            )
            .await,
        Err(AuthorityError::SnapshotNotFound)
    ));
}
