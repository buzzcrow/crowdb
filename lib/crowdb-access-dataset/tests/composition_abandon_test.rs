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
use crowdb_access_dataset::{composition::*, split::SplitReclaimCursor, *};

#[tokio::test]
async fn abandoned_complete_composition_preserves_raw_candidate_and_initial_split() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
    let snapshot = SnapshotId::random();
    let input = CompositionRequest {
        splits: split_store::partition_splits(),
        ..common::request(parent)
    };
    let result = authority
        .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
        .await
        .unwrap();
    let revision = authority
        .get_split_revision(
            &identity,
            snapshot,
            result.initial_split.unwrap(),
            common::budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let split_operation = OperationId::from_bytes(snapshot.as_bytes()).unwrap();
    legacy_split::install(store.as_ref(), &identity, split_operation, 1, None, &revision).await;
    let split_key = legacy::key(b"DS\0SD\0", &identity, split_operation);
    let old = store.get(&split_key).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    state["initial"] = true.into();
    legacy::replace(store.as_ref(), &split_key, &old.bytes, &state).await;
    legacy::prepared_composition(store.as_ref(), &identity, operation, snapshot).await;
    let update = SnapshotHeadUpdate {
        snapshot,
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    for _ in 0..2 {
        assert_eq!(
            authority
                .abandon_composition(&identity, operation, 1)
                .await
                .unwrap()
                .record
                .progress,
            DraftProgress::Abandoned
        );
    }
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(parent)
    );
    let mut complete = false;
    let mut cursor = SplitReclaimCursor::new(identity.clone(), split_operation);
    for _ in 0..100 {
        if authority
            .cleanup_split_scratch_step_for_tests(&mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert!(authority.get_snapshot(&identity, snapshot).await.is_ok());
    assert_eq!(
        authority
            .get_split_revision(
                &identity,
                snapshot,
                result.initial_split.unwrap(),
                common::budget(),
                &ReadCancellation::new(),
            )
            .await
            .unwrap()
            .splits,
        revision.splits
    );
    assert_eq!(
        authority
            .inspect_split_head(&identity, snapshot)
            .await
            .unwrap()
            .revision,
        result.initial_split
    );
    authority.update_snapshot_head(&identity, &update).await.unwrap();
}

#[tokio::test]
async fn abandon_recognizes_a_successful_historical_candidate_without_deleting_it() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
    let snapshot = SnapshotId::random();
    authority
        .prepare_composition_resource(
            &identity,
            snapshot,
            &common::request(parent),
            None,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    legacy::prepared_composition(store.as_ref(), &identity, operation, snapshot).await;
    authority
        .update_snapshot_head(
            &identity,
            &SnapshotHeadUpdate {
                snapshot,
                expected_head: authority.inspect_head(&identity).await.unwrap().generation,
            },
        )
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
        .update_snapshot_head(
            &identity,
            &SnapshotHeadUpdate {
                snapshot: root,
                expected_head: authority.inspect_head(&identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    assert_eq!(
        authority
            .composition_status(&identity, operation)
            .await
            .unwrap()
            .record
            .progress,
        DraftProgress::Abandoned
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
