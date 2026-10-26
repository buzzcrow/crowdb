// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/legacy_split.rs"]
mod legacy;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::DraftProgress, split::*, *};
use std::sync::atomic::Ordering;

fn draft(operation: OperationId, snapshot: SnapshotId) -> SplitDraftRecord {
    SplitDraftRecord {
        version: 1,
        operation,
        snapshot,
        expected_revision: None,
        revision: 1,
        policy: SplitPolicy::default(),
        splits: vec![],
        progress: DraftProgress::Editing,
    }
}
fn costs(store: &split_store::TestDraftStore) -> (usize, usize) {
    (
        store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}

#[tokio::test]
async fn abandoning_legacy_split_draft_is_idempotent_and_does_not_replay_edit_receipts() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::state(store.as_ref(), &identity, &draft(operation, snapshot)).await;
    let key = legacy::state_key(b"DS\0SD\0", &identity, operation);
    let previous = store.get(&key).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&previous.bytes).unwrap();
    state["edit"] = serde_json::json!({ "id": OperationId::random(), "digest": vec![1u8; 32] });
    assert!(matches!(
        store
            .compare_exchange(&key, Some(&previous.bytes), &serde_json::to_vec(&state).unwrap())
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    let (reads, writes) = costs(&store);
    let abandoned = authority
        .abandon_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    assert_eq!(abandoned.progress, DraftProgress::Abandoned);
    assert_eq!(costs(&store), (reads + 1, writes + 1));
    assert_eq!(
        DatasetAuthority::new(store.clone())
            .abandon_split_draft(&identity, operation, 1)
            .await
            .unwrap(),
        abandoned
    );
    assert_eq!(costs(&store), (reads + 2, writes + 1));
}

#[tokio::test]
async fn lost_legacy_abandon_reply_does_not_confirm_or_write_again_on_retry() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::state(store.as_ref(), &identity, &draft(operation, snapshot)).await;
    store.lose_edit_reply.store(true, Ordering::Relaxed);
    let (reads, writes) = costs(&store);
    assert!(authority
        .abandon_split_draft(&identity, operation, 1)
        .await
        .is_err());
    assert_eq!(costs(&store), (reads + 1, writes + 1));
    let abandoned = DatasetAuthority::new(store.clone())
        .abandon_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    assert_eq!(abandoned.progress, DraftProgress::Abandoned);
    assert_eq!(costs(&store), (reads + 2, writes + 1));
}

#[tokio::test]
async fn legacy_evaluation_remains_readable_and_validated_without_advancing_it() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    let mut record = draft(operation, snapshot);
    record.progress = DraftProgress::Evaluating {
        input_revision: 1,
        next_page: 1,
    };
    legacy::state(store.as_ref(), &identity, &record).await;
    let key = legacy::state_key(b"DS\0SD\0", &identity, operation);
    let previous = store.get(&key).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&previous.bytes).unwrap();
    state["evaluation"] = serde_json::json!({
        "input_revision": 1, "candidate": legacy::revision(operation, 1), "next_page": 1,
        "after": [1, 0, 255], "complete": false,
        "validation": { "scanned": 1, "assigned": 0, "unassigned": 1, "overlapping": 0, "unassigned_examples": [[1, 0, 255]], "overlap_examples": [] },
        "bindings": [],
    });
    let bytes = serde_json::to_vec(&state).unwrap();
    assert!(matches!(
        store
            .compare_exchange(&key, Some(&previous.bytes), &bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    let (reads, writes) = costs(&store);
    assert_eq!(
        authority.split_draft_status(&identity, operation).await.unwrap(),
        record
    );
    assert_eq!(costs(&store), (reads + 1, writes));
    assert_eq!(store.get(&key).await.unwrap().unwrap().bytes, bytes);
    state["evaluation"]["candidate"] = serde_json::json!(SplitRevisionId::random());
    assert!(matches!(
        store
            .compare_exchange(&key, Some(&bytes), &serde_json::to_vec(&state).unwrap())
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    assert!(matches!(
        DatasetAuthority::new(store)
            .split_draft_status(&identity, operation)
            .await,
        Err(SplitLifecycleError::Corrupt)
    ));
}
