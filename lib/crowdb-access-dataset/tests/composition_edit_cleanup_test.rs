// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/legacy_drafts.rs"]
#[allow(dead_code)]
mod legacy;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::*, *};
fn page_key(identity: &DatasetIdentity, operation: OperationId, edit: OperationId) -> Vec<u8> {
    let mut key = b"DS\0CE\0".to_vec();
    key.extend(identity.key().unwrap());
    key.extend(operation.as_bytes());
    key.extend(edit.as_bytes());
    key
}
#[tokio::test]
async fn failed_edit_admission_is_tracked_and_fences_late_payload_writes() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    let mut cursor = CompositionReclaimCursor::new(identity.clone(), operation);
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
    let (edit, payload) = legacy::composition_edit(
        store.as_ref(),
        &identity,
        operation,
        vec![SampleChange::Upsert(common::sample(b"new", 2))],
        false,
    )
    .await;
    let key = page_key(&identity, operation, edit);
    assert!(store.get(&key).await.unwrap().is_none());
    let restarted = DatasetAuthority::new(store.clone());
    restarted
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    let cleanup = restarted
        .cleanup_composition_edits_step_for_tests(&mut cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_pages, 1);
    assert!(matches!(
        store.compare_exchange(&key, None, &payload).await.unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert_eq!(
        restarted
            .cleanup_composition_edits_step_for_tests(&mut cursor, &ReadCancellation::new())
            .await
            .unwrap(),
        cleanup
    );
}
#[tokio::test]
async fn edit_payload_cleanup_preserves_historical_receipts_and_published_data() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    let mut cursor = CompositionReclaimCursor::new(identity.clone(), operation);
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
    let changes = vec![SampleChange::Upsert(common::sample(b"new", 2))];
    let (edit, _) =
        legacy::composition_edit(store.as_ref(), &identity, operation, changes.clone(), true).await;
    let mut receipt_key = legacy::key(b"DS\0CX\0", &identity, operation);
    receipt_key.extend(edit.as_bytes());
    let receipt = store.get(&receipt_key).await.unwrap().unwrap().bytes;
    assert!(matches!(
        authority
            .cleanup_composition_edits_step_for_tests(&mut cursor, &ReadCancellation::new())
            .await,
        Err(CompositionError::Frozen)
    ));
    let snapshot = SnapshotId::random();
    authority
        .prepare_composition_resource(
            &identity,
            snapshot,
            &CompositionRequest {
                parent: Some(parent),
                source: CompositionSource::All,
                schema: None,
                samples: changes,
                splits: vec![],
                policy: split::SplitPolicy::default(),
            },
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
    let progress = authority
        .cleanup_composition_edits_step_for_tests(&mut cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(progress.complete);
    assert_eq!(progress.retired_pages, 1);
    assert_eq!(
        store
            .get(&page_key(&identity, operation, edit))
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"composition-edit-retired-v1"
    );
    assert_eq!(store.get(&receipt_key).await.unwrap().unwrap().bytes, receipt);
    assert_eq!(
        authority
            .get_manifest(&identity, snapshot)
            .await
            .unwrap()
            .samples
            .len(),
        2
    );
}
