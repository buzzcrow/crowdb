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
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    let edit = common::edit(1, vec![SampleChange::Upsert(common::sample(b"new", 2))]);
    store.lose_edit_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .edit_composition(&identity, operation, &edit)
        .await
        .is_err());
    let key = page_key(&identity, operation, edit.id);
    assert!(store.get(&key).await.unwrap().is_none());
    let restarted = DatasetAuthority::new(store.clone());
    restarted
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    let cleanup = restarted
        .cleanup_composition_edits_step(&identity, operation, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_pages, 1);
    assert!(matches!(
        store
            .compare_exchange(&key, None, &serde_json::to_vec(&edit).unwrap())
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert_eq!(
        restarted
            .cleanup_composition_edits_step(&identity, operation, &ReadCancellation::new())
            .await
            .unwrap(),
        cleanup
    );
}
#[tokio::test]
async fn edit_payload_cleanup_preserves_historical_receipts_and_published_data() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    let edit = common::edit(1, vec![SampleChange::Upsert(common::sample(b"new", 2))]);
    let receipt = authority
        .edit_composition(&identity, operation, &edit)
        .await
        .unwrap();
    assert!(matches!(
        authority
            .cleanup_composition_edits_step(&identity, operation, &ReadCancellation::new())
            .await,
        Err(CompositionError::Frozen)
    ));
    common::evaluate(&authority, &identity, operation, 2).await;
    let snapshot = common::prepare(&authority, &identity, operation, 2)
        .await
        .preparation
        .unwrap()
        .candidate;
    let attempt = common::attempt(&authority, &identity, snapshot).await;
    authority
        .publish_composition(&identity, operation, &attempt)
        .await
        .unwrap();
    let progress = authority
        .cleanup_composition_edits_step(&identity, operation, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(progress.complete);
    assert_eq!(progress.retired_pages, 1);
    assert_eq!(
        store
            .get(&page_key(&identity, operation, edit.id))
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"composition-edit-retired-v1"
    );
    assert_eq!(
        authority
            .edit_composition(&identity, operation, &edit)
            .await
            .unwrap(),
        receipt
    );
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
