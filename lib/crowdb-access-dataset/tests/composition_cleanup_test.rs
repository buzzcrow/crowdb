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

fn scope(prefix: &[u8], identity: &DatasetIdentity, operation: OperationId, revision: u64) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend(identity.key().unwrap());
    key.extend(operation.as_bytes());
    key.extend(revision.to_be_bytes());
    key
}
async fn finish(authority: &DatasetAuthority, identity: &DatasetIdentity, operation: OperationId) {
    for _ in 0..100 {
        if authority
            .cleanup_composition_scratch_step(identity, operation, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("scratch cleanup did not finish");
}

#[tokio::test]
async fn obsolete_evaluation_retires_bulk_rows_without_breaking_current_input() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    common::evaluate(&authority, &identity, operation, 1).await;
    let rows = store
        .scan_page(
            &MetadataScan {
                prefix: scope(b"DS\0CM\0", &identity, operation, 1),
                after: None,
                budget: common::budget(),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.entries.len(), 1);
    let (row_key, original) = &rows.entries[0];
    authority
        .edit_composition(
            &identity,
            operation,
            &common::edit(1, vec![SampleChange::Upsert(common::sample(b"new", 3))]),
        )
        .await
        .unwrap();
    finish(&authority, &identity, operation).await;
    let sealed = store.get(row_key).await.unwrap().unwrap();
    assert!(sealed.bytes.len() < original.bytes.len());
    assert!(matches!(
        store
            .compare_exchange(row_key, Some(&original.bytes), &original.bytes)
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert!(matches!(
        store
            .compare_exchange(row_key, None, &original.bytes)
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert_eq!(
        common::evaluate(&authority, &identity, operation, 2)
            .await
            .members,
        2
    );
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
    let restarted = DatasetAuthority::new(store);
    finish(&restarted, &identity, operation).await;
    assert_eq!(
        restarted
            .get_manifest(&identity, snapshot)
            .await
            .unwrap()
            .samples
            .len(),
        2
    );
    assert_eq!(
        restarted
            .publish_composition(&identity, operation, &attempt)
            .await
            .unwrap()
            .attempt,
        attempt
    );
}

#[tokio::test]
async fn abandoning_before_first_window_fences_a_delayed_work_page() {
    let (store, authority, identity, parent) = split_store::fixture().await;
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
    let mut work_key = scope(b"DS\0CW\0", &identity, operation, 1);
    work_key.extend(0u64.to_be_bytes());
    assert!(matches!(
        store
            .compare_exchange(&work_key, None, b"delayed work")
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    finish(&authority, &identity, operation).await;
    assert!(matches!(
        authority
            .evaluate_composition(&identity, operation, 1, 0, &ReadCancellation::new())
            .await,
        Err(CompositionError::Frozen)
    ));
}

#[tokio::test]
async fn cancellation_and_partial_window_cleanup_resume_after_restart() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    authority
        .evaluate_composition(&identity, operation, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    let cancel = ReadCancellation::new();
    cancel.cancel();
    assert!(authority
        .cleanup_composition_scratch_step(&identity, operation, &cancel)
        .await
        .is_err());
    let first = authority
        .cleanup_composition_scratch_step(&identity, operation, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(!first.complete);
    assert_eq!(first.next_page, 1);
    finish(&DatasetAuthority::new(store), &identity, operation).await;
}
