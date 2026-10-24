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
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
    let rows = legacy::composition_window(
        store.as_ref(),
        &identity,
        operation,
        &[common::sample(b"sample-1", 1)],
    )
    .await;
    let (row_key, original) = &rows[0];
    let changes = vec![SampleChange::Upsert(common::sample(b"new", 3))];
    legacy::composition_edit(store.as_ref(), &identity, operation, changes.clone(), true).await;
    finish(&authority, &identity, operation).await;
    let sealed = store.get(row_key).await.unwrap().unwrap();
    assert!(sealed.bytes.len() < original.len());
    assert!(matches!(
        store
            .compare_exchange(row_key, Some(original), original)
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert!(matches!(
        store.compare_exchange(row_key, None, original).await.unwrap(),
        CasOutcome::Conflict(_)
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
    let update = SnapshotHeadUpdate {
        snapshot,
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    authority.update_snapshot_head(&identity, &update).await.unwrap();
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
            .update_snapshot_head(&identity, &update)
            .await
            .unwrap()
            .latest,
        Some(snapshot)
    );
}

#[tokio::test]
async fn abandoning_before_first_window_fences_a_delayed_work_page() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
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
    assert_eq!(
        authority
            .composition_status(&identity, operation)
            .await
            .unwrap()
            .record
            .progress,
        DraftProgress::Abandoned
    );
}

#[tokio::test]
async fn cancellation_and_partial_window_cleanup_resume_after_restart() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
    legacy::composition_window(
        store.as_ref(),
        &identity,
        operation,
        &[common::sample(b"sample-1", 1)],
    )
    .await;
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
