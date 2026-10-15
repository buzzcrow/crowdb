// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/composition.rs"]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::*, *};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn composition_resumes_binary_members_without_counting_retried_windows_twice() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let parent = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    let cancel = ReadCancellation::new();
    store.lose_edit_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .evaluate_composition(&identity, operation, 1, 0, &cancel)
        .await
        .is_err());
    let restarted = DatasetAuthority::new(store);
    let first = restarted
        .evaluate_composition(&identity, operation, 1, 0, &cancel)
        .await
        .unwrap();
    assert_eq!(first.next_page, 1);
    assert!(!first.complete);
    let completed = common::evaluate(&restarted, &identity, operation, 1).await;
    assert_eq!(completed.members, 45);
    assert_eq!(
        restarted
            .evaluate_composition(&identity, operation, 1, 0, &cancel)
            .await
            .unwrap(),
        completed
    );
    let prepared = restarted
        .prepare_composition(&identity, operation, 1, &cancel)
        .await
        .unwrap();
    let DraftProgress::Prepared {
        target: CommitTarget::Snapshot { snapshot },
    } = prepared.record.progress
    else {
        panic!("missing candidate");
    };
    assert_eq!(
        restarted
            .get_manifest(&identity, snapshot)
            .await
            .unwrap()
            .samples
            .len(),
        45
    );
}

#[tokio::test]
async fn cancelled_preview_and_invalid_patches_never_become_prepared() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    let cancelled = ReadCancellation::new();
    cancelled.cancel();
    assert!(authority
        .evaluate_composition(&identity, operation, 1, 0, &cancelled)
        .await
        .is_err());
    assert_eq!(
        authority
            .composition_status(&identity, operation)
            .await
            .unwrap()
            .record
            .progress,
        DraftProgress::Editing
    );
    let duplicate = common::edit(
        1,
        vec![
            SampleChange::Upsert(common::sample(b"x", 1)),
            SampleChange::Remove(b"x".to_vec()),
        ],
    );
    assert!(authority
        .edit_composition(&identity, operation, &duplicate)
        .await
        .is_err());
    let removal = CompositionEdit {
        schema: Some(SchemaRecord {
            version: 2,
            fields: vec![],
        }),
        ..common::edit(1, vec![])
    };
    assert!(authority
        .edit_composition(&identity, operation, &removal)
        .await
        .is_err());
    let partial = common::edit(
        1,
        vec![SampleChange::Upsert(SampleRecord {
            sample_id: b"new".to_vec(),
            fields: vec![],
        })],
    );
    authority
        .edit_composition(&identity, operation, &partial)
        .await
        .unwrap();
    let mut failed = false;
    for page in 0..10 {
        if authority
            .evaluate_composition(&identity, operation, 2, page, &ReadCancellation::new())
            .await
            .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(failed, "missing required value must fail complete validation");
    assert!(authority
        .prepare_composition(&identity, operation, 2, &ReadCancellation::new())
        .await
        .is_err());
}
