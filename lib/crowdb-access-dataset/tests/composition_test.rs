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
async fn composition_edit_lost_reply_and_historical_retry_keep_latest_revision() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    let input = common::input(parent);
    authority
        .begin_composition(&identity, operation, &input)
        .await
        .unwrap();
    let edit = common::edit(1, vec![SampleChange::Upsert(common::sample(b"new", 2))]);
    store.lose_edit_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .edit_composition(&identity, operation, &edit)
        .await
        .is_err());
    let restarted = DatasetAuthority::new(store);
    let result = restarted
        .edit_composition(&identity, operation, &edit)
        .await
        .unwrap();
    assert_eq!(result.record.revision, 2);
    let next = restarted
        .edit_composition(
            &identity,
            operation,
            &common::edit(2, vec![SampleChange::Remove(b"sample-1".to_vec())]),
        )
        .await
        .unwrap();
    assert_eq!(
        restarted
            .edit_composition(&identity, operation, &edit)
            .await
            .unwrap(),
        result
    );
    assert_eq!(
        restarted
            .begin_composition(&identity, operation, &input)
            .await
            .unwrap(),
        next
    );
    assert!(matches!(
        restarted
            .edit_composition(
                &identity,
                operation,
                &CompositionEdit {
                    samples: vec![],
                    ..edit
                }
            )
            .await,
        Err(CompositionError::Conflict)
    ));
    let preview = common::evaluate(&restarted, &identity, operation, 3).await;
    assert_eq!(preview.members, 1);
    let prepared = restarted
        .prepare_composition(&identity, operation, 3, &ReadCancellation::new())
        .await
        .unwrap();
    let DraftProgress::Prepared {
        target: CommitTarget::Snapshot { snapshot },
    } = prepared.record.progress
    else {
        panic!("missing candidate");
    };
    assert_eq!(
        restarted.inspect_head(&identity).await.unwrap().latest,
        Some(parent)
    );
    let candidate = restarted.get_manifest(&identity, snapshot).await.unwrap();
    assert_eq!(candidate.samples, vec![common::sample(b"new", 2)]);
}

#[tokio::test]
async fn composition_keeps_effective_parent_values_and_invalidates_preview_on_edit() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_composition(&identity, operation, &common::input(parent))
        .await
        .unwrap();
    let preview = common::evaluate(&authority, &identity, operation, 1).await;
    assert_eq!(preview.members, 1);
    let edit = common::edit(
        1,
        vec![SampleChange::Upsert(SampleRecord {
            sample_id: b"sample-1".to_vec(),
            fields: vec![],
        })],
    );
    let changed = authority
        .edit_composition(&identity, operation, &edit)
        .await
        .unwrap();
    assert_eq!(changed.record.progress, DraftProgress::Editing);
    assert!(authority
        .prepare_composition(&identity, operation, 1, &ReadCancellation::new())
        .await
        .is_err());
    assert!(authority
        .prepare_composition(&identity, operation, 2, &ReadCancellation::new())
        .await
        .is_err());
    let preview = common::evaluate(&authority, &identity, operation, 2).await;
    assert_eq!(preview.members, 1);
    let prepared = authority
        .prepare_composition(&identity, operation, 2, &ReadCancellation::new())
        .await
        .unwrap();
    let DraftProgress::Prepared {
        target: CommitTarget::Snapshot { snapshot },
    } = prepared.record.progress
    else {
        panic!("missing candidate");
    };
    assert_eq!(
        authority.get_manifest(&identity, snapshot).await.unwrap().samples,
        vec![common::sample(b"sample-1", 1)]
    );
}

#[tokio::test]
async fn source_filter_and_explicit_upsert_allow_reintroducing_parent_members() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    let input = CompositionInput {
        source: CompositionSource::Selection(Selection::Prefix(b"excluded".to_vec())),
        ..common::input(parent)
    };
    authority
        .begin_composition(&identity, operation, &input)
        .await
        .unwrap();
    assert_eq!(
        common::evaluate(&authority, &identity, operation, 1)
            .await
            .members,
        0
    );
    assert!(authority
        .prepare_composition(&identity, operation, 1, &ReadCancellation::new())
        .await
        .is_err());
    authority
        .edit_composition(
            &identity,
            operation,
            &common::edit(
                1,
                vec![SampleChange::Upsert(SampleRecord {
                    sample_id: b"sample-1".to_vec(),
                    fields: vec![],
                })],
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        common::evaluate(&authority, &identity, operation, 2)
            .await
            .members,
        1
    );
    authority
        .abandon_composition(&identity, operation, 2)
        .await
        .unwrap();
    assert!(matches!(
        authority
            .edit_composition(&identity, operation, &common::edit(2, vec![]))
            .await,
        Err(CompositionError::Frozen)
    ));
}
