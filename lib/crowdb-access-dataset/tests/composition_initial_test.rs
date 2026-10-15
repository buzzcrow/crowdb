// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/composition.rs"]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::*, *};

#[tokio::test]
async fn initial_split_and_data_share_visibility_and_later_revision_is_independent() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let parent = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let operation = OperationId::random();
    let input = CompositionInput {
        splits: split_store::partition_splits(),
        ..common::input(parent)
    };
    common::begin_evaluated(&authority, &identity, operation, &input).await;
    let cancel = ReadCancellation::new();
    let first = authority
        .prepare_composition(&identity, operation, 1, &cancel)
        .await
        .unwrap();
    assert!(matches!(first.record.progress, DraftProgress::Evaluated { .. }));
    let preparation = first.preparation.unwrap();
    let snapshot = preparation.candidate;
    let split_operation = preparation.split_operation.unwrap();
    let attempt = common::attempt(&authority, &identity, snapshot).await;
    assert!(authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .is_err());
    assert!(authority.inspect_split_head(&identity, snapshot).await.is_err());
    let prepared = common::prepare(&authority, &identity, operation, 1).await;
    assert!(matches!(prepared.record.progress, DraftProgress::Prepared { .. }));
    assert!(authority.inspect_split_head(&identity, snapshot).await.is_err());
    let receipt = authority
        .publish_composition(&identity, operation, &attempt)
        .await
        .unwrap();
    let initial = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    assert!(initial.revision.is_some());
    assert_eq!(initial.generation, None);
    let record = authority
        .get_split_revision(
            &identity,
            snapshot,
            initial.revision.unwrap(),
            common::budget(),
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(record.splits.iter().map(|s| s.count).sum::<u64>(), 45);
    assert_eq!(
        authority.inspect_head(&identity).await.unwrap().latest,
        Some(snapshot)
    );
    assert!(authority
        .publish_split_draft(
            &identity,
            split_operation,
            &split_store::attempt(&authority, &identity, &record).await
        )
        .await
        .is_err());
    let next = split_store::prepared(&authority, &identity, snapshot, initial.revision).await;
    authority
        .publish_split_draft(
            &identity,
            next.operation,
            &split_store::attempt(&authority, &identity, &next).await,
        )
        .await
        .unwrap();
    assert_eq!(
        authority
            .inspect_split_head(&identity, snapshot)
            .await
            .unwrap()
            .revision,
        Some(next.revision)
    );
    assert_eq!(
        authority
            .publish_composition(&identity, operation, &attempt)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        authority
            .inspect_split_head(&identity, snapshot)
            .await
            .unwrap()
            .revision,
        Some(next.revision)
    );
    assert!(authority
        .get_split_revision(
            &identity,
            snapshot,
            initial.revision.unwrap(),
            common::budget(),
            &cancel
        )
        .await
        .is_ok());
}

#[tokio::test]
async fn child_inherits_frozen_splits_and_new_samples_stay_unassigned() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let revision = split_store::prepared(&authority, &identity, parent, None).await;
    authority
        .publish_split_draft(
            &identity,
            revision.operation,
            &split_store::attempt(&authority, &identity, &revision).await,
        )
        .await
        .unwrap();
    let operation = OperationId::random();
    let input = CompositionInput {
        inherit_parent_splits: true,
        ..common::input(parent)
    };
    let draft = authority
        .begin_composition(&identity, operation, &input)
        .await
        .unwrap();
    assert_eq!(draft.record.parent_split_revision, Some(revision.revision));
    authority
        .edit_composition(
            &identity,
            operation,
            &common::edit(1, vec![SampleChange::Upsert(common::sample(b"new", 2))]),
        )
        .await
        .unwrap();
    common::evaluate(&authority, &identity, operation, 2).await;
    let prepared = common::prepare(&authority, &identity, operation, 2).await;
    let preparation = prepared.preparation.unwrap();
    assert_eq!(preparation.split_preview.unwrap().validation.unassigned, 1);
    let attempt = CommitAttempt {
        version: 1,
        id: OperationId::random(),
        target: CommitTarget::Snapshot {
            snapshot: preparation.candidate,
        },
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    authority
        .publish_composition(&identity, operation, &attempt)
        .await
        .unwrap();
    let current = authority
        .inspect_split_head(&identity, preparation.candidate)
        .await
        .unwrap()
        .revision
        .unwrap();
    let child = authority
        .get_split_revision(
            &identity,
            preparation.candidate,
            current,
            common::budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(child.splits.iter().map(|s| s.count).sum::<u64>(), 1);
    assert!(child.splits.iter().all(|s| matches!(&s.origin, split::MembershipOrigin::Inherited(source) if source.snapshot == parent && source.revision == revision.revision)));
}

#[tokio::test]
async fn child_split_source_is_pinned_while_parent_current_revision_changes() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let revision = split_store::prepared(&authority, &identity, parent, None).await;
    authority
        .publish_split_draft(
            &identity,
            revision.operation,
            &split_store::attempt(&authority, &identity, &revision).await,
        )
        .await
        .unwrap();
    let selected = revision.splits.iter().find(|s| s.count > 0).unwrap();
    let operation = OperationId::random();
    let input = CompositionInput {
        source: CompositionSource::Split {
            source: split::SplitSource {
                snapshot: parent,
                revision: revision.revision,
                name: selected.name.clone(),
            },
            selection: Some(Selection::Prefix(b"sample".to_vec())),
        },
        inherit_parent_splits: true,
        ..common::input(parent)
    };
    authority
        .begin_composition(&identity, operation, &input)
        .await
        .unwrap();
    let next = split_store::prepared(&authority, &identity, parent, Some(revision.revision)).await;
    authority
        .publish_split_draft(
            &identity,
            next.operation,
            &split_store::attempt(&authority, &identity, &next).await,
        )
        .await
        .unwrap();
    assert_eq!(
        common::evaluate(&authority, &identity, operation, 1)
            .await
            .members,
        selected.count
    );
    let prepared = common::prepare(&authority, &identity, operation, 1).await;
    assert_eq!(prepared.record.parent, Some(parent));
    assert_eq!(prepared.record.parent_split_revision, Some(revision.revision));
    let snapshot = prepared.preparation.unwrap().candidate;
    authority
        .publish_composition(
            &identity,
            operation,
            &common::attempt(&authority, &identity, snapshot).await,
        )
        .await
        .unwrap();
    assert_eq!(
        authority.get_manifest(&identity, snapshot).await.unwrap().samples,
        vec![common::sample(b"sample-1", 1)]
    );
}
