// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::DraftProgress, split::*, *};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn split_draft_edits_recover_lost_replies_and_historical_retries() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let head = authority.inspect_head(&identity).await.unwrap();
    let operation = OperationId::random();
    let initial = authority
        .begin_split_draft(
            &identity,
            operation,
            snapshot,
            None,
            SplitPolicy::default(),
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(initial.revision, 1);
    let edit = SplitDraftEdit {
        id: OperationId::random(),
        expected_revision: 1,
        policy: SplitPolicy::default(),
        splits: vec![DraftSplit {
            name: SplitName::new("train").unwrap(),
            definition: SplitDefinition::Selection(Selection::Prefix(b"sample".to_vec())),
            origin: MembershipOrigin::Evaluated,
        }],
    };
    store.lose_edit_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .edit_split_draft(&identity, operation, &edit)
        .await
        .is_err());
    let restarted = DatasetAuthority::new(store);
    let changed = restarted
        .edit_split_draft(&identity, operation, &edit)
        .await
        .unwrap();
    assert_eq!(changed.revision, 2);
    let next = SplitDraftEdit {
        id: OperationId::random(),
        expected_revision: 2,
        policy: SplitPolicy::default(),
        splits: vec![],
    };
    let latest = restarted
        .edit_split_draft(&identity, operation, &next)
        .await
        .unwrap();
    assert_eq!(latest.revision, 3);
    assert_eq!(
        restarted
            .edit_split_draft(&identity, operation, &edit)
            .await
            .unwrap(),
        changed
    );
    assert_eq!(
        restarted.split_draft_status(&identity, operation).await.unwrap(),
        latest
    );
    let divergent = SplitDraftEdit {
        splits: vec![],
        ..edit.clone()
    };
    assert!(matches!(
        restarted.edit_split_draft(&identity, operation, &divergent).await,
        Err(SplitLifecycleError::Conflict)
    ));
    assert_eq!(
        restarted
            .begin_split_draft(
                &identity,
                operation,
                snapshot,
                None,
                SplitPolicy::default(),
                vec![]
            )
            .await
            .unwrap(),
        latest
    );
    assert!(matches!(
        restarted
            .begin_split_draft(
                &identity,
                operation,
                snapshot,
                None,
                SplitPolicy::default(),
                edit.splits
            )
            .await,
        Err(SplitLifecycleError::Conflict)
    ));
    assert_eq!(restarted.inspect_head(&identity).await.unwrap(), head);
}

#[tokio::test]
async fn abandoning_split_draft_is_idempotent_and_freezes_edits() {
    let (_, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    authority
        .begin_split_draft(
            &identity,
            operation,
            snapshot,
            None,
            SplitPolicy::default(),
            vec![],
        )
        .await
        .unwrap();
    let abandoned = authority
        .abandon_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    assert_eq!(abandoned.progress, DraftProgress::Abandoned);
    assert_eq!(
        authority
            .abandon_split_draft(&identity, operation, 1)
            .await
            .unwrap(),
        abandoned
    );
    assert!(matches!(
        authority
            .edit_split_draft(
                &identity,
                operation,
                &SplitDraftEdit {
                    id: OperationId::random(),
                    expected_revision: 1,
                    policy: SplitPolicy::default(),
                    splits: vec![]
                }
            )
            .await,
        Err(SplitLifecycleError::Frozen)
    ));
}
