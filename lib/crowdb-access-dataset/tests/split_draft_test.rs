// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/legacy_split.rs"]
mod legacy;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::DraftProgress, split::*, *};

#[tokio::test]
async fn abandoning_legacy_split_draft_is_idempotent_and_does_not_replay_edit_receipts() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let operation = OperationId::random();
    legacy::state(
        store.as_ref(),
        &identity,
        &SplitDraftRecord {
            version: 1,
            operation,
            snapshot,
            expected_revision: None,
            revision: 1,
            policy: SplitPolicy::default(),
            splits: vec![],
            progress: DraftProgress::Editing,
        },
    )
    .await;
    store
        .reject_receipts
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let abandoned = authority
        .abandon_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    assert_eq!(abandoned.progress, DraftProgress::Abandoned);
    assert_eq!(
        DatasetAuthority::new(store)
            .abandon_split_draft(&identity, operation, 1)
            .await
            .unwrap(),
        abandoned
    );
}
