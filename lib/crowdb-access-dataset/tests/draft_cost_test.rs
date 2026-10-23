// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/composition.rs"]
#[allow(dead_code)]
mod composition;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::{composition::SampleChange, split::*, *};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn draft_edit_replays_read_existing_receipts_without_mutations() {
    let store = access_store::TestAccessStore::shared();
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "draft-cost").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let parent = authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let composition = OperationId::random();
    authority
        .begin_composition(&identity, composition, &composition::input(parent))
        .await
        .unwrap();
    let edit = composition::edit(1, vec![SampleChange::Upsert(composition::sample(b"new", 2))]);
    let expected = authority
        .edit_composition(&identity, composition, &edit)
        .await
        .unwrap();
    assert_eq!(expected.record.source_lease_deadline, 0);
    let writes = store.writes.load(Ordering::Relaxed);
    for _ in 0..8 {
        assert_eq!(
            authority
                .edit_composition(&identity, composition, &edit)
                .await
                .unwrap(),
            expected
        );
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
    let split = OperationId::random();
    authority
        .begin_split_draft(&identity, split, parent, None, SplitPolicy::default(), vec![])
        .await
        .unwrap();
    let edit = SplitDraftEdit {
        id: OperationId::random(),
        expected_revision: 1,
        policy: SplitPolicy::default(),
        splits: vec![],
    };
    let expected = authority.edit_split_draft(&identity, split, &edit).await.unwrap();
    let writes = store.writes.load(Ordering::Relaxed);
    for _ in 0..8 {
        assert_eq!(
            authority.edit_split_draft(&identity, split, &edit).await.unwrap(),
            expected
        );
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}
