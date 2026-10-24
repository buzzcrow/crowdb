// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::*, split::*, *};

#[tokio::test]
async fn complete_initial_split_and_later_revision_are_independent_of_snapshot_head_retries() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let parent = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let input = CompositionRequest {
        splits: split_store::partition_splits(),
        ..common::request(parent)
    };
    let snapshot = SnapshotId::random();
    let result = authority
        .prepare_composition_resource(&identity, snapshot, &input, None, &ReadCancellation::new())
        .await
        .unwrap();
    let initial = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    assert_eq!(initial.revision, result.initial_split);
    assert_eq!(initial.generation, None);
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(parent));
    let update = SnapshotHeadUpdate {
        snapshot,
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    authority.update_snapshot_head(&identity, &update).await.unwrap();
    let record = authority
        .get_split_revision(
            &identity,
            snapshot,
            initial.revision.unwrap(),
            common::budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(record.splits.iter().map(|split| split.count).sum::<u64>(), 45);
    let next = split_store::prepared(&authority, &identity, snapshot, initial.revision).await;
    authority
        .update_split_head(
            &identity,
            snapshot,
            &SplitHeadUpdate {
                revision: next.revision,
                expected_revision: initial.revision,
                expected_head: initial.generation,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        authority
            .update_snapshot_head(&identity, &update)
            .await
            .unwrap()
            .latest,
        Some(snapshot)
    );
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
            .get_split_revision(
                &identity,
                snapshot,
                initial.revision.unwrap(),
                common::budget(),
                &ReadCancellation::new(),
            )
            .await
            .unwrap(),
        record
    );
}
