// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/read_query.rs"]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::*;
use std::sync::Arc;

#[tokio::test]
async fn continuation_pins_snapshot_and_split_across_instances() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let first = split_store::prepared(&authority, &identity, snapshot, None).await;
    split_store::publish_revision(&authority, &identity, &first).await;
    let authority = Arc::new(authority);
    let reads = DatasetReads::new(authority.clone());
    let binding = reads.bind(&identity, common::spec("train")).await.unwrap();
    assert_eq!(binding.split.as_ref().unwrap().revision, first.revision);
    let next = split_store::prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    split_store::publish_revision(&authority, &identity, &next).await;
    authority
        .publish_snapshot(&identity, Some(snapshot), publication::publication_manifest(2))
        .await
        .unwrap();
    let batch = reads
        .next(&identity, &binding.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(batch.snapshot, snapshot);
    let restarted = DatasetReads::new(authority);
    assert_eq!(
        restarted
            .next(&identity, &binding.cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .ids,
        batch.ids
    );
    let ids = common::drain(&restarted, &identity, binding).await;
    assert_eq!(
        ids.len() as u64,
        first
            .splits
            .iter()
            .find(|s| s.name.as_str() == "train")
            .unwrap()
            .count
    );
}

#[tokio::test]
async fn split_read_filter_and_workers_partition_only_admitted_members() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let revision = split_store::prepared(&authority, &identity, snapshot, None).await;
    split_store::publish_revision(&authority, &identity, &revision).await;
    let reads = DatasetReads::new(Arc::new(authority));
    let mut all = std::collections::BTreeSet::new();
    let mut total = 0;
    for worker in 0..2 {
        let mut spec = common::spec("train");
        spec.selection = Selection::Range {
            start: vec![10],
            end: vec![30],
        };
        spec.partition.workers = 2;
        spec.partition.worker = worker;
        spec.shuffle = Some(ReadShuffle {
            seed: 42,
            group_size: 8,
        });
        let prepared = reads.bind(&identity, spec.clone()).await.unwrap();
        for id in common::drain(&reads, &identity, prepared).await {
            assert!((10..30).contains(&id[0]));
            assert!(all.insert(id));
            total += 1;
        }
    }
    let mut spec = common::spec("train");
    spec.selection = Selection::Range {
        start: vec![10],
        end: vec![30],
    };
    let prepared = reads.bind(&identity, spec.clone()).await.unwrap();
    let expected = common::drain(&reads, &identity, prepared).await;
    assert_eq!(total, expected.len());
    assert_eq!(all, expected.into_iter().collect());
}
