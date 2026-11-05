// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, MutationOperation, PartitionRange, PartitionTree, SplitOverlayLimits,
};

fn put(key: &[u8], value: &[u8]) -> MutationOperation {
    MutationOperation::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

#[tokio::test]
async fn observer_covers_registration_replay_gap_and_detaches_at_exact_frontier() {
    let source = MemoryPartitionTree::with_tree_id(1);
    let destination = MemoryPartitionTree::with_tree_id(2);
    source.apply(1, &put(b"a", b"base")).await.unwrap();
    destination.apply(1, &put(b"a", b"base")).await.unwrap();
    let (token, start) = source
        .arm_split_overlay(
            &destination,
            SplitOverlayLimits {
                max_records: 8,
                max_bytes: 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!(start, 1);
    source.apply(2, &put(b"b", b"before-replay")).await.unwrap();
    destination.apply(2, &put(b"b", b"before-replay")).await.unwrap();
    token.prepare(2).await.unwrap();
    source.apply(3, &put(b"c", b"after-replay")).await.unwrap();
    let installed = token.try_install(3).unwrap();
    assert_eq!(token.committed_frontier(), Some(3));
    assert_eq!(token.try_install(3).unwrap(), installed);
    assert!(token.try_install(4).is_err());
    source
        .apply(4, &put(b"d", b"parent-after-cutover"))
        .await
        .unwrap();
    destination
        .apply(4, &put(b"b", b"child-after-cutover"))
        .await
        .unwrap();
    source
        .publish_split_memtable_view(installed.generation, 3, &destination, &PartitionRange::default())
        .await
        .unwrap();
    assert_eq!(destination.get(b"a").await.unwrap().unwrap().value, b"base");
    assert_eq!(
        destination.get(b"b").await.unwrap().unwrap().value,
        b"child-after-cutover"
    );
    assert_eq!(
        destination.get(b"c").await.unwrap().unwrap().value,
        b"after-replay"
    );
    assert!(destination.get(b"d").await.unwrap().is_none());
    source
        .release_split_memtable_view(installed.generation)
        .await
        .unwrap();
    assert_eq!(source.prepared_overlay_footprint_for_tests().await, (0, 0, false));
}

#[tokio::test]
async fn expired_observer_stops_growth_without_rejecting_parent_writes() {
    let source = MemoryPartitionTree::with_tree_id(1);
    let destination = MemoryPartitionTree::with_tree_id(2);
    let (token, start) = source
        .arm_split_overlay(
            &destination,
            SplitOverlayLimits {
                max_records: 2,
                max_bytes: 4,
            },
        )
        .await
        .unwrap();
    assert_eq!(start, 0);
    token.prepare(0).await.unwrap();
    source.apply(1, &put(b"a", b"1")).await.unwrap();
    source.apply(2, &put(b"b", b"2")).await.unwrap();
    source.apply(3, &put(b"c", b"3")).await.unwrap();
    let exhausted = source.prepared_overlay_footprint_for_tests().await;
    assert_eq!(exhausted, (2, 4, true));
    for sequence in 4_u64..100 {
        source
            .apply(sequence, &put(&sequence.to_be_bytes(), b"still-serving"))
            .await
            .unwrap();
    }
    assert_eq!(source.prepared_overlay_footprint_for_tests().await, exhausted);
    assert_eq!(source.last_applied_seq(), 99);
    assert!(token.try_install(99).is_err());
    assert_eq!(destination.last_applied_seq(), 0);
    assert!(destination.get(b"a").await.unwrap().is_none());
    assert_eq!(
        source.get(&99_u64.to_be_bytes()).await.unwrap().unwrap().value,
        b"still-serving"
    );
}
