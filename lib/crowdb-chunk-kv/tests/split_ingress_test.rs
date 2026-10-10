// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_storage.rs"]
mod split_storage;

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, MutationOperation, PartitionId, PartitionRange, RequestId, SplitChild,
    SplitPlan, TransitionId,
};
use split_storage::TestSplitStorage;
use std::sync::Arc;

struct TestHandoffGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl crowdb_chunk_kv::SplitHandoffStore for TestHandoffGate {
    async fn commit(&self, _: &crowdb_chunk_kv::PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[tokio::test]
async fn handoff_persistence_does_not_block_parent_reads_or_journal_appends() {
    let storage = TestSplitStorage::new();
    let parent_id = PartitionId { high: 10, low: 1 };
    let range = PartitionRange {
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
    };
    let parent_tree = Arc::new(MemoryPartitionTree::with_tree_id(10));
    let (parent, _) = storage
        .partition(parent_id, range.clone(), 1, parent_tree.clone())
        .await;
    parent.mutate(1, request(10), put(b"b", b"base")).await.unwrap();
    let plan = SplitPlan {
        transition_id: TransitionId { high: 10, low: 2 },
        parent_id,
        parent_range: range,
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 11, low: 1 },
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: Some(b"z".to_vec()),
            },
            ownership_epoch: 1,
        },
    };
    let target = crowdb_chunk_kv::SplitWriterTarget {
        tree_id: 11,
        tree_config: crowdb_tree_ffi::Config::default(),
        journal: storage
            .journal(crowdb_chunk_stream::StreamName { high: 11, low: 1 }, 1)
            .await,
    };
    let gate = Arc::new(TestHandoffGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let preparing_parent = parent.clone();
    let preparing_gate = gate.clone();
    let preparing = tokio::spawn(async move {
        preparing_parent
            .prepare_split_child_session(plan, target, 8, preparing_gate)
            .await
    });
    gate.entered.notified().await;
    let old_stream = parent.snapshot().stream_name;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        for (sequence, key) in [(11, b"c"), (12, b"n")] {
            let result = parent
                .mutate(1, request(sequence), put(key, b"during-handoff"))
                .await
                .unwrap();
            assert_eq!(result.journal_position.stream_name, old_stream);
            assert_eq!(
                parent.get(1, key, None).await.unwrap().unwrap().value,
                b"during-handoff"
            );
        }
    })
    .await
    .unwrap();
    parent_tree.pause_split_publish();
    gate.release.notify_one();
    parent_tree.wait_for_split_publish().await;
    verify_live_dispatch_during_publication(&parent).await;
    parent_tree.resume_split_publish();
    let prepared = preparing.await.unwrap().unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 3);
    assert_eq!(prepared.artifact.retained_parent.tree_id, 10);
    assert_eq!(prepared.artifact.retained_parent.stream_name, old_stream);
    let child = prepared
        .child
        .open_warmed(crowdb_chunk_kv::PartitionConfig::default())
        .unwrap();
    assert_eq!(
        child.get(1, b"n", None).await.unwrap().unwrap().value,
        b"new-child-value"
    );
    let right = parent
        .mutate(1, request(13), put(b"o", b"child-wal"))
        .await
        .unwrap();
    assert_eq!(right.journal_position.stream_name, child.snapshot().stream_name);
}

async fn verify_live_dispatch_during_publication(parent: &crowdb_chunk_kv::Partition) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        parent
            .mutate(1, request(20), put(b"n", b"new-child-value"))
            .await
            .unwrap();
        parent
            .mutate(1, request(21), put(b"d", b"new-parent-value"))
            .await
            .unwrap();
        assert_eq!(
            parent.get(1, b"n", None).await.unwrap().unwrap().value,
            b"new-child-value"
        );
        assert_eq!(
            parent.get(1, b"d", None).await.unwrap().unwrap().value,
            b"new-parent-value"
        );
    })
    .await
    .unwrap();
}

fn put(key: &[u8], value: &[u8]) -> MutationOperation {
    MutationOperation::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn request(client_sequence: u64) -> RequestId {
    RequestId {
        client_high: 1,
        client_low: 1,
        client_sequence,
    }
}

struct TestCommittedHandoff;

#[async_trait::async_trait]
impl crowdb_chunk_kv::SplitHandoffStore for TestCommittedHandoff {
    async fn commit(&self, _: &crowdb_chunk_kv::PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn retained_parent_can_complete_a_second_split_on_the_original_worker() {
    let storage = TestSplitStorage::new();
    let id = PartitionId { high: 70, low: 1 };
    let range = PartitionRange {
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
    };
    let (mut parent, _) = storage
        .partition(id, range, 1, Arc::new(MemoryPartitionTree::with_tree_id(70)))
        .await;
    let original_stream = parent.snapshot().stream_name;
    for (epoch, split_key) in [(1, b"m"), (2, b"g")] {
        parent
            .mutate(epoch, request(70 + epoch), put(b"b", b"left"))
            .await
            .unwrap();
        let plan = SplitPlan {
            transition_id: TransitionId { high: 70, low: epoch },
            parent_id: id,
            parent_range: parent.snapshot().range,
            parent_epoch: epoch,
            parent_next_epoch: epoch + 1,
            split_key: split_key.to_vec(),
            child: SplitChild {
                partition_id: PartitionId { high: 71, low: epoch },
                range: PartitionRange {
                    start: Some(split_key.to_vec()),
                    end: parent.snapshot().range.end,
                },
                ownership_epoch: 1,
            },
        };
        let target = crowdb_chunk_kv::SplitWriterTarget {
            tree_id: 70 + epoch,
            tree_config: crowdb_tree_ffi::Config::default(),
            journal: storage
                .journal(crowdb_chunk_stream::StreamName { high: 71, low: epoch }, 1)
                .await,
        };
        let prepared = parent
            .prepare_split_child_session(plan.clone(), target, 8, Arc::new(TestCommittedHandoff))
            .await
            .unwrap();
        assert_eq!(
            parent.lifecycle(),
            crowdb_chunk_kv::PartitionLifecycle::SplitFinalizing
        );
        parent
            .complete_local_split_handoff(&prepared.artifact)
            .await
            .unwrap();
        // Simulate the catalog publishing an independent child before another split.
        parent.release_generation_pin(plan.transition_id).unwrap();
        parent = parent.split_ingress().unwrap().retained_parent();
        assert_eq!(parent.snapshot().stream_name, original_stream);
        assert_eq!(
            parent.get(epoch + 1, b"b", None).await.unwrap().unwrap().value,
            b"left"
        );
    }
    parent
        .mutate(3, request(80), put(b"c", b"after-two-splits"))
        .await
        .unwrap();
}

#[tokio::test]
async fn child_dispatch_keeps_left_writes_in_original_parent_tree_and_journal() {
    let storage = TestSplitStorage::new();
    let parent_id = PartitionId { high: 1, low: 1 };
    let parent_range = PartitionRange {
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
    };
    let (parent, _) = storage
        .partition(
            parent_id,
            parent_range.clone(),
            1,
            Arc::new(MemoryPartitionTree::with_tree_id(1)),
        )
        .await;
    parent.mutate(1, request(1), put(b"b", b"before")).await.unwrap();
    let before = parent.snapshot();
    let plan = SplitPlan {
        transition_id: TransitionId { high: 3, low: 4 },
        parent_id,
        parent_range,
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 2, low: 1 },
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: Some(b"z".to_vec()),
            },
            ownership_epoch: 1,
        },
    };
    parent.begin_split(plan.clone()).await.unwrap();
    let (child, _) = storage
        .partition(
            plan.child.partition_id,
            plan.child.range.clone(),
            1,
            Arc::new(MemoryPartitionTree::with_tree_id(2)),
        )
        .await;
    parent
        .install_child_split_ingress(&plan, child.clone())
        .await
        .unwrap();
    let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(
            parent.mutate(1, request(2), put(b"c", b"left")),
            parent.mutate(1, request(3), put(b"n", b"right"))
        )
    })
    .await
    .unwrap();
    assert_eq!(left.unwrap().journal_position.stream_name, before.stream_name);
    assert_eq!(
        right.unwrap().journal_position.stream_name,
        child.snapshot().stream_name
    );
    let retained = parent.split_ingress().unwrap().retained_parent();
    assert_eq!(retained.tree_id(), parent.tree_id());
    assert_eq!(retained.snapshot().stream_name, before.stream_name);
    assert_eq!(retained.snapshot().journal_durable_seq, 2);
    assert_eq!(parent.get(1, b"b", None).await.unwrap().unwrap().value, b"before");
    assert_eq!(parent.get(1, b"c", None).await.unwrap().unwrap().value, b"left");
    assert_eq!(parent.get(1, b"n", None).await.unwrap().unwrap().value, b"right");
    // Registry ownership keeps the original worker alive independently of the
    // old full-range compatibility handle.
    drop(parent);
    retained
        .mutate(2, request(4), put(b"d", b"still-live"))
        .await
        .unwrap();
    assert_eq!(retained.snapshot().journal_durable_seq, 3);
}
