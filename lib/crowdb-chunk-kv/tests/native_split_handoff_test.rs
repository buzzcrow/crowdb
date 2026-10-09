// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_storage.rs"]
mod split_storage;

use crowdb_chunk_kv::{
    CrowdbPartitionTree, MutationOperation, PartitionId, PartitionRange, RequestId, SplitChild,
    SplitHandoffStore, SplitPlan, SplitWriterTarget, TransitionId,
};
use split_storage::TestSplitStorage;
use std::sync::Arc;

struct TestNativeHandoffGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl SplitHandoffStore for TestNativeHandoffGate {
    async fn commit(&self, _: &crowdb_chunk_kv::PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

fn put(sequence: u64, key: &[u8], value: &[u8]) -> (RequestId, MutationOperation) {
    (
        RequestId {
            client_high: 60,
            client_low: 1,
            client_sequence: sequence,
        },
        MutationOperation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        },
    )
}

struct TestCommittedHandoff;

#[async_trait::async_trait]
impl SplitHandoffStore for TestCommittedHandoff {
    async fn commit(&self, _: &crowdb_chunk_kv::PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        Ok(())
    }
}

fn native_config() -> crowdb_tree_ffi::Config {
    crowdb_tree_ffi::Config {
        page_store: Some(Arc::new(crowdb_tree_ffi::PageStore::open_mem(65_536).unwrap())),
        ..crowdb_tree_ffi::Config::default()
    }
}

#[tokio::test]
async fn consecutive_native_splits_preserve_every_original_record() {
    let storage = TestSplitStorage {
        store: Arc::new(crowdb_chunk_stream::memory::MemoryStreamStore::new(128 * 1024)),
    };
    let tree = Arc::new(CrowdbPartitionTree::open(80, &native_config()).unwrap());
    let (parent, _) = storage
        .partition(
            PartitionId { high: 80, low: 1 },
            PartitionRange::default(),
            1,
            tree,
        )
        .await;
    for index in 0..512 {
        let key = format!("key-{index:04}");
        let (request, operation) = put(index + 1, key.as_bytes(), &native_value(index));
        parent.mutate(1, request, operation).await.unwrap();
    }
    let mut partitions = vec![parent];
    for (round, split_key) in [(1, b"key-0256"), (2, b"key-0128"), (3, b"key-0384")] {
        let index = usize::from(round == 3);
        let source = partitions[index].clone();
        let snapshot = source.snapshot();
        let plan = SplitPlan {
            transition_id: TransitionId { high: 80, low: round },
            parent_id: snapshot.partition_id,
            parent_range: snapshot.range.clone(),
            parent_epoch: snapshot.ownership_epoch,
            parent_next_epoch: snapshot.ownership_epoch + 1,
            split_key: split_key.to_vec(),
            child: SplitChild {
                partition_id: PartitionId { high: 81, low: round },
                ownership_epoch: 1,
                range: PartitionRange {
                    start: Some(split_key.to_vec()),
                    end: snapshot.range.end,
                },
            },
        };
        let target = SplitWriterTarget {
            tree_id: 80 + round,
            tree_config: native_config(),
            journal: storage
                .journal(crowdb_chunk_stream::StreamName { high: 81, low: round }, 1)
                .await,
        };
        let prepared = source
            .prepare_split_child_session(plan.clone(), target, 8, Arc::new(TestCommittedHandoff))
            .await
            .unwrap();
        let child = prepared
            .child
            .open_warmed(crowdb_chunk_kv::PartitionConfig::default())
            .unwrap();
        source
            .complete_local_split_handoff(&prepared.artifact)
            .await
            .unwrap();
        source.release_generation_pin(plan.transition_id).unwrap();
        child.release_generation_pin(plan.transition_id).unwrap();
        partitions[index] = source.split_ingress().unwrap().retained_parent();
        partitions.push(child);
        verify_native_records(&partitions).await;
    }
    for partition in &partitions {
        partition
            .checkpoint(partition.snapshot().ownership_epoch)
            .await
            .unwrap();
    }
    verify_native_records(&partitions).await;
}

fn native_value(index: u64) -> Vec<u8> {
    let mut state = index + 1;
    let mut bytes = Vec::with_capacity(64 * 1024);
    for _ in 0..8 * 1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes
}

async fn verify_native_records(partitions: &[crowdb_chunk_kv::Partition]) {
    for index in 0..512 {
        let key = format!("key-{index:04}");
        let partition = partitions
            .iter()
            .find(|partition| partition.snapshot().range.contains(key.as_bytes()))
            .unwrap();
        let record = partition
            .get(partition.snapshot().ownership_epoch, key.as_bytes(), None)
            .await
            .unwrap();
        assert!(record.is_some(), "missing {key}");
        assert_eq!(record.unwrap().value, native_value(index), "{key}");
    }
}

#[tokio::test]
async fn native_child_inherits_writes_accepted_while_handoff_status_is_pending() {
    let storage = TestSplitStorage::new();
    let parent_id = PartitionId { high: 60, low: 1 };
    let parent_tree = Arc::new(
        CrowdbPartitionTree::open(
            60,
            &crowdb_tree_ffi::Config {
                page_store: Some(Arc::new(crowdb_tree_ffi::PageStore::open_mem(65_536).unwrap())),
                ..crowdb_tree_ffi::Config::default()
            },
        )
        .unwrap(),
    );
    let (parent, _) = storage
        .partition(parent_id, PartitionRange::default(), 1, parent_tree)
        .await;
    let (request, operation) = put(1, b"n", b"base");
    parent.mutate(1, request, operation).await.unwrap();
    let plan = SplitPlan {
        transition_id: TransitionId { high: 60, low: 2 },
        parent_id,
        parent_range: PartitionRange::default(),
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 61, low: 1 },
            ownership_epoch: 1,
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: None,
            },
        },
    };
    let target = SplitWriterTarget {
        tree_id: 61,
        tree_config: crowdb_tree_ffi::Config {
            page_store: Some(Arc::new(crowdb_tree_ffi::PageStore::open_mem(65_536).unwrap())),
            ..crowdb_tree_ffi::Config::default()
        },
        journal: storage
            .journal(crowdb_chunk_stream::StreamName { high: 61, low: 1 }, 1)
            .await,
    };
    let gate = Arc::new(TestNativeHandoffGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let source = parent.clone();
    let waiting_gate = gate.clone();
    let preparing = tokio::spawn(async move {
        source
            .prepare_split_child_session(plan, target, 8, waiting_gate)
            .await
    });
    gate.entered.notified().await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        for (sequence, key) in [(2, b"b"), (3, b"n"), (4, b"o")] {
            let (request, operation) = put(sequence, key, b"during-status");
            parent.mutate(1, request, operation).await.unwrap();
            assert_eq!(
                parent.get(1, key, None).await.unwrap().unwrap().value,
                b"during-status"
            );
        }
    })
    .await
    .unwrap();
    gate.release.notify_one();
    let prepared = preparing.await.unwrap().unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 4);
    assert_eq!(prepared.artifact.retained_parent.tree_id, 60);
    let child = prepared
        .child
        .open_warmed(crowdb_chunk_kv::PartitionConfig::default())
        .unwrap();
    for key in [b"n", b"o"] {
        assert_eq!(
            child.get(1, key, None).await.unwrap().unwrap().value,
            b"during-status"
        );
    }
    let (request, operation) = put(5, b"n", b"child-own-wal");
    let written = parent.mutate(1, request, operation).await.unwrap();
    assert_eq!(written.journal_position.stream_name, child.snapshot().stream_name);
    assert_eq!(
        parent.get(1, b"b", None).await.unwrap().unwrap().value,
        b"during-status"
    );
}
