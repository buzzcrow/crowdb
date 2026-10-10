// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_storage.rs"]
mod split_storage;

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, ChunkKvError, MutationOperation, PartitionId, PartitionRange,
    PreparedSplitWriterArtifact, RequestId, SplitChild, SplitHandoffStore, SplitPlan, SplitWriterTarget,
    TransitionId,
};
use split_storage::TestSplitStorage;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct TestUnconfirmedHandoff(Mutex<Vec<PreparedSplitWriterArtifact>>);

#[async_trait::async_trait]
impl SplitHandoffStore for TestUnconfirmedHandoff {
    async fn commit(&self, artifact: &PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        let mut attempts = self.0.lock().unwrap();
        attempts.push(artifact.clone());
        if attempts.len() == 1 {
            Err(ChunkKvError::SplitRetry("handoff response was lost".into()))
        } else {
            assert_eq!(
                attempts[0], *artifact,
                "retry must preserve the exact handoff base"
            );
            Ok(())
        }
    }
}

struct TestConfirmedHandoff;

#[async_trait::async_trait]
impl SplitHandoffStore for TestConfirmedHandoff {
    async fn commit(&self, _: &PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn authoritative_abort_discards_the_pending_base_before_another_split() {
    let storage = TestSplitStorage::new();
    let range = PartitionRange {
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
    };
    let id = PartitionId { high: 92, low: 1 };
    let (parent, _) = storage
        .partition(
            id,
            range.clone(),
            1,
            Arc::new(MemoryPartitionTree::with_tree_id(92)),
        )
        .await;
    parent
        .mutate(
            1,
            RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: 1,
            },
            MutationOperation::Put {
                key: b"b".to_vec(),
                value: b"base".to_vec(),
            },
        )
        .await
        .unwrap();
    let mut plan = SplitPlan {
        transition_id: TransitionId { high: 92, low: 2 },
        parent_id: id,
        parent_range: range,
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 93, low: 1 },
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: Some(b"z".to_vec()),
            },
            ownership_epoch: 1,
        },
    };
    let target = SplitWriterTarget {
        tree_id: 93,
        tree_config: crowdb_tree_ffi::Config::default(),
        journal: storage
            .journal(crowdb_chunk_stream::StreamName { high: 93, low: 1 }, 1)
            .await,
    };
    assert!(parent
        .prepare_split_child_session(
            plan.clone(),
            target,
            8,
            Arc::new(TestUnconfirmedHandoff::default())
        )
        .await
        .is_err());
    assert!(parent.pending_split_child_target(&plan).unwrap().is_some());
    parent.release_generation_pin(plan.transition_id).unwrap();
    parent
        .abort_split(&crowdb_chunk_kv::SplitAbortProof {
            transition_id: plan.transition_id,
            parent_id: id,
            parent_epoch: 1,
            catalog_revision: 1,
        })
        .await
        .unwrap();
    assert!(parent.pending_split_child_target(&plan).unwrap().is_none());
    plan.transition_id.low = 3;
    plan.child.partition_id.low = 2;
    let target = SplitWriterTarget {
        tree_id: 94,
        tree_config: crowdb_tree_ffi::Config::default(),
        journal: storage
            .journal(crowdb_chunk_stream::StreamName { high: 94, low: 1 }, 1)
            .await,
    };
    let prepared = parent
        .prepare_split_child_session(plan, target, 8, Arc::new(TestConfirmedHandoff))
        .await
        .unwrap();
    assert_eq!(prepared.artifact.child.tree_id, 94);
}

#[tokio::test]
async fn unconfirmed_handoff_retries_the_same_base_while_parent_keeps_writing() {
    let storage = TestSplitStorage::new();
    let range = PartitionRange {
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
    };
    let id = PartitionId { high: 90, low: 1 };
    let (parent, _) = storage
        .partition(
            id,
            range.clone(),
            1,
            Arc::new(MemoryPartitionTree::with_tree_id(90)),
        )
        .await;
    parent
        .mutate(
            1,
            RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: 1,
            },
            MutationOperation::Put {
                key: b"b".to_vec(),
                value: b"base".to_vec(),
            },
        )
        .await
        .unwrap();
    let plan = SplitPlan {
        transition_id: TransitionId { high: 90, low: 2 },
        parent_id: id,
        parent_range: range,
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 91, low: 1 },
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: Some(b"z".to_vec()),
            },
            ownership_epoch: 1,
        },
    };
    let target = SplitWriterTarget {
        tree_id: 91,
        tree_config: crowdb_tree_ffi::Config::default(),
        journal: storage
            .journal(crowdb_chunk_stream::StreamName { high: 91, low: 1 }, 1)
            .await,
    };
    let store = Arc::new(TestUnconfirmedHandoff::default());
    assert!(parent
        .prepare_split_child_session(plan.clone(), target.clone(), 8, store.clone())
        .await
        .is_err());
    parent
        .mutate(
            1,
            RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: 2,
            },
            MutationOperation::Put {
                key: b"n".to_vec(),
                value: b"after-lost-response".to_vec(),
            },
        )
        .await
        .unwrap();
    let prepared = parent
        .prepare_split_child_session(plan, target, 8, store.clone())
        .await
        .unwrap();
    assert_eq!(store.0.lock().unwrap().len(), 2);
    assert_eq!(prepared.artifact.cutover_seq, 2);
    let child = parent.split_ingress().unwrap().child();
    assert_eq!(
        child.get(1, b"n", None).await.unwrap().unwrap().value,
        b"after-lost-response"
    );
}
