// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_storage.rs"]
mod split_storage;

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange,
    PartitionTree, PreparedSplitWriterArtifact, RequestId,
};
use crowdb_chunk_stream::StreamName;
use split_storage::TestSplitStorage;
use std::sync::Arc;

fn request(sequence: u64) -> RequestId {
    RequestId {
        client_high: 1,
        client_low: 1,
        client_sequence: sequence,
    }
}

fn put(key: &[u8], value: &[u8]) -> MutationOperation {
    MutationOperation::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

async fn recover_after_handoff(child_has_write: bool, checkpoint_after_dispatch: bool) {
    let storage = TestSplitStorage::new();
    let parent_id = PartitionId { high: 50, low: 1 };
    let child_id = PartitionId { high: 51, low: 1 };
    let child_journal = storage.journal(StreamName { high: 51, low: 1 }, 1).await;
    let parent_tree = Arc::new(MemoryPartitionTree::with_tree_id(50));
    let (parent, parent_journal) = storage
        .partition(parent_id, PartitionRange::default(), 1, parent_tree.clone())
        .await;
    parent
        .mutate(1, request(1), put(b"b", b"base-left"))
        .await
        .unwrap();
    let base_offset = parent_journal.tail();
    let (_, _, mut cold_parent_tree) = parent_tree.checkpoint_snapshot(base_offset).await.unwrap();
    let mut source_checkpoint = source_checkpoint_at_base(&parent_journal, base_offset);
    let child_range = PartitionRange {
        start: Some(b"m".to_vec()),
        end: None,
    };
    let initial = PreparedSplitWriterArtifact {
        partition_id: child_id,
        range: child_range.clone(),
        ownership_epoch: 1,
        tree_id: 51,
        tree_manifest: 1,
        root_manifest_generation: 1,
        stream_name: child_journal.stream_name(),
        base_applied_seq: 1,
        parent_id,
        parent_epoch: 1,
        parent_stream_name: parent_journal.stream_name(),
        parent_stream_manifest_generation: parent_journal.manifest_generation(),
        parent_replay_offset: base_offset,
        parent_cutover_offset: base_offset,
        applied_seq: 1,
        child_stream_start_seq: 2,
    };
    // Status is durable, while dispatch still has not changed. Both writes
    // belong to the original parent WAL and must survive a crash here.
    parent
        .mutate(1, request(2), put(b"n", b"parent-tail"))
        .await
        .unwrap();
    parent
        .mutate(1, request(3), put(b"c", b"left-tail"))
        .await
        .unwrap();
    if child_has_write {
        let live_tree = Arc::new(MemoryPartitionTree::with_tree_id(51));
        live_tree.advance_noop(3).await.unwrap();
        let child = Partition::open(
            child_id,
            child_range,
            1,
            PartitionConfig::default(),
            live_tree,
            child_journal.clone(),
        )
        .unwrap();
        child
            .mutate(1, request(4), put(b"n", b"child-tail"))
            .await
            .unwrap();
        // Parent and child sequences diverge after dispatch. The parent's
        // later sequence 4 must not move the inherited child frontier to 4.
        parent
            .mutate(1, request(5), put(b"d", b"later-left"))
            .await
            .unwrap();
        drop(child);
    }
    if checkpoint_after_dispatch {
        let (checkpoint, tree) =
            checkpoint_source_after_dispatch(&parent, &parent_tree, &parent_journal, source_checkpoint).await;
        source_checkpoint = checkpoint;
        cold_parent_tree = tree;
    }
    drop(parent);
    let cold_tree = Arc::new(MemoryPartitionTree::with_tree_id(51));
    cold_tree.advance_noop(1).await.unwrap();
    cold_tree.checkpoint(0).await.unwrap();
    let (recovered, artifact) = Partition::recover_split_handoff(
        initial,
        PartitionConfig::default(),
        cold_tree.clone(),
        child_journal,
        parent_journal.clone(),
    )
    .await
    .unwrap();
    verify_recovery(&recovered, &artifact, &cold_tree, child_has_write).await;
    verify_restored_dispatch(
        recovered,
        artifact,
        cold_parent_tree,
        parent_journal,
        source_checkpoint,
    )
    .await;
}

async fn verify_restored_dispatch(
    child: Partition,
    artifact: PreparedSplitWriterArtifact,
    source_tree: Arc<dyn PartitionTree>,
    source_journal: Arc<dyn crowdb_chunk_kv::PartitionJournal>,
    checkpoint: crowdb_chunk_kv::Checkpoint,
) {
    let parent_id = artifact.parent_id;
    let parent = Partition::recover_prepared_assignment(
        parent_id,
        PartitionRange::default(),
        1,
        checkpoint,
        PartitionConfig::default(),
        source_tree,
        source_journal,
    )
    .await
    .unwrap();
    let plan = crowdb_chunk_kv::SplitPlan {
        transition_id: crowdb_chunk_kv::TransitionId { high: 52, low: 1 },
        parent_id,
        parent_range: PartitionRange::default(),
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: crowdb_chunk_kv::SplitChild {
            partition_id: artifact.partition_id,
            range: artifact.range.clone(),
            ownership_epoch: 1,
        },
    };
    let prepared = parent
        .resume_split_child_session(plan, child, artifact)
        .await
        .unwrap();
    assert_eq!(prepared.artifact.retained_parent.tree_id, 50);
    assert_eq!(
        parent.get(1, b"b", None).await.unwrap().unwrap().value,
        b"base-left"
    );
    let right = parent
        .mutate(1, request(6), put(b"o", b"restarted-child"))
        .await
        .unwrap();
    assert_eq!(
        right.journal_position.stream_name,
        prepared.artifact.child.stream_name
    );
    let left = parent
        .mutate(1, request(7), put(b"e", b"restarted-parent"))
        .await
        .unwrap();
    assert_eq!(
        left.journal_position.stream_name,
        prepared.artifact.retained_parent.stream_name
    );
}

async fn verify_recovery(
    recovered: &Partition,
    artifact: &PreparedSplitWriterArtifact,
    cold_tree: &MemoryPartitionTree,
    child_has_write: bool,
) {
    assert_eq!(artifact.applied_seq, 3);
    assert_eq!(artifact.child_stream_start_seq, 4);
    assert_eq!(
        recovered.snapshot().applied_seq,
        if child_has_write { 4 } else { 3 }
    );
    assert_eq!(
        cold_tree.get(b"n").await.unwrap().unwrap().value,
        if child_has_write {
            b"child-tail".as_slice()
        } else {
            b"parent-tail".as_slice()
        }
    );
    for key in [b"b", b"c", b"d"] {
        assert!(cold_tree.get(key).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn status_committed_before_dispatch_recovers_parent_suffix() {
    recover_after_handoff(false, false).await;
}

#[tokio::test]
async fn child_wal_identifies_inherited_frontier_and_replays_after_parent() {
    recover_after_handoff(true, false).await;
}

#[tokio::test]
async fn recovered_parent_checkpoint_can_be_ahead_of_child_inherited_frontier() {
    recover_after_handoff(true, true).await;
}

async fn checkpoint_source_after_dispatch(
    parent: &Partition,
    tree: &MemoryPartitionTree,
    journal: &Arc<dyn crowdb_chunk_kv::PartitionJournal>,
    mut checkpoint: crowdb_chunk_kv::Checkpoint,
) -> (crowdb_chunk_kv::Checkpoint, Arc<dyn PartitionTree>) {
    let offset = journal.tail();
    let (manifest, generation, snapshot) = tree.checkpoint_snapshot(offset).await.unwrap();
    checkpoint.tree_manifest = manifest;
    checkpoint.root_manifest_generation = generation;
    checkpoint.applied_seq = parent.snapshot().applied_seq;
    checkpoint.replay_offset = offset;
    assert!(checkpoint.applied_seq > 3);
    (checkpoint, snapshot)
}

fn source_checkpoint_at_base(
    journal: &Arc<dyn crowdb_chunk_kv::PartitionJournal>,
    base_offset: u64,
) -> crowdb_chunk_kv::Checkpoint {
    crowdb_chunk_kv::Checkpoint {
        tree_id: 50,
        tree_manifest: 1,
        root_manifest_generation: 1,
        applied_seq: 1,
        stream_name: journal.stream_name(),
        stream_manifest_generation: journal.manifest_generation(),
        replay_offset: base_offset,
    }
}
