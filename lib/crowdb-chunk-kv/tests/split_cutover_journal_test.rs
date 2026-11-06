// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_storage.rs"]
mod split_storage;

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, Checkpoint, MutationOperation, Partition, PartitionConfig, PartitionId,
    PartitionJournal, PartitionRange, PartitionTree, PreparedSplitWriterArtifact, RequestId, SplitChild,
    SplitHandoffStore, SplitPlan, SplitWriterTarget, StreamPartitionJournal, TransitionId,
};
use crowdb_chunk_stream::{memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName};
use split_storage::TestSplitStorage;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

#[derive(Default)]
struct TestHandoff {
    proof: Mutex<Option<PreparedSplitWriterArtifact>>,
    entered: Notify,
    release: Notify,
}

#[async_trait::async_trait]
impl SplitHandoffStore for TestHandoff {
    async fn commit(&self, artifact: &PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        *self.proof.lock().await = Some(artifact.clone());
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

struct TestCrashState {
    parent_store: Arc<MemoryStreamStore>,
    child_store: Arc<MemoryStreamStore>,
    proof: PreparedSplitWriterArtifact,
    checkpoint: Checkpoint,
    plan: SplitPlan,
    published_base_end: u64,
    end: u64,
}

fn operation(key: &[u8], value: &[u8]) -> MutationOperation {
    MutationOperation::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

async fn mutate(parent: &Partition, sequence: u64, key: &[u8], value: &[u8]) {
    parent
        .mutate(
            1,
            RequestId {
                client_high: 92,
                client_low: 1,
                client_sequence: sequence,
            },
            operation(key, value),
        )
        .await
        .unwrap();
}

async fn interrupt_after_route() -> TestCrashState {
    let parent_storage = TestSplitStorage::new();
    let child_storage = TestSplitStorage::new();
    let parent_tree = Arc::new(MemoryPartitionTree::with_tree_id(92));
    let (parent, journal) = parent_storage
        .partition(
            PartitionId { high: 92, low: 1 },
            PartitionRange::default(),
            1,
            parent_tree.clone(),
        )
        .await;
    mutate(&parent, 1, b"b", b"base-left").await;
    let base_end = journal.tail();
    let plan = SplitPlan {
        transition_id: TransitionId { high: 92, low: 2 },
        parent_id: parent.snapshot().partition_id,
        parent_range: PartitionRange::default(),
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 93, low: 1 },
            ownership_epoch: 1,
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: None,
            },
        },
    };
    let child_journal = child_storage.journal(StreamName { high: 93, low: 1 }, 1).await;
    let target = SplitWriterTarget {
        tree_id: 93,
        tree_config: crowdb_tree_ffi::Config::default(),
        journal: child_journal,
    };
    let handoff = Arc::new(TestHandoff::default());
    let source = parent.clone();
    let pending_plan = plan.clone();
    let gate = handoff.clone();
    let mut preparing = tokio::spawn(async move {
        source
            .prepare_split_child_session(pending_plan, target, 8, gate)
            .await
    });
    tokio::select! {
        () = handoff.entered.notified() => {},
        result = &mut preparing => panic!("handoff failed before commit: {}", failure(result)),
    }
    let published_base = open_published(&parent_storage.store, journal.stream_name()).await;
    let published_base_end = published_base.tail();
    assert_eq!(published_base_end, base_end);
    // Memory's live facade has no persisted root identity. Capture C before
    // this fixture advances the live parent beyond the immutable base.
    let (manifest, applied) = parent_tree.checkpoint_state().unwrap();
    let checkpoint = Checkpoint {
        tree_id: 92,
        tree_manifest: manifest,
        root_manifest_generation: parent_tree.root_manifest_generation().unwrap(),
        applied_seq: applied,
        stream_name: journal.stream_name(),
        stream_generation: journal.record_generation(),
        replay_offset: base_end,
    };
    assert_eq!(checkpoint.applied_seq, 1);
    mutate(&parent, 2, b"n", b"parent-tail").await;
    mutate(&parent, 3, b"c", b"left-tail").await;
    let end = journal.tail();
    parent_storage.store.pause_publication_for_tests();
    handoff.release.notify_one();
    tokio::select! {
        () = parent_storage.store.wait_for_publication_for_tests() => {},
        result = &mut preparing => panic!("handoff failed before WAL proof: {}", failure(result)),
    }
    assert!(parent.split_ingress().is_some());
    let child = parent.split_ingress().unwrap().child();
    assert_eq!(child.snapshot().applied_seq, 3);
    mutate(&parent, 4, b"n", b"child-tail").await;
    assert_eq!(child.snapshot().applied_seq, 4);
    assert!(!preparing.is_finished());
    let proof = handoff.proof.lock().await.clone().unwrap();
    assert_eq!(proof.applied_seq, 1);
    assert_eq!(proof.base_applied_seq, 1);
    // The fixed Tree already includes C. With no catch-up records, its replay
    // interval stays empty at the retained retry floor; it is not end(C).
    assert_eq!(proof.parent_replay_offset, 0);
    assert_eq!(proof.parent_cutover_offset, proof.parent_replay_offset);
    TestCrashState {
        parent_store: parent_storage.store,
        child_store: child_storage.store,
        proof,
        checkpoint,
        plan,
        published_base_end,
        end,
    }
}

fn failure(
    result: Result<crowdb_chunk_kv::Result<crowdb_chunk_kv::PreparedSplit>, tokio::task::JoinError>,
) -> String {
    match result {
        Ok(Err(error)) => error.to_string(),
        Err(error) => error.to_string(),
        Ok(Ok(_)) => "completed before the expected gate".into(),
    }
}

async fn open_published(store: &Arc<MemoryStreamStore>, name: StreamName) -> ChunkStream {
    ChunkStream::open_read_only(
        name,
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap()
}

async fn reopen_writer(store: &Arc<MemoryStreamStore>, name: StreamName) -> ChunkStream {
    ChunkStream::open(
        name,
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap()
}

async fn recover_after_interrupt(state: TestCrashState) {
    state.parent_store.resume_publication_for_tests();
    let source_name = state.checkpoint.stream_name;
    let stale_reader = open_published(&state.parent_store, source_name).await;
    assert_eq!(stale_reader.tail(), state.published_base_end);
    assert!(stale_reader.tail() < state.end);
    // The old runtime was dropped while its publication was paused. Writer
    // recovery scans synchronized frames before a separate read-only view opens.
    let source_stream = reopen_writer(&state.parent_store, source_name).await;
    assert_eq!(source_stream.tail(), state.end);
    let source_journal: Arc<dyn PartitionJournal> =
        Arc::new(StreamPartitionJournal::new(source_stream, source_name).unwrap());
    let source_tree = Arc::new(MemoryPartitionTree::with_tree_id(92));
    source_tree
        .apply(1, &operation(b"b", b"base-left"))
        .await
        .unwrap();
    source_tree
        .checkpoint(state.checkpoint.replay_offset)
        .await
        .unwrap();
    let parent = Partition::recover_prepared_assignment(
        state.plan.parent_id,
        PartitionRange::default(),
        1,
        state.checkpoint,
        PartitionConfig::default(),
        source_tree,
        source_journal,
    )
    .await
    .unwrap();
    assert!(
        parent
            .publish_split_journal_prefix(state.proof.parent_cutover_offset)
            .await
            .unwrap()
            >= state.end
    );
    let reader = open_published(&state.parent_store, source_name).await;
    assert!(reader.tail() >= state.end);
    let child_stream = reopen_writer(&state.child_store, state.proof.stream_name).await;
    let child_journal = Arc::new(StreamPartitionJournal::new(child_stream, state.proof.stream_name).unwrap());
    let child_tree = Arc::new(MemoryPartitionTree::with_tree_id(93));
    child_tree.advance_noop(1).await.unwrap();
    child_tree.checkpoint(0).await.unwrap();
    let source_reader = Arc::new(StreamPartitionJournal::new(reader, source_name).unwrap());
    let (child, artifact) = Partition::recover_split_handoff(
        state.proof,
        PartitionConfig::default(),
        child_tree.clone(),
        child_journal,
        source_reader,
    )
    .await
    .unwrap();
    assert_eq!(artifact.applied_seq, 3);
    assert_eq!(artifact.parent_cutover_offset, state.end);
    assert_eq!(artifact.child_stream_start_seq, 4);
    assert_eq!(child.snapshot().applied_seq, 4);
    assert_eq!(
        child.snapshot().lifecycle,
        crowdb_chunk_kv::PartitionLifecycle::Prepared
    );
    let prepared = parent
        .resume_split_child_session(state.plan, child.clone(), artifact)
        .await
        .unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 3);
    assert_eq!(
        child.get(1, b"n", None).await.unwrap().unwrap().value,
        b"child-tail"
    );
    assert!(matches!(
        child.get(1, b"c", None).await,
        Err(crowdb_chunk_kv::ChunkKvError::OutOfRange)
    ));
    assert!(child_tree.get(b"c").await.unwrap().is_none());
    mutate(&parent, 5, b"o", b"recovered-route").await;
    assert_eq!(
        parent
            .split_ingress()
            .unwrap()
            .child()
            .get(1, b"o", None)
            .await
            .unwrap()
            .unwrap()
            .value,
        b"recovered-route"
    );
}

#[test]
fn crash_after_route_before_parent_publication_recovers_first_child_wal() {
    let old_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let state = old_runtime.block_on(interrupt_after_route());
    drop(old_runtime);
    let recovered_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    recovered_runtime.block_on(recover_after_interrupt(state));
}
