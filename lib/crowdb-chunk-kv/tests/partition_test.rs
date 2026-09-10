// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    Checkpoint, ChunkKvError, CompareCondition, MutationOperation, MutationResult, Partition,
    PartitionConfig, PartitionId, PartitionJournal, PartitionManager, PartitionRange, PartitionTree,
    PreparedChildArtifact, RequestId, SplitAbortProof, SplitArtifact, SplitChild, SplitCommitProof,
    SplitPlan, StreamPartitionJournal, TransitionId,
};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, CursorAdvance, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig,
    StreamMetadataStore, StreamName, StreamRegistry,
};

fn request(sequence: u64) -> RequestId {
    RequestId {
        client_high: 11,
        client_low: 12,
        client_sequence: sequence,
    }
}

fn split_plan(parent_id: PartitionId, parent_epoch: u64) -> SplitPlan {
    SplitPlan {
        transition_id: TransitionId { high: 44, low: 55 },
        parent_id,
        parent_range: PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        parent_epoch,
        split_key: b"g".to_vec(),
        left: SplitChild {
            partition_id: PartitionId { high: 21, low: 1 },
            range: PartitionRange {
                start: Some(b"a".to_vec()),
                end: Some(b"g".to_vec()),
            },
            ownership_epoch: parent_epoch + 1,
        },
        right: SplitChild {
            partition_id: PartitionId { high: 21, low: 2 },
            range: PartitionRange {
                start: Some(b"g".to_vec()),
                end: Some(b"m".to_vec()),
            },
            ownership_epoch: parent_epoch + 1,
        },
    }
}

fn split_artifact(plan: &SplitPlan, cutover_seq: u64) -> SplitArtifact {
    let child = |spec: &SplitChild, low| PreparedChildArtifact {
        partition_id: spec.partition_id,
        range: spec.range.clone(),
        ownership_epoch: spec.ownership_epoch,
        tree_manifest: cutover_seq + low,
        stream_name: StreamName { high: 90, low },
        applied_seq: cutover_seq,
    };
    SplitArtifact {
        transition_id: plan.transition_id,
        parent_id: plan.parent_id,
        parent_epoch: plan.parent_epoch,
        cutover_seq,
        left: child(&plan.left, 1),
        right: child(&plan.right, 2),
    }
}

async fn partition(
    store: &Arc<MemoryStreamStore>,
    tree: Arc<MemoryPartitionTree>,
    stream_name: StreamName,
    epoch: u64,
    config: PartitionConfig,
) -> Partition {
    let binding = StreamBinding {
        stream_name,
        metadata_group_id: 7,
        binding_generation: 1,
        state: StreamBindingState::Active,
        owner_kind: Some("chunk-kv-partition".into()),
    };
    let registry: Arc<dyn StreamRegistry> = store.clone();
    let metadata: Arc<dyn StreamMetadataStore> = store.clone();
    let chunks: Arc<dyn StreamChunkStore> = store.clone();
    let stream = ChunkStream::create(
        binding,
        epoch,
        StreamConfig::default(),
        registry,
        metadata,
        chunks,
    )
    .await
    .unwrap();
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name));
    let tree: Arc<dyn PartitionTree> = tree;
    Partition::open(
        PartitionId {
            high: stream_name.high,
            low: stream_name.low,
        },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        epoch,
        config,
        tree,
        journal,
    )
    .unwrap()
}

#[tokio::test]
async fn pending_mutation_is_invisible_until_durable_and_applied() {
    let store = Arc::new(MemoryStreamStore::new(1_024));
    let tree = Arc::new(MemoryPartitionTree::default());
    let partition = partition(
        &store,
        tree,
        StreamName { high: 1, low: 1 },
        4,
        PartitionConfig::default(),
    )
    .await;
    partition
        .mutate(
            4,
            request(1),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"old".to_vec(),
            },
        )
        .await
        .unwrap();

    store.pause_writes();
    let writer = partition.clone();
    let pending = tokio::spawn(async move {
        writer
            .mutate(
                4,
                request(2),
                MutationOperation::Put {
                    key: b"key".to_vec(),
                    value: b"new".to_vec(),
                },
            )
            .await
    });
    store.wait_for_write().await;
    assert_eq!(
        partition.get(4, b"key", None).await.unwrap().unwrap().value,
        b"old"
    );
    store.resume_writes();
    let response = pending.await.unwrap().unwrap();
    assert_eq!(
        partition
            .get(4, b"key", Some(response.journal_position))
            .await
            .unwrap()
            .unwrap()
            .value,
        b"new"
    );
}

#[tokio::test]
async fn same_batch_conditions_observe_preceding_staged_mutations() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 2, low: 2 },
        5,
        PartitionConfig::default(),
    )
    .await;
    store.pause_writes();
    let blocker_partition = partition.clone();
    let blocker = tokio::spawn(async move {
        blocker_partition
            .mutate(
                5,
                request(10),
                MutationOperation::Put {
                    key: b"b".to_vec(),
                    value: b"block".to_vec(),
                },
            )
            .await
    });
    store.wait_for_write().await;

    let first_partition = partition.clone();
    let first = tokio::spawn(async move {
        first_partition
            .mutate(
                5,
                request(11),
                MutationOperation::PutIfAbsent {
                    key: b"k".to_vec(),
                    value: b"one".to_vec(),
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    let second_partition = partition.clone();
    let second = tokio::spawn(async move {
        second_partition
            .mutate(
                5,
                request(12),
                MutationOperation::PutIfAbsent {
                    key: b"k".to_vec(),
                    value: b"two".to_vec(),
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    store.resume_writes();
    blocker.await.unwrap().unwrap();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert!(matches!(first.result, MutationResult::Applied { .. }));
    assert!(matches!(
        second.result,
        MutationResult::ConditionFailed {
            observed: Some(ref value)
        } if value.value == b"one" && value.revision == first.mutation_seq
    ));
    assert_eq!(second.mutation_seq, first.mutation_seq + 1);
    assert_eq!(partition.get(5, b"k", None).await.unwrap().unwrap().value, b"one");
}

#[tokio::test]
async fn retry_returns_original_result_and_digest_conflict_does_no_io() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 3, low: 3 },
        6,
        PartitionConfig::default(),
    )
    .await;
    let operation = MutationOperation::CompareExchange {
        key: b"k".to_vec(),
        condition: CompareCondition::Revision(99),
        value: b"never".to_vec(),
    };
    let original = partition.mutate(6, request(20), operation.clone()).await.unwrap();
    assert!(matches!(
        original.result,
        MutationResult::ConditionFailed { observed: None }
    ));
    partition
        .mutate(
            6,
            request(21),
            MutationOperation::Put {
                key: b"k".to_vec(),
                value: b"later".to_vec(),
            },
        )
        .await
        .unwrap();
    let writes = store.chunk_write_count();
    assert_eq!(
        partition.mutate(6, request(20), operation).await.unwrap(),
        original
    );
    assert_eq!(store.chunk_write_count(), writes);
    assert_eq!(
        partition
            .mutate(6, request(20), MutationOperation::Delete { key: b"k".to_vec() })
            .await,
        Err(ChunkKvError::RequestConflict)
    );
    assert_eq!(store.chunk_write_count(), writes);
}

#[tokio::test]
async fn range_and_epoch_reject_before_journaling() {
    let store = Arc::new(MemoryStreamStore::new(1_024));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 4, low: 4 },
        7,
        PartitionConfig::default(),
    )
    .await;
    let writes = store.chunk_write_count();
    assert_eq!(
        partition
            .mutate(
                6,
                request(30),
                MutationOperation::Put {
                    key: b"b".to_vec(),
                    value: b"v".to_vec()
                }
            )
            .await,
        Err(ChunkKvError::StaleEpoch)
    );
    assert_eq!(
        partition
            .mutate(
                7,
                request(31),
                MutationOperation::Put {
                    key: b"m".to_vec(),
                    value: b"v".to_vec()
                }
            )
            .await,
        Err(ChunkKvError::OutOfRange)
    );
    assert_eq!(store.chunk_write_count(), writes);
}

#[tokio::test]
async fn journal_uncertainty_stalls_only_writes_and_keeps_applied_reads() {
    let store = Arc::new(MemoryStreamStore::new(1_024));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 5, low: 5 },
        8,
        PartitionConfig::default(),
    )
    .await;
    partition
        .mutate(
            8,
            request(40),
            MutationOperation::Put {
                key: b"k".to_vec(),
                value: b"safe".to_vec(),
            },
        )
        .await
        .unwrap();
    store.queue_cursor_outcome(CursorAdvance::Ambiguous, false).await;
    assert_eq!(
        partition
            .mutate(
                8,
                request(41),
                MutationOperation::Put {
                    key: b"k".to_vec(),
                    value: b"unsafe".to_vec(),
                },
            )
            .await,
        Err(ChunkKvError::WriteStalled)
    );
    assert_eq!(
        partition.snapshot().lifecycle,
        crowdb_chunk_kv::PartitionLifecycle::WriteStalled
    );
    assert_eq!(
        partition.get(8, b"k", None).await.unwrap().unwrap().value,
        b"safe"
    );
}

#[tokio::test]
async fn recovery_replays_recorded_results_and_restores_deduplication() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream_name = StreamName { high: 6, low: 6 };
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        stream_name,
        9,
        PartitionConfig::default(),
    )
    .await;
    let operation = MutationOperation::PutIfAbsent {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
    };
    let original = partition.mutate(9, request(50), operation.clone()).await.unwrap();
    partition
        .mutate(
            9,
            request(51),
            MutationOperation::PutIfAbsent {
                key: b"key".to_vec(),
                value: b"other".to_vec(),
            },
        )
        .await
        .unwrap();
    drop(partition);
    tokio::task::yield_now().await;

    let registry: Arc<dyn StreamRegistry> = store.clone();
    let metadata: Arc<dyn StreamMetadataStore> = store.clone();
    let chunks: Arc<dyn StreamChunkStore> = store.clone();
    let stream = ChunkStream::open(
        stream_name,
        9,
        StreamConfig::default(),
        registry,
        metadata,
        chunks,
    )
    .await
    .unwrap();
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name));
    let recovered = Partition::recover(
        PartitionId { high: 6, low: 6 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        9,
        Checkpoint {
            tree_manifest: 0,
            applied_seq: 0,
            stream_name,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::default()),
        journal,
    )
    .await
    .unwrap();
    assert_eq!(recovered.snapshot().applied_seq, 2);
    assert_eq!(
        recovered.get(9, b"key", None).await.unwrap().unwrap().value,
        b"value"
    );
    let writes = store.chunk_write_count();
    assert_eq!(
        recovered.mutate(9, request(50), operation).await.unwrap(),
        original
    );
    assert_eq!(store.chunk_write_count(), writes);
}

#[tokio::test]
async fn mutation_fence_drains_admitted_work_and_rejects_later_writes() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    store.pause_writes();
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 7, low: 7 },
        10,
        PartitionConfig::default(),
    )
    .await;
    let writer_partition = partition.clone();
    let writer = tokio::spawn(async move {
        writer_partition
            .mutate(
                10,
                request(60),
                MutationOperation::Put {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                },
            )
            .await
    });
    store.wait_for_write().await;
    let fence_partition = partition.clone();
    let fence = tokio::spawn(async move { fence_partition.fence_mutations(10).await });
    tokio::task::yield_now().await;
    assert_eq!(
        partition
            .mutate(
                10,
                request(61),
                MutationOperation::Delete { key: b"key".to_vec() }
            )
            .await,
        Err(ChunkKvError::NotServing("SplitFenced".into()))
    );
    assert!(!fence.is_finished());
    store.resume_writes();
    writer.await.unwrap().unwrap();
    fence.await.unwrap().unwrap();

    let checkpoint = partition.checkpoint_fenced(10).await.unwrap();
    assert_eq!(checkpoint.applied_seq, 1);
    assert_eq!(checkpoint.tree_manifest, 1);
    assert_eq!(checkpoint.replay_offset, 0);
    partition
        .trim_published_checkpoint(10, &checkpoint)
        .await
        .unwrap();
}

#[tokio::test]
async fn manager_hosts_more_than_one_independent_partition() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let first = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 8, low: 1 },
        11,
        PartitionConfig::default(),
    )
    .await;
    let second = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 8, low: 2 },
        12,
        PartitionConfig::default(),
    )
    .await;
    let manager = PartitionManager::new(8).unwrap();
    manager.insert(first.clone()).await.unwrap();
    manager.insert(second.clone()).await.unwrap();
    assert_eq!(manager.len().await, 2);
    assert_ne!(first.snapshot().stream_name, second.snapshot().stream_name);

    first
        .mutate(
            11,
            request(70),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"first".to_vec(),
            },
        )
        .await
        .unwrap();
    second
        .mutate(
            12,
            request(71),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"second".to_vec(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        first.get(11, b"key", None).await.unwrap().unwrap().value,
        b"first"
    );
    assert_eq!(
        second.get(12, b"key", None).await.unwrap().unwrap().value,
        b"second"
    );
}

#[tokio::test]
async fn evicted_request_identity_returns_expired_instead_of_reexecuting() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 9, low: 9 },
        13,
        PartitionConfig {
            retained_results: 1,
            ..PartitionConfig::default()
        },
    )
    .await;
    let first = MutationOperation::Put {
        key: b"key".to_vec(),
        value: b"first".to_vec(),
    };
    partition.mutate(13, request(80), first.clone()).await.unwrap();
    partition
        .mutate(
            13,
            request(81),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"second".to_vec(),
            },
        )
        .await
        .unwrap();
    let writes = store.chunk_write_count();
    assert_eq!(
        partition.mutate(13, request(80), first).await,
        Err(ChunkKvError::RequestExpired)
    );
    assert_eq!(store.chunk_write_count(), writes);
}

#[tokio::test]
async fn unknown_post_journal_apply_recovers_only_that_partition() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let failing_tree = Arc::new(MemoryPartitionTree::default());
    let failing = partition(
        &store,
        Arc::clone(&failing_tree),
        StreamName { high: 10, low: 1 },
        14,
        PartitionConfig::default(),
    )
    .await;
    let healthy = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 10, low: 2 },
        15,
        PartitionConfig::default(),
    )
    .await;
    failing_tree.fail_next_apply();
    assert_eq!(
        failing
            .mutate(
                14,
                request(90),
                MutationOperation::Put {
                    key: b"key".to_vec(),
                    value: b"uncertain".to_vec()
                }
            )
            .await,
        Err(ChunkKvError::ApplyStateUnknown)
    );
    assert_eq!(
        failing.snapshot().lifecycle,
        crowdb_chunk_kv::PartitionLifecycle::Recovering
    );
    assert_eq!(
        failing
            .mutate(
                14,
                request(91),
                MutationOperation::Delete { key: b"key".to_vec() }
            )
            .await,
        Err(ChunkKvError::Recovering)
    );
    healthy
        .mutate(
            15,
            request(92),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"healthy".to_vec(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        healthy.get(15, b"key", None).await.unwrap().unwrap().value,
        b"healthy"
    );
}

#[tokio::test]
async fn transfer_reuses_tree_and_stream_under_higher_epoch() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let tree = Arc::new(MemoryPartitionTree::default());
    let stream_name = StreamName { high: 11, low: 11 };
    let old = partition(
        &store,
        Arc::clone(&tree),
        stream_name,
        16,
        PartitionConfig::default(),
    )
    .await;
    old.mutate(
        16,
        request(100),
        MutationOperation::Put {
            key: b"key".to_vec(),
            value: b"before".to_vec(),
        },
    )
    .await
    .unwrap();
    old.fence_mutations(16).await.unwrap();
    let checkpoint = old.checkpoint_fenced(16).await.unwrap();

    let registry: Arc<dyn StreamRegistry> = store.clone();
    let metadata: Arc<dyn StreamMetadataStore> = store.clone();
    let chunks: Arc<dyn StreamChunkStore> = store.clone();
    let stream = ChunkStream::open(
        stream_name,
        17,
        StreamConfig::default(),
        registry,
        metadata,
        chunks,
    )
    .await
    .unwrap();
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name));
    let tree_for_new: Arc<dyn PartitionTree> = tree;
    let new = Partition::recover(
        PartitionId { high: 11, low: 11 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        17,
        checkpoint,
        PartitionConfig::default(),
        tree_for_new,
        journal,
    )
    .await
    .unwrap();
    assert_eq!(new.get(17, b"key", None).await.unwrap().unwrap().value, b"before");
    new.mutate(
        17,
        request(101),
        MutationOperation::Put {
            key: b"key".to_vec(),
            value: b"after".to_vec(),
        },
    )
    .await
    .unwrap();
    assert_eq!(new.get(17, b"key", None).await.unwrap().unwrap().value, b"after");
    assert_eq!(
        old.mutate(
            16,
            request(102),
            MutationOperation::Delete { key: b"key".to_vec() }
        )
        .await,
        Err(ChunkKvError::NotServing("SplitFenced".into()))
    );
}

#[tokio::test]
async fn split_control_is_idempotent_and_commits_only_an_exact_artifact() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream_name = StreamName { high: 12, low: 12 };
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        stream_name,
        18,
        PartitionConfig::default(),
    )
    .await;
    let plan = split_plan(PartitionId { high: 12, low: 12 }, 18);
    partition.begin_split(plan.clone()).await.unwrap();
    partition.begin_split(plan.clone()).await.unwrap();
    assert_eq!(
        partition.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::SplitPreparing
    );

    partition
        .mutate(
            18,
            request(110),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"during-prepare".to_vec(),
            },
        )
        .await
        .unwrap();
    partition.fence_split(plan.transition_id).await.unwrap();
    let artifact = split_artifact(&plan, 1);
    partition.record_split_artifact(artifact.clone()).await.unwrap();

    let mut wrong = artifact.clone();
    wrong.right.tree_manifest += 1;
    assert!(partition
        .commit_split(&SplitCommitProof {
            catalog_revision: 3,
            artifact: wrong,
        })
        .await
        .is_err());
    assert_eq!(
        partition.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::SplitFenced
    );
    partition
        .commit_split(&SplitCommitProof {
            catalog_revision: 4,
            artifact,
        })
        .await
        .unwrap();
    assert_eq!(
        partition.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::Retired
    );
}

#[tokio::test]
async fn split_abort_requires_exact_nonpublication_proof_before_resuming() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 13, low: 13 },
        19,
        PartitionConfig::default(),
    )
    .await;
    let plan = split_plan(PartitionId { high: 13, low: 13 }, 19);
    partition.begin_split(plan.clone()).await.unwrap();
    let mut wrong_transition = plan.transition_id;
    wrong_transition.low += 1;
    assert!(partition
        .abort_split(&SplitAbortProof {
            catalog_revision: 8,
            transition_id: wrong_transition,
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
        })
        .await
        .is_err());
    partition
        .abort_split(&SplitAbortProof {
            catalog_revision: 9,
            transition_id: plan.transition_id,
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
        })
        .await
        .unwrap();
    assert_eq!(
        partition.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::Serving
    );
    partition
        .mutate(
            19,
            request(120),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"resumed".to_vec(),
            },
        )
        .await
        .unwrap();
}
