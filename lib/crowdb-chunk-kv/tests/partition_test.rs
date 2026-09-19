// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    canonical_operation_digest, encode_frame, Checkpoint, ChunkKvError, CompareCondition, MutationOperation,
    MutationResult, Partition, PartitionConfig, PartitionId, PartitionJournal, PartitionManager,
    PartitionRange, PartitionTree, PreparedSplitWriterArtifact, RequestId, SplitAbortProof, SplitArtifact,
    SplitChild, SplitCommitProof, SplitPlan, SplitSessionTargets, SplitWriterTarget, StreamPartitionJournal,
    TransitionId, WalRecord,
};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkId, ChunkStream, CursorAdvance, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig,
    StreamMetadataStore, StreamName, StreamRegistry,
};
use crowdb_tree_ffi::{ChunkPageStoreOptions, ChunkRootCatalog, PageStore};

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
        parent_next_epoch: parent_epoch + 1,
        split_key: b"g".to_vec(),
        child: SplitChild {
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
    let child = |spec: &SplitChild, low| PreparedSplitWriterArtifact {
        partition_id: spec.partition_id,
        range: spec.range.clone(),
        ownership_epoch: spec.ownership_epoch,
        tree_id: 100 + low,
        tree_manifest: cutover_seq + low,
        root_manifest_generation: cutover_seq + low,
        stream_name: StreamName { high: 90, low },
        base_applied_seq: cutover_seq,
        parent_id: plan.parent_id,
        parent_epoch: plan.parent_epoch,
        parent_stream_name: StreamName { high: 89, low: 1 },
        parent_stream_manifest_generation: 1,
        parent_replay_offset: 0,
        parent_cutover_offset: cutover_seq,
        applied_seq: cutover_seq,
        child_stream_start_seq: cutover_seq + 1,
    };
    SplitArtifact {
        transition_id: plan.transition_id,
        parent_id: plan.parent_id,
        parent_epoch: plan.parent_epoch,
        parent_next_epoch: plan.parent_next_epoch,
        shared_view_generation: 0,
        cutover_seq,
        retained_parent: child(
            &SplitChild {
                partition_id: plan.parent_id,
                range: plan.parent_range.split(&plan.split_key).unwrap().0,
                ownership_epoch: plan.parent_next_epoch,
            },
            1,
        ),
        child: child(&plan.child, 2),
    }
}

fn empty_prepared_child(
    partition_id: PartitionId,
    range: PartitionRange,
    tree_id: u64,
    stream_name: StreamName,
) -> PreparedSplitWriterArtifact {
    PreparedSplitWriterArtifact {
        partition_id,
        range,
        ownership_epoch: 20,
        tree_id,
        tree_manifest: 0,
        root_manifest_generation: 1,
        stream_name,
        base_applied_seq: 0,
        parent_id: PartitionId { high: 10, low: 1 },
        parent_epoch: 19,
        parent_stream_name: StreamName { high: 12, low: 11 },
        parent_stream_manifest_generation: 1,
        parent_replay_offset: 0,
        parent_cutover_offset: 0,
        applied_seq: 0,
        child_stream_start_seq: 1,
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

async fn empty_journal(
    store: &Arc<MemoryStreamStore>,
    stream_name: StreamName,
    epoch: u64,
) -> Arc<dyn PartitionJournal> {
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        epoch,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store.clone() as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    Arc::new(StreamPartitionJournal::new(stream, stream_name))
}

async fn append_record(journal: &dyn PartitionJournal, record: &WalRecord) {
    journal
        .append_frames(&[Bytes::from(encode_frame(record).unwrap())])
        .await
        .unwrap();
}

#[tokio::test]
async fn split_ingress_routes_old_parent_requests_to_both_new_writers() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let parent_id = PartitionId { high: 700, low: 1 };
    let parent_range = PartitionRange {
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
    };
    let parent = Partition::open(
        parent_id,
        parent_range.clone(),
        1,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(700)),
        empty_journal(&store, StreamName { high: 700, low: 1 }, 1).await,
    )
    .unwrap();
    let plan = SplitPlan {
        transition_id: TransitionId { high: 700, low: 2 },
        parent_id,
        parent_range,
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 700, low: 3 },
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: Some(b"z".to_vec()),
            },
            ownership_epoch: 2,
        },
    };
    parent.begin_split(plan.clone()).await.unwrap();
    let retained = Partition::open(
        parent_id,
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        2,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(701)),
        empty_journal(&store, StreamName { high: 700, low: 4 }, 2).await,
    )
    .unwrap();
    let child = Partition::open(
        plan.child.partition_id,
        plan.child.range.clone(),
        2,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(702)),
        empty_journal(&store, StreamName { high: 700, low: 5 }, 2).await,
    )
    .unwrap();
    parent
        .install_split_ingress(retained.clone(), child.clone())
        .await
        .unwrap();

    parent
        .mutate(
            1,
            request(700),
            MutationOperation::Put {
                key: b"b".to_vec(),
                value: b"left".to_vec(),
            },
        )
        .await
        .unwrap();
    parent
        .mutate(
            1,
            request(701),
            MutationOperation::Put {
                key: b"t".to_vec(),
                value: b"right".to_vec(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        retained.get(2, b"b", None).await.unwrap().unwrap().value,
        b"left"[..]
    );
    assert_eq!(
        child.get(2, b"t", None).await.unwrap().unwrap().value,
        b"right"[..]
    );
    assert_eq!(
        parent.get(1, b"b", None).await.unwrap().unwrap().value,
        b"left"[..]
    );
    assert_eq!(
        parent.get(1, b"t", None).await.unwrap().unwrap().value,
        b"right"[..]
    );
}

fn chunk_page_store(tree_id: u64, owner_epoch: u64) -> Arc<PageStore> {
    let catalog = Arc::new(ChunkRootCatalog::open_memory(owner_epoch).unwrap());
    Arc::new(
        PageStore::open_chunk(
            ChunkPageStoreOptions {
                tree_id,
                owner_epoch,
                open_generation: 0,
                pack_bytes: 4_096,
                iu_size: 1,
                max_concurrent_packs: 2,
                materialization_bytes_per_pass: 4_096,
            },
            catalog,
            None,
        )
        .unwrap(),
    )
}

async fn finish_materialization(partition: &Partition, ownership_epoch: u64) -> u64 {
    let mut passes = 0;
    loop {
        let progress = partition
            .materialize_split_ownership(ownership_epoch)
            .await
            .unwrap();
        passes += 1;
        if progress.complete {
            return passes;
        }
        assert!(progress.bytes_written > 0);
        assert!(passes < 8);
    }
}

async fn assert_prepared_tree_identity_mismatch(
    artifact: &PreparedSplitWriterArtifact,
    journal: Arc<dyn PartitionJournal>,
) {
    let result = Partition::recover_prepared(
        artifact.clone(),
        Checkpoint {
            tree_id: artifact.tree_id + 1,
            tree_manifest: artifact.tree_manifest,
            root_manifest_generation: artifact.root_manifest_generation,
            applied_seq: artifact.applied_seq,
            stream_name: artifact.stream_name,
            stream_manifest_generation: 1,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(artifact.tree_id)),
        journal,
    )
    .await;
    assert!(matches!(result, Err(ChunkKvError::InvalidRequest(_))));
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
async fn native_partition_constructor_owns_tree_and_stream_storage() {
    let stream_name = StreamName { high: 1, low: 9 };
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        4,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    let partition = Partition::open_native(
        PartitionId { high: 1, low: 9 },
        PartitionRange {
            start: Some(Vec::new()),
            end: Some(b"m".to_vec()),
        },
        4,
        PartitionConfig::default(),
        44,
        crowdb_tree_ffi::Config::default(),
        Arc::new(crowdb_tree_ffi::PageStore::open_mem(1).unwrap()),
        stream,
    )
    .unwrap();
    partition
        .mutate(
            4,
            request(8),
            MutationOperation::Put {
                key: Vec::new(),
                value: b"lower-bound".to_vec(),
            },
        )
        .await
        .unwrap();
    partition
        .mutate(
            4,
            request(9),
            MutationOperation::Put {
                key: b"b".to_vec(),
                value: b"native".to_vec(),
            },
        )
        .await
        .unwrap();
    let page = partition
        .scan_forward(4, None, None, 8, 1024, None)
        .await
        .unwrap();
    assert_eq!(page.entries[0].key.as_ref(), b"");
    assert_eq!(
        partition.get(4, b"b", None).await.unwrap().unwrap().value,
        b"native"
    );
    partition.suspend_for_transfer(4).await.unwrap();
    assert_eq!(partition.checkpoint_quiesced(4).await.unwrap().tree_id, 44);
}

#[tokio::test]
async fn native_partition_reopens_the_latest_tree_root() {
    let page_store = Arc::new(crowdb_tree_ffi::PageStore::open_mem(1).unwrap());
    let tree = crowdb_tree_ffi::Crowdbtree::open(&crowdb_tree_ffi::Config {
        page_store: Some(Arc::clone(&page_store)),
        key_range: crowdb_tree_ffi::KeyRange::Bounded {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        ..crowdb_tree_ffi::Config::default()
    })
    .unwrap();
    tree.apply_put(1, b"b", b"persisted").unwrap();
    tree.flush().unwrap();
    tree.snapshot_info().unwrap();
    drop(tree);

    let stream_name = StreamName { high: 1, low: 10 };
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        4,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    let partition = Partition::recover_native_latest_prepared_assignment(
        PartitionId { high: 1, low: 10 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        4,
        44,
        PartitionConfig::default(),
        crowdb_tree_ffi::Config::default(),
        page_store,
        stream,
    )
    .await
    .unwrap();
    partition.activate_recovered(4).unwrap();
    assert_eq!(
        partition.get(4, b"b", None).await.unwrap().unwrap().value,
        b"persisted"
    );
}

#[tokio::test]
async fn chunk_root_checkpoint_supplies_the_wal_replay_offset() {
    let stream_name = StreamName { high: 1, low: 11 };
    let stream_store = Arc::new(MemoryStreamStore::new(4_096));
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        4,
        StreamConfig::default(),
        stream_store.clone() as Arc<dyn StreamRegistry>,
        stream_store.clone() as Arc<dyn StreamMetadataStore>,
        stream_store.clone() as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    let catalog = Arc::new(ChunkRootCatalog::open_memory(4).unwrap());
    let options = ChunkPageStoreOptions {
        tree_id: 45,
        owner_epoch: 4,
        open_generation: 0,
        pack_bytes: 4_096,
        iu_size: 1,
        max_concurrent_packs: 2,
        materialization_bytes_per_pass: 4_096,
    };
    let page_store = Arc::new(PageStore::open_chunk(options, Arc::clone(&catalog), None).unwrap());
    let config = PartitionConfig {
        retained_results: 1,
        ..PartitionConfig::default()
    };
    let partition = Partition::open_native(
        PartitionId { high: 1, low: 11 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        4,
        config.clone(),
        45,
        crowdb_tree_ffi::Config::default(),
        Arc::clone(&page_store),
        stream,
    )
    .unwrap();
    for sequence in [1, 2] {
        partition
            .mutate(
                4,
                request(sequence),
                MutationOperation::Put {
                    key: b"b".to_vec(),
                    value: sequence.to_string().into_bytes(),
                },
            )
            .await
            .unwrap();
    }
    partition.suspend_for_transfer(4).await.unwrap();
    let checkpoint = partition.checkpoint_quiesced(4).await.unwrap();
    assert!(checkpoint.replay_offset > 0);
    assert_eq!(page_store.wal_replay_offset().unwrap(), checkpoint.replay_offset);
    drop(partition);

    let stream = ChunkStream::open(
        stream_name,
        4,
        StreamConfig::default(),
        stream_store.clone() as Arc<dyn StreamRegistry>,
        stream_store.clone() as Arc<dyn StreamMetadataStore>,
        stream_store as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    let page_store = Arc::new(PageStore::open_chunk(options, catalog, None).unwrap());
    let recovered = Partition::recover_native_latest_prepared_assignment(
        PartitionId { high: 1, low: 11 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        4,
        45,
        config,
        crowdb_tree_ffi::Config::default(),
        page_store,
        stream,
    )
    .await
    .unwrap();
    recovered.activate_recovered(4).unwrap();
    assert_eq!(recovered.get(4, b"b", None).await.unwrap().unwrap().value, b"2");
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
async fn forward_scan_is_bounded_and_clipped_to_the_partition() {
    let store = Arc::new(MemoryStreamStore::new(16_384));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 2, low: 20 },
        4,
        PartitionConfig::default(),
    )
    .await;
    for (sequence, key) in [(1, b"b".as_slice()), (2, b"c"), (3, b"d"), (4, b"e")] {
        partition
            .mutate(
                4,
                request(200 + sequence),
                MutationOperation::Put {
                    key: key.to_vec(),
                    value: key.to_vec(),
                },
            )
            .await
            .unwrap();
    }
    let page = partition
        .scan_forward(4, Some(b"a"), Some(b"z"), 2, 1024, None)
        .await
        .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.key.as_ref())
            .collect::<Vec<_>>(),
        vec![b"b".as_slice(), b"c".as_slice()]
    );
    assert!(page.truncated);
    assert_eq!(partition.metrics().snapshot().forward_scans, 1);
    assert_eq!(partition.metrics().snapshot().scan_entries, 2);

    let inclusive = partition
        .scan_forward(4, Some(b"b"), Some(b"d"), 10, 1024, None)
        .await
        .unwrap();
    assert_eq!(
        inclusive
            .entries
            .iter()
            .map(|entry| entry.key.as_ref())
            .collect::<Vec<_>>(),
        vec![b"b".as_slice(), b"c".as_slice()]
    );
    let continued = partition
        .scan_forward_after(4, b"b", Some(b"d"), 10, 1024, None)
        .await
        .unwrap();
    assert_eq!(
        continued
            .entries
            .iter()
            .map(|entry| entry.key.as_ref())
            .collect::<Vec<_>>(),
        vec![b"c".as_slice()]
    );

    let empty = partition
        .scan_forward(4, Some(b"z"), None, 10, 1024, None)
        .await
        .unwrap();
    assert!(empty.entries.is_empty());
    assert!(!empty.truncated);

    let reverse = partition
        .scan_reverse(4, Some(b"z"), Some(b"a"), 2, 1024, None)
        .await
        .unwrap();
    assert_eq!(
        reverse
            .entries
            .iter()
            .map(|entry| entry.key.as_ref())
            .collect::<Vec<_>>(),
        vec![b"e".as_slice(), b"d".as_slice()]
    );
    assert!(reverse.truncated);
    let continued = partition
        .scan_reverse(4, Some(b"d"), None, 10, 1024, None)
        .await
        .unwrap();
    assert_eq!(
        continued
            .entries
            .iter()
            .map(|entry| entry.key.as_ref())
            .collect::<Vec<_>>(),
        vec![b"c".as_slice(), b"b".as_slice()]
    );
    assert_eq!(partition.metrics().snapshot().reverse_scans, 2);
    assert_eq!(partition.metrics().snapshot().scan_entries, 9);
}

#[tokio::test]
async fn ordered_seeks_return_nearest_key_from_one_tree_view() {
    let store = Arc::new(MemoryStreamStore::new(16_384));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 2, low: 21 },
        4,
        PartitionConfig::default(),
    )
    .await;
    for (sequence, key) in [(1, b"b".as_slice()), (2, b"d")] {
        partition
            .mutate(
                4,
                request(220 + sequence),
                MutationOperation::Put {
                    key: key.to_vec(),
                    value: key.to_vec(),
                },
            )
            .await
            .unwrap();
    }

    assert_eq!(
        partition.ceiling(4, b"b", None).await.unwrap().unwrap().key,
        b"b".as_slice()
    );
    assert_eq!(
        partition.higher(4, b"b", None).await.unwrap().unwrap().key,
        b"d".as_slice()
    );
    assert_eq!(
        partition.ceiling(4, b"c", None).await.unwrap().unwrap().key,
        b"d".as_slice()
    );
    assert!(partition.higher(4, b"d", None).await.unwrap().is_none());
    assert_eq!(partition.metrics().snapshot().forward_seeks, 4);
    assert_eq!(
        partition.floor(4, b"d", None).await.unwrap().unwrap().key,
        b"d".as_slice()
    );
    assert_eq!(
        partition.lower(4, b"d", None).await.unwrap().unwrap().key,
        b"b".as_slice()
    );
    assert_eq!(
        partition.floor(4, b"c", None).await.unwrap().unwrap().key,
        b"b".as_slice()
    );
    assert!(partition.lower(4, b"b", None).await.unwrap().is_none());
    assert_eq!(partition.metrics().snapshot().reverse_seeks, 4);
    assert_eq!(
        partition.ceiling(4, b"m", None).await,
        Err(ChunkKvError::OutOfRange)
    );
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
    let metrics = partition.metrics().snapshot();
    assert_eq!(metrics.mutation_requests, 2);
    assert_eq!(metrics.stale_epochs, 1);
    assert_eq!(metrics.range_rejects, 1);
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
    let recovered = Partition::recover_prepared_assignment(
        PartitionId { high: 6, low: 6 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        9,
        Checkpoint {
            tree_id: 1,
            tree_manifest: 0,
            root_manifest_generation: 1,
            applied_seq: 0,
            stream_name,
            stream_manifest_generation: 1,
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
        recovered.snapshot().lifecycle,
        crowdb_chunk_kv::PartitionLifecycle::Prepared
    );
    assert!(matches!(
        recovered.get(9, b"key", None).await,
        Err(ChunkKvError::NotServing(_))
    ));
    assert!(matches!(
        recovered
            .mutate(9, request(52), MutationOperation::Delete { key: b"key".to_vec() })
            .await,
        Err(ChunkKvError::NotServing(_))
    ));
    recovered.activate_recovered(9).unwrap();
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
async fn recovery_rejects_a_tree_root_other_than_the_checkpoint() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream_name = StreamName { high: 6, low: 7 };
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        stream_name,
        9,
        PartitionConfig::default(),
    )
    .await;
    drop(partition);

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
    let result = Partition::recover_prepared_assignment(
        PartitionId { high: 6, low: 7 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        9,
        Checkpoint {
            tree_id: 1,
            tree_manifest: 1,
            root_manifest_generation: 1,
            applied_seq: 0,
            stream_name,
            stream_manifest_generation: 1,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::default()),
        journal,
    )
    .await;
    assert!(matches!(result, Err(ChunkKvError::TreeCorruption(_))));
}

#[tokio::test]
async fn recovery_rejects_a_frame_bound_to_another_physical_chunk() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream_name = StreamName { high: 60, low: 60 };
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        stream_name,
        9,
        PartitionConfig::default(),
    )
    .await;
    partition
        .mutate(
            9,
            request(500),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await
        .unwrap();
    assert_eq!(partition.snapshot().applied_seq, 1);
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
    let durable_tail = stream.tail();
    store
        .flip_durable_byte(
            ChunkId { high: 0, low: 1 },
            usize::try_from(durable_tail - 1).unwrap(),
        )
        .await;
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name));
    let result = Partition::recover(
        PartitionId { high: 60, low: 60 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        9,
        Checkpoint {
            tree_id: 1,
            tree_manifest: 0,
            root_manifest_generation: 1,
            applied_seq: 0,
            stream_name,
            stream_manifest_generation: 1,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::default()),
        journal,
    )
    .await;
    assert!(matches!(result, Err(ChunkKvError::JournalCorruption(_))));
}

#[tokio::test]
async fn transfer_quiesce_drains_admitted_work_and_rejects_later_writes() {
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
    let fence = tokio::spawn(async move { fence_partition.suspend_for_transfer(10).await });
    tokio::task::yield_now().await;
    assert_eq!(
        partition
            .mutate(
                10,
                request(61),
                MutationOperation::Delete { key: b"key".to_vec() }
            )
            .await,
        Err(ChunkKvError::WriteStalled)
    );
    assert!(!fence.is_finished());
    store.resume_writes();
    writer.await.unwrap().unwrap();
    fence.await.unwrap().unwrap();

    let checkpoint = partition.checkpoint_quiesced(10).await.unwrap();
    assert_eq!(checkpoint.applied_seq, 1);
    assert_eq!(checkpoint.tree_manifest, 1);
    assert_eq!(checkpoint.replay_offset, 0);
    let mut unsafe_watermark = checkpoint.clone();
    unsafe_watermark.replay_offset = 1;
    assert!(matches!(
        partition.trim_published_checkpoint(10, &unsafe_watermark).await,
        Err(ChunkKvError::InvalidRequest(_))
    ));
    let reclaimed = partition
        .reclaim_published_checkpoint(10, &checkpoint)
        .await
        .unwrap();
    assert_eq!(reclaimed.tree_bytes, 0);
    assert_eq!(reclaimed.orphan_bytes, 0);
    let metrics = partition.metrics().snapshot();
    assert_eq!(metrics.checkpoints, 1);
    assert_eq!(metrics.reclaimed_tree_bytes, 0);
}

#[tokio::test]
async fn serving_checkpoint_captures_a_recoverable_tree_frontier() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 7, low: 8 },
        10,
        PartitionConfig::default(),
    )
    .await;
    partition
        .mutate(
            10,
            request(62),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await
        .unwrap();

    let checkpoint = partition.checkpoint(10).await.unwrap();
    assert_eq!(
        partition.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::Serving
    );
    assert_eq!(checkpoint.tree_manifest, checkpoint.applied_seq);
    assert_eq!(checkpoint.applied_seq, 1);
    assert_eq!(checkpoint.replay_offset, 0);
}

#[tokio::test]
async fn transition_generation_pin_suppresses_source_checkpoint_branching() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 7, low: 9 },
        10,
        PartitionConfig::default(),
    )
    .await;
    partition
        .mutate(
            10,
            request(63),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await
        .unwrap();
    let checkpoint = partition.checkpoint(10).await.unwrap();
    let transition = TransitionId { high: 70, low: 71 };

    partition
        .retain_generation_pin(transition, checkpoint.root_manifest_generation)
        .unwrap();
    assert!(matches!(
        partition.checkpoint(10).await,
        Err(ChunkKvError::SplitRetry(_))
    ));
    partition.release_generation_pin(transition).unwrap();
    assert!(partition.checkpoint(10).await.is_ok());
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
    old.suspend_for_transfer(16).await.unwrap();
    let checkpoint = old.checkpoint_quiesced(16).await.unwrap();

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
        Err(ChunkKvError::WriteStalled)
    );
}

#[tokio::test]
async fn prepared_child_serves_only_after_exact_catalog_proof() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream_name = StreamName { high: 12, low: 12 };
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        20,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name));
    let artifact = empty_prepared_child(
        PartitionId { high: 12, low: 1 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        1,
        stream_name,
    );
    assert_prepared_tree_identity_mismatch(&artifact, Arc::clone(&journal)).await;
    let prepared = Partition::recover_prepared(
        artifact.clone(),
        Checkpoint {
            tree_id: 1,
            tree_manifest: 0,
            root_manifest_generation: 1,
            applied_seq: 0,
            stream_name,
            stream_manifest_generation: 1,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::default()),
        journal,
    )
    .await
    .unwrap();
    assert_eq!(
        prepared.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::Prepared
    );
    assert!(matches!(
        prepared.get(20, b"b", None).await,
        Err(ChunkKvError::NotServing(_))
    ));

    let proof = SplitCommitProof {
        catalog_revision: 7,
        artifact: SplitArtifact {
            transition_id: TransitionId { high: 70, low: 71 },
            parent_id: PartitionId { high: 10, low: 1 },
            parent_epoch: 19,
            parent_next_epoch: 20,
            shared_view_generation: 0,
            cutover_seq: 0,
            retained_parent: artifact.clone(),
            child: artifact,
        },
    };
    prepared.activate_prepared(&proof).unwrap();
    assert_eq!(prepared.lifecycle(), crowdb_chunk_kv::PartitionLifecycle::Serving);
    prepared.activate_recovered(20).unwrap();
    prepared
        .mutate(
            20,
            request(700),
            MutationOperation::Put {
                key: b"b".to_vec(),
                value: b"ready".to_vec(),
            },
        )
        .await
        .unwrap();
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
    partition
        .begin_split_finalization(plan.transition_id)
        .await
        .unwrap();
    let artifact = split_artifact(&plan, 1);
    partition.record_split_artifact(artifact.clone()).await.unwrap();
    assert_eq!(
        partition.prepared_split_artifact(plan.transition_id).await,
        Some(artifact.clone())
    );

    let mut wrong = artifact.clone();
    wrong.child.tree_manifest += 1;
    assert!(partition
        .commit_split(&SplitCommitProof {
            catalog_revision: 3,
            artifact: wrong,
        })
        .await
        .is_err());
    assert_eq!(
        partition.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::SplitFinalizing
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
        crowdb_chunk_kv::PartitionLifecycle::Serving
    );
    assert_eq!(partition.snapshot().ownership_epoch, plan.parent_next_epoch);
    assert_eq!(
        partition.snapshot().range.end.as_deref(),
        Some(plan.split_key.as_slice())
    );
}

#[tokio::test]
async fn serving_grant_refresh_keeps_a_preparing_parent_active() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 14, low: 14 },
        20,
        PartitionConfig::default(),
    )
    .await;
    let plan = split_plan(PartitionId { high: 14, low: 14 }, 20);
    partition.begin_split(plan).await.unwrap();

    partition.activate_recovered(20).unwrap();

    assert_eq!(
        partition.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::SplitPreparing
    );
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn split_session_installs_two_live_writers_at_ingress_frontier() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let parent_tree = Arc::new(MemoryPartitionTree::with_tree_id(810));
    let parent = partition(
        &store,
        Arc::clone(&parent_tree),
        StreamName { high: 810, low: 1 },
        8,
        PartitionConfig::default(),
    )
    .await;
    parent
        .mutate(
            8,
            request(810),
            MutationOperation::Put {
                key: b"b".to_vec(),
                value: b"left-base".to_vec(),
            },
        )
        .await
        .unwrap();
    parent
        .mutate(
            8,
            request(811),
            MutationOperation::Put {
                key: b"h".to_vec(),
                value: b"right-base".to_vec(),
            },
        )
        .await
        .unwrap();
    let plan = split_plan(PartitionId { high: 810, low: 1 }, 8);
    let prepared = parent
        .prepare_split_session(
            plan.clone(),
            SplitSessionTargets {
                retained_parent: SplitWriterTarget {
                    tree_id: 811,
                    tree_config: crowdb_tree_ffi::Config::default(),
                    journal: empty_journal(&store, StreamName { high: 811, low: 1 }, 9).await,
                },
                child: SplitWriterTarget {
                    tree_id: 812,
                    tree_config: crowdb_tree_ffi::Config::default(),
                    journal: empty_journal(&store, StreamName { high: 812, low: 1 }, 9).await,
                },
            },
            8,
        )
        .await
        .unwrap();
    assert_eq!(prepared.artifact.retained_parent.tree_id, 811);
    assert_eq!(prepared.artifact.child.tree_id, 812);
    assert_ne!(prepared.artifact.shared_view_generation, 0);
    assert_eq!(prepared.artifact.retained_parent.applied_seq, 2);
    assert_eq!(prepared.artifact.child.applied_seq, 2);
    assert_eq!(parent.metrics().snapshot().split_finalizations, 1);

    let artifact = prepared.artifact.clone();
    let retained = prepared
        .retained_parent
        .unwrap()
        .open_warmed(PartitionConfig::default())
        .unwrap();
    let child = prepared.child.open_warmed(PartitionConfig::default()).unwrap();
    retained.activate_local_split_writer(&artifact).unwrap();
    child.activate_local_split_writer(&artifact).unwrap();
    parent
        .install_split_ingress(retained.clone(), child.clone())
        .await
        .unwrap();
    parent
        .mutate(
            8,
            request(812),
            MutationOperation::Put {
                key: b"c".to_vec(),
                value: b"left-new".to_vec(),
            },
        )
        .await
        .unwrap();
    parent
        .mutate(
            8,
            request(813),
            MutationOperation::Put {
                key: b"i".to_vec(),
                value: b"right-new".to_vec(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        retained.get(9, b"b", None).await.unwrap().unwrap().value,
        b"left-base"[..]
    );
    assert_eq!(
        child.get(9, b"h", None).await.unwrap().unwrap().value,
        b"right-base"[..]
    );
    assert_eq!(
        retained.get(9, b"c", None).await.unwrap().unwrap().value,
        b"left-new"[..]
    );
    assert_eq!(
        child.get(9, b"i", None).await.unwrap().unwrap().value,
        b"right-new"[..]
    );
}

#[tokio::test]
async fn split_session_replays_existing_writer_journals_on_retry() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let parent_id = PartitionId { high: 815, low: 1 };
    let parent = partition(
        &store,
        Arc::new(MemoryPartitionTree::with_tree_id(815)),
        StreamName { high: 815, low: 1 },
        8,
        PartitionConfig::default(),
    )
    .await;
    for (sequence, key) in [(1, b"b".as_slice()), (2, b"h".as_slice())] {
        parent
            .mutate(
                8,
                request(815 + sequence),
                MutationOperation::Put {
                    key: key.to_vec(),
                    value: b"before-cutover".to_vec(),
                },
            )
            .await
            .unwrap();
    }
    let plan = split_plan(parent_id, 8);
    let retained_journal = empty_journal(&store, StreamName { high: 816, low: 1 }, 9).await;
    let child_journal = empty_journal(&store, StreamName { high: 817, low: 1 }, 9).await;
    for (journal, partition_id, request_id, key, value) in [
        (
            retained_journal.as_ref(),
            parent_id,
            request(818),
            b"c".as_slice(),
            b"retained-after-cutover".as_slice(),
        ),
        (
            child_journal.as_ref(),
            plan.child.partition_id,
            request(819),
            b"i".as_slice(),
            b"child-after-cutover".as_slice(),
        ),
    ] {
        let operation = MutationOperation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        append_record(
            journal,
            &WalRecord {
                partition_id,
                ownership_epoch: 9,
                mutation_seq: 3,
                request_id,
                operation_digest: canonical_operation_digest(&operation),
                result: MutationResult::Applied { revision: 3 },
                operation,
            },
        )
        .await;
    }

    let prepared = parent
        .prepare_split_session(
            plan,
            SplitSessionTargets {
                retained_parent: SplitWriterTarget {
                    tree_id: 816,
                    tree_config: crowdb_tree_ffi::Config::default(),
                    journal: retained_journal,
                },
                child: SplitWriterTarget {
                    tree_id: 817,
                    tree_config: crowdb_tree_ffi::Config::default(),
                    journal: child_journal,
                },
            },
            8,
        )
        .await
        .unwrap();

    assert_eq!(prepared.artifact.cutover_seq, 2);
    let ingress = parent.split_ingress().unwrap();
    assert_eq!(ingress.retained_parent().snapshot().applied_seq, 3);
    assert_eq!(ingress.child().snapshot().applied_seq, 3);
    assert_eq!(
        parent.get(8, b"c", None).await.unwrap().unwrap().value,
        b"retained-after-cutover"[..]
    );
    assert_eq!(
        parent.get(8, b"i", None).await.unwrap().unwrap().value,
        b"child-after-cutover"[..]
    );
}

#[tokio::test]
async fn split_routes_writes_before_shared_memtable_publish_finishes() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let parent_tree = Arc::new(MemoryPartitionTree::with_tree_id(820));
    let parent = partition(
        &store,
        Arc::clone(&parent_tree),
        StreamName { high: 820, low: 1 },
        8,
        PartitionConfig::default(),
    )
    .await;
    parent
        .mutate(
            8,
            request(820),
            MutationOperation::Put {
                key: b"h".to_vec(),
                value: b"before-cutover".to_vec(),
            },
        )
        .await
        .unwrap();
    parent_tree.pause_split_publish();
    let split_parent = parent.clone();
    let split_store = Arc::clone(&store);
    let split = tokio::spawn(async move {
        split_parent
            .prepare_split_session(
                split_plan(PartitionId { high: 820, low: 1 }, 8),
                SplitSessionTargets {
                    retained_parent: SplitWriterTarget {
                        tree_id: 821,
                        tree_config: crowdb_tree_ffi::Config::default(),
                        journal: empty_journal(&split_store, StreamName { high: 821, low: 1 }, 9).await,
                    },
                    child: SplitWriterTarget {
                        tree_id: 822,
                        tree_config: crowdb_tree_ffi::Config::default(),
                        journal: empty_journal(&split_store, StreamName { high: 822, low: 1 }, 9).await,
                    },
                },
                8,
            )
            .await
    });
    parent_tree.wait_for_split_publish().await;

    let conditional = parent
        .mutate(
            8,
            request(821),
            MutationOperation::PutIfAbsent {
                key: b"h".to_vec(),
                value: b"wrong".to_vec(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        conditional.result,
        MutationResult::ConditionFailed { .. }
    ));
    parent
        .mutate(
            8,
            request(822),
            MutationOperation::Put {
                key: b"i".to_vec(),
                value: b"child-live".to_vec(),
            },
        )
        .await
        .unwrap();
    parent
        .mutate(
            8,
            request(823),
            MutationOperation::Put {
                key: b"c".to_vec(),
                value: b"parent-live".to_vec(),
            },
        )
        .await
        .unwrap();

    parent_tree.resume_split_publish();
    let prepared = split.await.unwrap().unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 1);
    assert_eq!(
        parent.get(8, b"h", None).await.unwrap().unwrap().value,
        b"before-cutover"[..]
    );
    assert_eq!(
        parent.get(8, b"i", None).await.unwrap().unwrap().value,
        b"child-live"[..]
    );
    assert_eq!(
        parent.get(8, b"c", None).await.unwrap().unwrap().value,
        b"parent-live"[..]
    );
}

#[tokio::test]
async fn online_split_retains_parent_and_replays_child_serving_deltas() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let tree = Arc::new(MemoryPartitionTree::with_tree_id(90));
    let parent = partition(
        &store,
        Arc::clone(&tree),
        StreamName { high: 12, low: 12 },
        18,
        PartitionConfig::default(),
    )
    .await;
    for (sequence, key) in [(1, b"b".as_slice()), (2, b"h".as_slice())] {
        parent
            .mutate(
                18,
                request(sequence),
                MutationOperation::Put {
                    key: key.to_vec(),
                    value: key.to_vec(),
                },
            )
            .await
            .unwrap();
    }
    tree.pause_rebuild();
    let plan = split_plan(PartitionId { high: 12, low: 12 }, 18);
    let child_target = SplitWriterTarget {
        tree_id: 92,
        tree_config: crowdb_tree_ffi::Config::default(),
        journal: empty_journal(&store, StreamName { high: 92, low: 1 }, 19).await,
    };
    let split_parent = parent.clone();
    let split_plan = plan.clone();
    let split = tokio::spawn(async move { split_parent.prepare_split(split_plan, child_target, 8).await });
    tree.wait_for_rebuild().await;
    for (sequence, key) in [(3, b"d".as_slice()), (4, b"i".as_slice())] {
        parent
            .mutate(
                18,
                request(sequence),
                MutationOperation::Put {
                    key: key.to_vec(),
                    value: key.to_vec(),
                },
            )
            .await
            .unwrap();
    }
    tree.resume_rebuild();

    let prepared = split.await.unwrap().unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 4);
    assert_eq!(prepared.delta_records, 2);
    assert_eq!(
        parent.lifecycle(),
        crowdb_chunk_kv::PartitionLifecycle::SplitFinalizing
    );
    let metrics = parent.metrics().snapshot();
    assert_eq!(metrics.split_entries_examined, 2);
    assert_eq!(metrics.split_entries_emitted, 1);
    assert_eq!(metrics.split_delta_records, 2);
    assert!(metrics.split_tail_bytes > 0);
    assert!(metrics.split_preparation_duration_us > 0);
    assert!(metrics.split_catchup_lag_records <= 8);
    assert!(metrics.split_finalization_duration_us > 0);
    let proof = SplitCommitProof {
        catalog_revision: 7,
        artifact: prepared.artifact.clone(),
    };
    let child = prepared.child.open(PartitionConfig::default()).await.unwrap();
    child.activate_prepared(&proof).unwrap();
    assert_eq!(
        child.materialize_split_ownership(19).await.unwrap(),
        crowdb_chunk_kv::MaterializationProgress {
            bytes_written: 0,
            complete: true,
        }
    );
    for key in [b"h".as_slice(), b"i".as_slice()] {
        assert_eq!(child.get(19, key, None).await.unwrap().unwrap().value, key);
    }
    assert!(matches!(
        child.get(19, b"b", None).await,
        Err(ChunkKvError::OutOfRange)
    ));
    parent.commit_split(&proof).await.unwrap();
    assert_eq!(parent.lifecycle(), crowdb_chunk_kv::PartitionLifecycle::Serving);
    assert_eq!(parent.snapshot().ownership_epoch, 19);
    assert_eq!(parent.snapshot().range.end.as_deref(), Some(b"g".as_slice()));
    for key in [b"b".as_slice(), b"d".as_slice()] {
        assert_eq!(parent.get(19, key, None).await.unwrap().unwrap().value, key);
    }
}

#[tokio::test]
async fn native_online_split_rebuilds_one_child_from_the_exact_parent_manifest() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let parent_stream_name = StreamName { high: 120, low: 120 };
    let parent_stream = ChunkStream::create(
        StreamBinding {
            stream_name: parent_stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        18,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store.clone() as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    let parent = Partition::open_native(
        PartitionId { high: 120, low: 120 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        18,
        PartitionConfig::default(),
        190,
        crowdb_tree_ffi::Config::default(),
        chunk_page_store(190, 18),
        parent_stream,
    )
    .unwrap();
    for (sequence, key) in [(1, b"b".as_slice()), (2, b"h".as_slice())] {
        parent
            .mutate(
                18,
                request(sequence + 200),
                MutationOperation::Put {
                    key: key.to_vec(),
                    value: key.to_vec(),
                },
            )
            .await
            .unwrap();
    }
    let plan = SplitPlan {
        parent_id: PartitionId { high: 120, low: 120 },
        ..split_plan(PartitionId { high: 120, low: 120 }, 18)
    };
    let prepared = parent
        .prepare_split(
            plan,
            SplitWriterTarget {
                tree_id: 192,
                tree_config: crowdb_tree_ffi::Config {
                    page_store: Some(chunk_page_store(192, 19)),
                    ..crowdb_tree_ffi::Config::default()
                },
                journal: empty_journal(&store, StreamName { high: 192, low: 1 }, 19).await,
            },
            8,
        )
        .await
        .unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 2);
    assert!(prepared.child.artifact().tree_manifest > 0);
    assert_eq!(prepared.child_rebuild.entries_emitted, 1);
    let proof = SplitCommitProof {
        catalog_revision: 8,
        artifact: prepared.artifact.clone(),
    };
    let child = prepared.child.open(PartitionConfig::default()).await.unwrap();
    child.activate_prepared(&proof).unwrap();
    let passes = finish_materialization(&child, 19).await;
    assert_eq!(child.metrics().snapshot().materialization_passes, passes);
    assert_eq!(child.chunk_storage_stats().unwrap().unwrap().shared_packs, 0);
}

async fn overlay_base_tree(tree_id: u64) -> (Arc<MemoryPartitionTree>, u64) {
    let tree = Arc::new(MemoryPartitionTree::with_tree_id(tree_id));
    tree.apply(
        1,
        &MutationOperation::Put {
            key: b"b".to_vec(),
            value: b"base".to_vec(),
        },
    )
    .await
    .unwrap();
    tree.advance_noop(2).await.unwrap();
    let (manifest, applied) = tree.checkpoint(0).await.unwrap();
    assert_eq!(applied, 2);
    (tree, manifest)
}

struct OverlayFixture {
    store: Arc<MemoryStreamStore>,
    parent_journal: Arc<dyn PartitionJournal>,
    child_journal: Arc<dyn PartitionJournal>,
    base_tree: Arc<MemoryPartitionTree>,
    artifact: PreparedSplitWriterArtifact,
    checkpoint: Checkpoint,
    proof: SplitCommitProof,
    operations: Vec<MutationOperation>,
    responses: Vec<crowdb_chunk_kv::MutationResponse>,
}

fn overlay_operations() -> Vec<MutationOperation> {
    vec![
        MutationOperation::Put {
            key: b"b".to_vec(),
            value: b"base".to_vec(),
        },
        MutationOperation::Put {
            key: b"h".to_vec(),
            value: b"sibling".to_vec(),
        },
        MutationOperation::Put {
            key: b"d".to_vec(),
            value: b"tail".to_vec(),
        },
        MutationOperation::Put {
            key: b"i".to_vec(),
            value: b"sibling-tail".to_vec(),
        },
        MutationOperation::PutIfAbsent {
            key: b"b".to_vec(),
            value: b"ignored".to_vec(),
        },
    ]
}

async fn overlay_fixture() -> OverlayFixture {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let parent_name = StreamName { high: 130, low: 1 };
    let parent_journal = empty_journal(&store, parent_name, 18).await;
    let parent = Partition::open(
        PartitionId { high: 130, low: 1 },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        18,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(130)),
        Arc::clone(&parent_journal),
    )
    .unwrap();
    let operations = overlay_operations();
    let mut responses = Vec::new();
    for (index, operation) in operations.iter().cloned().enumerate() {
        responses.push(
            parent
                .mutate(18, request(index as u64 + 1), operation)
                .await
                .unwrap(),
        );
    }
    let child_name = StreamName { high: 131, low: 1 };
    let child_journal = empty_journal(&store, child_name, 19).await;
    let (base_tree, manifest) = overlay_base_tree(131).await;
    let artifact = PreparedSplitWriterArtifact {
        partition_id: PartitionId { high: 131, low: 1 },
        range: PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"g".to_vec()),
        },
        ownership_epoch: 19,
        tree_id: 131,
        tree_manifest: manifest,
        root_manifest_generation: manifest,
        stream_name: child_name,
        base_applied_seq: 2,
        parent_id: PartitionId { high: 130, low: 1 },
        parent_epoch: 18,
        parent_stream_name: parent_name,
        parent_stream_manifest_generation: parent_journal.manifest_generation(),
        parent_replay_offset: 0,
        parent_cutover_offset: parent_journal.tail(),
        applied_seq: 5,
        child_stream_start_seq: 6,
    };
    let checkpoint = Checkpoint {
        tree_id: 131,
        tree_manifest: manifest,
        root_manifest_generation: manifest,
        applied_seq: 2,
        stream_name: child_name,
        stream_manifest_generation: child_journal.manifest_generation(),
        replay_offset: 0,
    };
    let proof = SplitCommitProof {
        catalog_revision: 9,
        artifact: SplitArtifact {
            transition_id: TransitionId { high: 130, low: 9 },
            parent_id: artifact.parent_id,
            parent_epoch: artifact.parent_epoch,
            parent_next_epoch: artifact.parent_epoch + 1,
            shared_view_generation: 0,
            cutover_seq: artifact.applied_seq,
            retained_parent: artifact.clone(),
            child: artifact.clone(),
        },
    };
    OverlayFixture {
        store,
        parent_journal,
        child_journal,
        base_tree,
        artifact,
        checkpoint,
        proof,
        operations,
        responses,
    }
}

#[tokio::test]
async fn both_split_halves_recover_pre_split_keys_from_parent_overlay() {
    let OverlayFixture {
        store,
        parent_journal,
        child_journal: retained_journal,
        base_tree: retained_tree,
        artifact: retained_artifact,
        checkpoint: retained_checkpoint,
        ..
    } = overlay_fixture().await;
    let retained_artifact = PreparedSplitWriterArtifact {
        partition_id: retained_artifact.parent_id,
        ..retained_artifact
    };
    let child_stream_name = StreamName { high: 132, low: 1 };
    let child_journal = empty_journal(&store, child_stream_name, 19).await;
    let (child_tree, child_manifest) = overlay_base_tree(132).await;
    let child_artifact = PreparedSplitWriterArtifact {
        partition_id: PartitionId { high: 132, low: 1 },
        range: PartitionRange {
            start: Some(b"g".to_vec()),
            end: Some(b"m".to_vec()),
        },
        tree_id: 132,
        tree_manifest: child_manifest,
        root_manifest_generation: child_manifest,
        stream_name: child_stream_name,
        ..retained_artifact.clone()
    };
    let child_checkpoint = Checkpoint {
        tree_id: child_artifact.tree_id,
        tree_manifest: child_artifact.tree_manifest,
        root_manifest_generation: child_artifact.root_manifest_generation,
        applied_seq: child_artifact.base_applied_seq,
        stream_name: child_artifact.stream_name,
        stream_manifest_generation: child_journal.manifest_generation(),
        replay_offset: 0,
    };
    let proof = SplitCommitProof {
        catalog_revision: 10,
        artifact: SplitArtifact {
            transition_id: TransitionId { high: 130, low: 10 },
            parent_id: retained_artifact.parent_id,
            parent_epoch: retained_artifact.parent_epoch,
            parent_next_epoch: retained_artifact.ownership_epoch,
            shared_view_generation: 0,
            cutover_seq: retained_artifact.applied_seq,
            retained_parent: retained_artifact.clone(),
            child: child_artifact.clone(),
        },
    };

    let retained = Partition::recover_prepared_overlay(
        retained_artifact,
        retained_checkpoint,
        PartitionConfig::default(),
        retained_tree,
        retained_journal,
        Arc::clone(&parent_journal),
    )
    .await
    .unwrap();
    let child = Partition::recover_prepared_overlay(
        child_artifact,
        child_checkpoint,
        PartitionConfig::default(),
        child_tree,
        child_journal,
        parent_journal,
    )
    .await
    .unwrap();
    retained.activate_split_writer(&proof).unwrap();
    child.activate_split_writer(&proof).unwrap();

    assert_eq!(
        retained.get(19, b"d", None).await.unwrap().unwrap().value,
        b"tail"
    );
    assert_eq!(
        child.get(19, b"i", None).await.unwrap().unwrap().value,
        b"sibling-tail"
    );
    assert!(matches!(
        retained.get(19, b"i", None).await,
        Err(ChunkKvError::OutOfRange)
    ));
    assert!(matches!(
        child.get(19, b"d", None).await,
        Err(ChunkKvError::OutOfRange)
    ));
}

#[tokio::test]
async fn child_overlay_recovers_parent_results_then_its_own_wal() {
    let OverlayFixture {
        store: _,
        parent_journal,
        child_journal,
        base_tree,
        artifact,
        checkpoint,
        proof,
        operations,
        responses,
    } = overlay_fixture().await;
    let child = Partition::recover_prepared_overlay(
        artifact.clone(),
        checkpoint.clone(),
        PartitionConfig::default(),
        base_tree,
        Arc::clone(&child_journal),
        Arc::clone(&parent_journal),
    )
    .await
    .unwrap();
    let overlay_metrics = child.metrics().snapshot();
    assert_eq!(overlay_metrics.split_overlay_apply_records, 3);
    assert!(overlay_metrics.split_overlay_apply_bytes > 0);
    child.activate_prepared(&proof).unwrap();
    assert_eq!(child.get(19, b"d", None).await.unwrap().unwrap().value, b"tail");
    assert!(matches!(
        child.get(19, b"h", None).await,
        Err(ChunkKvError::OutOfRange)
    ));
    assert_eq!(
        child
            .get(19, b"d", Some(responses[2].journal_position))
            .await
            .unwrap()
            .unwrap()
            .value,
        b"tail"
    );
    assert_eq!(
        child.mutate(19, request(5), operations[4].clone()).await.unwrap(),
        responses[4]
    );
    let own = child
        .mutate(
            19,
            request(6),
            MutationOperation::Put {
                key: b"e".to_vec(),
                value: b"child".to_vec(),
            },
        )
        .await
        .unwrap();
    assert_eq!(own.mutation_seq, 6);
    assert_eq!(own.journal_position.stream_name, artifact.stream_name);

    let (cold_tree, _) = overlay_base_tree(131).await;
    let recovered = Partition::recover_prepared_overlay(
        artifact,
        checkpoint,
        PartitionConfig::default(),
        cold_tree,
        child_journal,
        parent_journal,
    )
    .await
    .unwrap();
    recovered.activate_prepared(&proof).unwrap();
    assert_eq!(recovered.snapshot().applied_seq, 6);
    assert_eq!(
        recovered.get(19, b"e", None).await.unwrap().unwrap().value,
        b"child"
    );
    assert_eq!(
        recovered
            .mutate(19, request(5), operations[4].clone())
            .await
            .unwrap(),
        responses[4]
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
