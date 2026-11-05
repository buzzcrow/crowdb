// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_storage.rs"]
mod split_storage;

#[path = "common/split_publish_tree.rs"]
mod split_publish_tree;

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

fn native_config(runtime: &Arc<crowdb_tree_ffi::TreeRuntime>) -> crowdb_tree_ffi::Config {
    // The 32 MiB fixture retains original pages through the split while the
    // checkpoint also holds its publication and page encoding workspace.
    let mut runtime_binding = crowdb_tree_ffi::TreeBindingOptions::default();
    runtime_binding.limits.reclaim_bytes = 128 * 1024 * 1024;
    crowdb_tree_ffi::Config {
        runtime: Some(runtime.clone()),
        runtime_binding,
        page_store: Some(Arc::new(crowdb_tree_ffi::PageStore::open_mem(65_536).unwrap())),
        ..crowdb_tree_ffi::Config::default()
    }
}

#[tokio::test]
async fn consecutive_native_splits_preserve_every_original_record() {
    let runtime =
        Arc::new(crowdb_tree_ffi::TreeRuntime::new(crowdb_tree_ffi::TreeRuntimeOptions::default()).unwrap());
    let storage = TestSplitStorage {
        store: Arc::new(crowdb_chunk_stream::memory::MemoryStreamStore::new(128 * 1024)),
    };
    let tree = Arc::new(CrowdbPartitionTree::open(80, &native_config(&runtime)).unwrap());
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
            tree_config: native_config(&runtime),
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
    native_child_publication(false, false).await;
}

#[tokio::test]
async fn native_publication_retry_retains_capture_and_new_child_writes() {
    native_child_publication(true, false).await;
}

#[tokio::test]
async fn native_release_retry_does_not_republish_after_child_writes() {
    native_child_publication(false, true).await;
}

struct TestNativeSplit {
    store: Arc<crowdb_chunk_stream::memory::MemoryStreamStore>,
    parent: crowdb_chunk_kv::Partition,
    publication: Arc<split_publish_tree::TestPublishTree>,
    gate: Arc<TestNativeHandoffGate>,
    plan: SplitPlan,
    preparing: tokio::task::JoinHandle<crowdb_chunk_kv::Result<crowdb_chunk_kv::PreparedSplit>>,
}

async fn setup_native_publication(
    fail_publish: bool,
    fail_release: bool,
    pause_prepare: bool,
) -> TestNativeSplit {
    // Each independent test has the default host budget. The parent and its
    // children share that host without competing with another test's workload.
    let runtime =
        Arc::new(crowdb_tree_ffi::TreeRuntime::new(crowdb_tree_ffi::TreeRuntimeOptions::default()).unwrap());
    let storage = TestSplitStorage::new();
    let parent_id = PartitionId { high: 60, low: 1 };
    let parent_tree = Arc::new(CrowdbPartitionTree::open(60, &native_config(&runtime)).unwrap());
    let publication = Arc::new(split_publish_tree::TestPublishTree::new(
        parent_tree,
        fail_publish,
        fail_release,
    ));
    if pause_prepare {
        publication.pause_preparation();
    }
    let (parent, _) = storage
        .partition(parent_id, PartitionRange::default(), 1, publication.clone())
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
        tree_config: native_config(&runtime),
        journal: storage
            .journal(crowdb_chunk_stream::StreamName { high: 61, low: 1 }, 1)
            .await,
    };
    let gate = Arc::new(TestNativeHandoffGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let source = parent.clone();
    let retry_plan = plan.clone();
    let waiting_gate = gate.clone();
    let preparing = tokio::spawn(async move {
        source
            .prepare_split_child_session(plan, target, 8, waiting_gate)
            .await
    });
    TestNativeSplit {
        store: storage.store,
        parent,
        publication,
        gate,
        plan: retry_plan,
        preparing,
    }
}

async fn native_child_publication(fail_publish: bool, fail_release: bool) {
    let TestNativeSplit {
        parent,
        publication,
        gate,
        plan: retry_plan,
        mut preparing,
        ..
    } = setup_native_publication(fail_publish, fail_release, false).await;
    tokio::select! {
        () = gate.entered.notified() => {},
        result = &mut preparing => match result {
            Ok(Err(error)) => panic!("native split failed before reaching the handoff gate: {error}; stage={}; calls={:?}", publication.stage(), publication.calls()),
            Err(error) => panic!("native split task failed before reaching the handoff gate: {error}; stage={}; calls={:?}", publication.stage(), publication.calls()),
            Ok(Ok(_)) => panic!("native split completed without entering the handoff gate"),
        },
    }
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
    tokio::select! {
        () = publication.entered.notified() => {},
        result = &mut preparing => match result {
            Ok(Err(error)) => panic!("split failed before inherited publication: {error}; stage={}; calls={:?}", publication.stage(), publication.calls()),
            Err(error) => panic!("split task failed before inherited publication: {error}; stage={}; calls={:?}", publication.stage(), publication.calls()),
            Ok(Ok(_)) => panic!("split completed without the publication gate"),
        },
    }
    for key in [b"n", b"o"] {
        assert_eq!(
            parent.get(1, key, None).await.unwrap().unwrap().value,
            b"during-status",
            "the child must read the inherited update while publication is paused"
        );
    }
    let (request, operation) = put(5, b"n", b"child-own-wal");
    let written = parent.mutate(1, request, operation).await.unwrap();
    let child_stream = parent.split_ingress().unwrap().child().snapshot().stream_name;
    assert_eq!(written.journal_position.stream_name, child_stream);
    publication.release.notify_one();
    verify_native_publication(
        &parent,
        &publication,
        retry_plan,
        preparing.await.unwrap(),
        fail_publish,
        fail_release,
    )
    .await;
}

async fn verify_native_publication(
    parent: &crowdb_chunk_kv::Partition,
    publication: &split_publish_tree::TestPublishTree,
    retry_plan: SplitPlan,
    attempt: crowdb_chunk_kv::Result<crowdb_chunk_kv::PreparedSplit>,
    fail_publish: bool,
    fail_release: bool,
) {
    let prepared = if fail_publish || fail_release {
        assert!(matches!(
            attempt,
            Err(crowdb_chunk_kv::ChunkKvError::SplitRetry(_))
        ));
        for (sequence, key, value) in [
            (6, b"o", b"after-error".as_slice()),
            (7, b"b", b"left-after-error".as_slice()),
        ] {
            let (request, operation) = put(sequence, key, value);
            parent.mutate(1, request, operation).await.unwrap();
        }
        parent
            .finalize_installed_split_child_session(retry_plan.clone())
            .await
            .unwrap()
    } else {
        attempt.unwrap()
    };
    assert_eq!(prepared.artifact.cutover_seq, 4);
    assert_eq!(prepared.artifact.retained_parent.tree_id, 60);
    let child = prepared
        .child
        .open_warmed(crowdb_chunk_kv::PartitionConfig::default())
        .unwrap();
    assert_eq!(
        child.get(1, b"n", None).await.unwrap().unwrap().value,
        b"child-own-wal"
    );
    assert_eq!(
        child.get(1, b"o", None).await.unwrap().unwrap().value,
        if fail_publish || fail_release {
            b"after-error".as_slice()
        } else {
            b"during-status".as_slice()
        }
    );
    assert_eq!(
        parent.get(1, b"b", None).await.unwrap().unwrap().value,
        if fail_publish || fail_release {
            b"left-after-error".as_slice()
        } else {
            b"during-status".as_slice()
        }
    );
    assert_eq!(
        publication.calls(),
        (
            0,
            if fail_publish { 2 } else { 1 },
            if fail_release { 2 } else { 1 }
        )
    );
    assert_eq!(publication.prepared_calls(), (1, 1));
    let repeated = parent
        .finalize_installed_split_child_session(retry_plan)
        .await
        .unwrap();
    assert_eq!(repeated.artifact, prepared.artifact);
    assert_eq!(
        publication.calls(),
        (
            0,
            if fail_publish { 2 } else { 1 },
            if fail_release { 2 } else { 1 }
        )
    );
}

#[tokio::test]
async fn native_overlay_preparation_keeps_parent_mutation_worker_serving() {
    let TestNativeSplit {
        parent,
        publication,
        gate,
        plan: _,
        mut preparing,
        ..
    } = setup_native_publication(false, false, true).await;
    tokio::select! {
        () = gate.entered.notified() => {},
        result = &mut preparing => panic!("failed before handoff: {}; stage={}; calls={:?}", preparation_failure(result), publication.stage(), publication.calls()),
    }
    gate.release.notify_one();
    tokio::select! {
        () = publication.prepare_entered.notified() => {},
        result = &mut preparing => panic!("failed before preparation: {}; stage={}; calls={:?}", preparation_failure(result), publication.stage(), publication.calls()),
    }
    let old_stream = parent.snapshot().stream_name;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        for (sequence, key) in [(2, b"n"), (3, b"o")] {
            let (request, operation) = put(sequence, key, b"during-prepare");
            let response = parent.mutate(1, request, operation).await.unwrap();
            assert_eq!(response.journal_position.stream_name, old_stream);
            assert_eq!(
                parent.get(1, key, None).await.unwrap().unwrap().value,
                b"during-prepare"
            );
        }
    })
    .await
    .unwrap();
    publication.prepare_release.notify_one();
    tokio::select! {
        () = publication.entered.notified() => {},
        result = &mut preparing => panic!("failed before publication: {}; stage={}; calls={:?}", preparation_failure(result), publication.stage(), publication.calls()),
    }
    for key in [b"n", b"o"] {
        assert_eq!(
            parent.get(1, key, None).await.unwrap().unwrap().value,
            b"during-prepare"
        );
    }
    let (request, operation) = put(4, b"n", b"child-own-wal");
    let response = parent.mutate(1, request, operation).await.unwrap();
    assert_ne!(response.journal_position.stream_name, old_stream);
    publication.release.notify_one();
    let prepared = preparing.await.unwrap().unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 3);
    assert_eq!(prepared.artifact.child.base_applied_seq, 1);
    assert_eq!(publication.prepared_calls(), (1, 1));
    assert_eq!(publication.calls(), (0, 1, 1));
    let child = prepared
        .child
        .open_warmed(crowdb_chunk_kv::PartitionConfig::default())
        .unwrap();
    assert_eq!(
        child.get(1, b"n", None).await.unwrap().unwrap().value,
        b"child-own-wal"
    );
    assert_eq!(
        child.get(1, b"o", None).await.unwrap().unwrap().value,
        b"during-prepare"
    );
}

fn preparation_failure(
    result: Result<crowdb_chunk_kv::Result<crowdb_chunk_kv::PreparedSplit>, tokio::task::JoinError>,
) -> String {
    match result {
        Ok(Err(error)) => error.to_string(),
        Err(error) => error.to_string(),
        Ok(Ok(_)) => "completed without reaching its gate".into(),
    }
}

#[tokio::test]
async fn native_route_and_child_wal_do_not_wait_for_parent_journal_publication() {
    let TestNativeSplit {
        store,
        parent,
        publication,
        gate,
        mut preparing,
        ..
    } = setup_native_publication(false, false, false).await;
    tokio::select! {
        () = gate.entered.notified() => {},
        result = &mut preparing => panic!("failed before durable handoff: {}", preparation_failure(result)),
    }
    let (request, operation) = put(2, b"o", b"synced-before-route");
    parent.mutate(1, request, operation).await.unwrap();
    let end = parent.snapshot().journal_durable_offset;
    store.pause_publication_for_tests();
    gate.release.notify_one();
    tokio::select! {
        () = store.wait_for_publication_for_tests() => {},
        result = &mut preparing => panic!("failed before parent WAL proof: {}; stage={}", preparation_failure(result), publication.stage()),
    }
    assert!(!preparing.is_finished());
    assert_eq!(publication.calls(), (0, 0, 0));
    assert_eq!(publication.prepared_calls(), (1, 1));
    let ingress = parent
        .split_ingress()
        .expect("route must precede WAL publication proof");
    let child = ingress.child();
    assert_eq!(
        child.get(1, b"o", None).await.unwrap().unwrap().value,
        b"synced-before-route"
    );
    let (request, operation) = put(3, b"n", b"independent-child-wal");
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        parent.mutate(1, request, operation),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.mutation_seq, 3);
    assert_eq!(
        response.journal_position.stream_name,
        child.snapshot().stream_name
    );
    // ChunkStream serializes its own checkpoint with later parent appends.
    // This case proves child routing, not progress through that WAL barrier.
    assert!(!preparing.is_finished());
    store.resume_publication_for_tests();
    tokio::select! {
        () = publication.entered.notified() => {},
        result = &mut preparing => panic!("failed before inherited publication: {}", preparation_failure(result)),
    }
    publication.release.notify_one();
    let prepared = preparing.await.unwrap().unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 2);
    assert_eq!(prepared.artifact.child.parent_cutover_offset, end);
    assert_eq!(prepared.artifact.child.child_stream_start_seq, 3);
    assert_eq!(
        child.get(1, b"n", None).await.unwrap().unwrap().value,
        b"independent-child-wal"
    );
}

#[tokio::test]
async fn cancelled_cutover_receiver_finalizes_installed_route_without_reinstalling() {
    let TestNativeSplit {
        parent,
        publication,
        gate,
        plan,
        mut preparing,
        ..
    } = setup_native_publication(false, false, false).await;
    tokio::select! {
        () = gate.entered.notified() => {},
        result = &mut preparing => panic!("failed before handoff: {}", preparation_failure(result)),
    }
    publication.cancel_control_after_install(preparing.abort_handle());
    gate.release.notify_one();
    match preparing.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("control future must be cancelled immediately after native commit"),
    }
    assert!(parent.split_ingress().is_some());
    assert!(parent.pending_split_child_target(&plan).unwrap().is_none());
    assert_eq!(publication.calls(), (0, 0, 0));
    let (request, operation) = put(2, b"n", b"after-cancel");
    let response = parent.mutate(1, request, operation).await.unwrap();
    assert_eq!(response.mutation_seq, 2);
    assert_eq!(
        response.journal_position.stream_name,
        parent.split_ingress().unwrap().child().snapshot().stream_name
    );
    let retry_source = parent.clone();
    let mut retry =
        tokio::spawn(async move { retry_source.finalize_installed_split_child_session(plan).await });
    tokio::select! {
        () = publication.entered.notified() => {},
        result = &mut retry => panic!("finalizer failed: {}", preparation_failure(result)),
    }
    publication.release.notify_one();
    let prepared = retry.await.unwrap().unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 1);
    assert_eq!(publication.prepared_calls(), (1, 1));
    assert_eq!(publication.calls(), (0, 1, 1));
    assert_eq!(
        parent.get(1, b"n", None).await.unwrap().unwrap().value,
        b"after-cancel"
    );
}

#[tokio::test]
async fn native_token_cleanup_never_blocks_parent_or_child_mutation_workers() {
    let TestNativeSplit {
        parent,
        publication,
        gate,
        mut preparing,
        ..
    } = setup_native_publication(false, false, false).await;
    tokio::select! {
        () = gate.entered.notified() => {},
        result = &mut preparing => panic!("failed before handoff: {}", preparation_failure(result)),
    }
    let cleanup = publication.pause_cleanup();
    gate.release.notify_one();
    tokio::select! {
        () = cleanup.entered() => {},
        result = &mut preparing => panic!("failed before cleanup: {}", preparation_failure(result)),
    }
    assert!(parent.split_ingress().is_some());
    assert!(!preparing.is_finished());
    assert_eq!(publication.calls(), (0, 0, 0));
    let parent_stream = parent.snapshot().stream_name;
    let child_stream = parent.split_ingress().unwrap().child().snapshot().stream_name;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        for (sequence, key, expected_stream) in [(2, b"b", parent_stream), (3, b"n", child_stream)] {
            let (request, operation) = put(sequence, key, b"during-host-cleanup");
            let response = parent.mutate(1, request, operation).await.unwrap();
            assert_eq!(response.journal_position.stream_name, expected_stream);
            assert_eq!(
                parent.get(1, key, None).await.unwrap().unwrap().value,
                b"during-host-cleanup"
            );
        }
    })
    .await
    .unwrap();
    drop(cleanup);
    tokio::select! {
        () = publication.entered.notified() => {},
        result = &mut preparing => panic!("failed before publication: {}", preparation_failure(result)),
    }
    publication.release.notify_one();
    let prepared = preparing.await.unwrap().unwrap();
    assert_eq!(prepared.artifact.cutover_seq, 1);
    assert_eq!(publication.prepared_calls(), (1, 1));
    assert_eq!(publication.calls(), (0, 1, 1));
    assert_eq!(
        parent.get(1, b"b", None).await.unwrap().unwrap().value,
        b"during-host-cleanup"
    );
    assert_eq!(
        parent.get(1, b"n", None).await.unwrap().unwrap().value,
        b"during-host-cleanup"
    );
}
