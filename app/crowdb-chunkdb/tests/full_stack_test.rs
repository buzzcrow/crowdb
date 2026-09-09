// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Full-stack E2E test: real KV cluster + diskdb + chunkdb in-process.
//!
//! Verifies the allocate → append → seal → query → delete lifecycle
//! against a real 3-node crowdb-kv-server cluster with diskdb running
//! in-process as a crowdb-rpc server.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster, DATA_GROUP_ID, STORE_ID};
use crowdb_chunkdb::allocator::StripAllocType;
use crowdb_chunkdb::conversion::io::ConversionDiskIo;
use crowdb_chunkdb::conversion::{decode_payload, ConversionCoordinator, MirrorToEcTaskHandler};
use crowdb_chunkdb::lifecycle::{ChunkLockMap, LifecycleError, LifecycleHandler};
use crowdb_chunkdb::metrics::{ChunkdbMetrics, LifecycleMetrics};
use crowdb_chunkdb::repair::{decode_payload as decode_repair_payload, RepairCoordinator};
use crowdb_chunkdb::routing::{default_binding_table, BindingCache};
use crowdb_chunkdb::selector::PlacementConstraints;
use crowdb_chunkdb::task::{
    TaskAdmission, TaskClaim, TaskExecutor, TaskHandler, TaskManager, TaskOutcome, TaskScanner, TaskStore,
};
use crowdb_common::metrics::MetricsRegistry;
use crowdb_protocol::chunk_task::{
    ChunkTaskState, ChunkTaskValue, CHUNK_TASK_SCHEMA_VERSION, TASK_KIND_MIRROR_TO_EC, TASK_KIND_REPAIR_STRIP,
};
use crowdb_protocol::chunkdb::rpc::{ChunkState, ChunkType, Strip, StripType};
use crowdb_protocol::common::ChunkId;

fn task_value() -> ChunkTaskValue {
    ChunkTaskValue {
        schema_version: CHUNK_TASK_SCHEMA_VERSION,
        task_id: ChunkId { high: 3, low: 4 },
        partition_id: ChunkId { high: 1, low: 2 },
        kind: TASK_KIND_MIRROR_TO_EC,
        kind_version: 1,
        state: ChunkTaskState::Pending,
        priority: 10,
        revision: 1,
        operation_id: ChunkId { high: 5, low: 6 },
        source_revision: 7,
        created_at_ms: 100,
        updated_at_ms: 100,
        eligible_at_ms: 100,
        attempt: 0,
        max_attempts: 3,
        estimated_queue_bytes: 20 * 1024 * 1024,
        claim_owner: 0,
        claim_generation: 0,
        claim_deadline_ms: 0,
        last_error_code: 0,
        last_error: String::new(),
        payload: vec![1, 2, 3],
    }
}

struct CompleteTaskHandler;

impl TaskHandler for CompleteTaskHandler {
    fn kind(&self) -> u16 {
        TASK_KIND_MIRROR_TO_EC
    }

    fn supports_version(&self, version: u16) -> bool {
        version == 1
    }

    fn execute<'a>(&'a self, _task: &'a ChunkTaskValue) -> crowdb_chunkdb::task::executor::TaskFuture<'a> {
        Box::pin(async { TaskOutcome::Complete })
    }
}

#[tokio::test]
async fn task_survives_claim_expiry_takeover_and_completion() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }

    let cluster = KvCluster::start().await;
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let store = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let first_manager = TaskManager::new(Arc::clone(&store), 41, 100);
    let task = task_value();

    assert!(matches!(
        first_manager.admit(task.clone()).await.unwrap(),
        TaskAdmission::Created(_)
    ));
    assert!(matches!(
        first_manager.admit(task.clone()).await.unwrap(),
        TaskAdmission::Existing(_)
    ));

    let ready = store.scan_ready(100, 16).await.unwrap();
    assert_eq!(ready.len(), 1);
    let first_claim = first_manager.claim(&ready[0], 100).await.unwrap().unwrap();
    assert_eq!(first_claim.task.claim_generation, 1);
    assert_eq!(first_claim.task.attempt, 1);
    assert!(store.scan_ready(100, 16).await.unwrap().is_empty());
    assert!(store.scan_expired_leases(199, 16).await.unwrap().is_empty());

    drop(first_manager);
    let restarted_manager = Arc::new(TaskManager::new(Arc::clone(&store), 42, 100));
    let executor = Arc::new(
        TaskExecutor::new(
            Arc::clone(&restarted_manager),
            2,
            vec![Arc::new(CompleteTaskHandler)],
        )
        .unwrap(),
    );
    let scanner = TaskScanner::new(
        Arc::clone(&store),
        Arc::clone(&restarted_manager),
        executor,
        16,
        Duration::from_secs(30),
    );
    let summary = scanner.run_once(200).await.unwrap();
    assert_eq!(summary.expired_claims_requeued, 1);
    assert_eq!(summary.ready_indexes_seen, 1);
    assert_eq!(summary.tasks_claimed, 1);
    assert_eq!(summary.tasks_completed_or_requeued, 1);
    assert_eq!(summary.dispatch_errors, 0);

    assert!(store.scan_ready(210, 16).await.unwrap().is_empty());
    assert!(store.scan_expired_leases(u64::MAX, 16).await.unwrap().is_empty());
    let stored = store
        .get(&task.partition_id, task.kind, &task.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, ChunkTaskState::Completed);
    assert_eq!(stored.revision, 5);
    assert_eq!(stored.claim_generation, 2);
    assert_eq!(stored.attempt, 2);

    for id in 10..13 {
        let mut queued = task_value();
        queued.task_id.low = id;
        queued.operation_id.low = id;
        restarted_manager.admit(queued).await.unwrap();
    }
    let bounded = scanner.run_once(300).await.unwrap();
    assert_eq!(bounded.ready_indexes_seen, 3);
    assert_eq!(bounded.tasks_claimed, 2);
    assert_eq!(bounded.tasks_completed_or_requeued, 2);
    assert_eq!(store.scan_ready(301, 16).await.unwrap().len(), 1);
    let drained = scanner.run_once(301).await.unwrap();
    assert_eq!(drained.tasks_claimed, 1);
    assert!(store.scan_ready(302, 16).await.unwrap().is_empty());
}

#[tokio::test]
async fn unavailable_strip_survives_crash_gap_and_is_admitted_as_repair_task() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let old = chunk.strips[0].clone();
    let Some(Strip::MirrorStrip(mirror)) = &old.strip else {
        panic!("expected mirror strip");
    };
    let failed = mirror.segments[0];
    let mut marked = old.clone();
    marked.unavailable_segments.push(failed);
    let marked_chunk = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            chunk.modify_ts,
            0,
            std::slice::from_ref(&old),
            std::slice::from_ref(&marked),
            ChunkId { high: 111, low: 1 },
        )
        .await
        .unwrap();

    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let restarted = RepairCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&tasks));
    assert_eq!(restarted.admit_chunk(&marked_chunk, 100).await.unwrap(), 1);
    assert_eq!(restarted.scan_batch(256, 101).await.unwrap(), 0);

    let ready = tasks.scan_ready(101, 16).await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].kind, TASK_KIND_REPAIR_STRIP);
    let task = tasks
        .get(&chunk_id, ready[0].kind, &ready[0].task_id)
        .await
        .unwrap()
        .unwrap();
    let payload = decode_repair_payload(&task.payload).unwrap();
    assert_eq!(payload.chunk_id, chunk_id);
    assert_eq!(payload.strip_sequence, old.strip_sequence);
    assert_eq!(payload.failed_segments, vec![failed]);
    assert_eq!(task.source_revision, marked_chunk.modify_ts);
    assert_eq!(
        task.estimated_queue_bytes,
        u64::from(failed.unit_count) * u64::from(old.unit_kb) * 1024 * 4
    );

    // A terminal task must not suppress repair while its durable failure
    // marker still exists.
    let mut completed = task.clone();
    completed.state = ChunkTaskState::Completed;
    completed.revision = completed.revision.saturating_add(1);
    tasks.write_transition(Some(&task), &completed).await.unwrap();
    assert_eq!(restarted.admit_chunk(&marked_chunk, 102).await.unwrap(), 1);
    let revived = tasks
        .get(&chunk_id, completed.kind, &completed.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(revived.state, ChunkTaskState::Pending);
    assert_eq!(revived.revision, completed.revision.saturating_add(1));
    assert_eq!(revived.attempt, 0);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn chunkdb_full_stack_allocate_seal_delete() {
    // Skip if crowdb-kv-server binary is not built.
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }

    // 1. Start the kv cluster.
    let cluster = KvCluster::start().await;
    eprintln!("kv cluster started");

    // 2. Seed hardware metadata.
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    eprintln!("hardware seeded");

    // 3. Start diskdb in-process.
    let diskdb = DiskdbServer::start(&cluster).await;
    eprintln!("diskdb started: {}", diskdb.rpc_endpoint);

    // 4. Wire chunkdb handler.
    let harness = ChunkdbHarness::start(&cluster).await;
    eprintln!("chunkdb harness ready");

    // 5. Allocate a chunk with three prefetched strips (1 MiB each,
    // 3 mirror copies).
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1, // 1 unit per strip
            3, // 3 strips
            StripType::Mirror,
            0,
            0,
            3, // 3 mirror copies
            ChunkType::Repo,
            0,
            0,
        )
        .await
        .expect("allocate_chunk");
    assert_eq!(chunk.state, ChunkState::Active as i32);
    assert!(!chunk.strips.is_empty(), "chunk should have strips");
    eprintln!("chunk allocated: {} strips", chunk.strips.len());

    // 6. Query the chunk.
    let chunk_id = chunk.id.as_ref().expect("chunk has id");
    let queried = harness.handler.query_chunk(chunk_id).await.expect("query_chunk");
    assert_eq!(queried.id, chunk.id);
    assert_eq!(queried.state, ChunkState::Active as i32);
    eprintln!("chunk queried");

    // 7. Seal the chunk.
    let sealed = harness
        .handler
        .seal_chunk(chunk_id, 100)
        .await
        .expect("seal_chunk");
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.sealed_length, 100);
    assert_eq!(sealed.strips.len(), 1);
    assert_eq!(sealed.capacity, 1024);
    assert!(sealed.cleanup_intents.is_empty());
    eprintln!("chunk sealed");

    // 8. Delete the chunk.
    let deleted = harness
        .handler
        .delete_chunk(chunk_id)
        .await
        .expect("delete_chunk");
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    eprintln!("chunk deleted");

    // 9. Delete again → should return the same idempotent tombstone.
    let deleted_again = harness
        .handler
        .delete_chunk(chunk_id)
        .await
        .expect("idempotent delete");
    assert_eq!(deleted_again.state, ChunkState::Deleted as i32);
    eprintln!("second delete returned Deleted (idempotent)");

    // 10. Query after delete → should return the deleted chunk (state=Deleted).
    let queried_after = harness
        .handler
        .query_chunk(chunk_id)
        .await
        .expect("query after delete");
    assert_eq!(queried_after.state, ChunkState::Deleted as i32);
    eprintln!("chunk queried after delete (state=Deleted)");
}

#[tokio::test]
async fn chunkdb_fenced_range_replacement_is_idempotent_and_preserves_geometry() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 2, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let old = chunk.strips[0].clone();
    let Some(Strip::MirrorStrip(mut mirror)) = old.strip.clone() else {
        panic!("expected mirror strip");
    };
    let failed = mirror.segments[0];
    let replacement = harness
        .handler
        .allocate_replacement_segment(
            &chunk_id,
            &failed,
            &mirror.segments[1..],
            &[failed.disk_id.unwrap()],
        )
        .await
        .unwrap();
    assert_ne!(replacement.disk_id, failed.disk_id);
    mirror.segments[0] = replacement;
    let mut installed = old.clone();
    installed.strip = Some(Strip::MirrorStrip(mirror));
    let operation_id = ChunkId { high: 9, low: 7 };
    let updated = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            chunk.modify_ts,
            0,
            std::slice::from_ref(&old),
            std::slice::from_ref(&installed),
            operation_id,
        )
        .await
        .unwrap();
    assert_eq!(updated.capacity, chunk.capacity);
    assert_eq!(updated.next_strip_sequence, chunk.next_strip_sequence);
    assert_eq!(updated.strips[0], installed);
    assert_eq!(updated.cleanup_intents.len(), 1);
    assert_eq!(updated.cleanup_intents[0].retired_segments, vec![failed]);

    let retried = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            chunk.modify_ts,
            0,
            std::slice::from_ref(&old),
            std::slice::from_ref(&installed),
            operation_id,
        )
        .await
        .unwrap();
    assert_eq!(retried, updated);
    assert_stale_replacement_conflicts(&harness, chunk_id, &chunk, &installed).await;

    let mut consolidated = harness
        .allocator
        .allocate_strip(
            &harness.topology.snapshot(),
            &chunk_id,
            StripAllocType::Mirror { copy_count: 3 },
            2,
            updated.strips[0].strip_sequence,
            &PlacementConstraints::new(),
        )
        .await
        .unwrap();
    consolidated.chunk_offset = updated.strips[0].chunk_offset;
    let consolidated = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            updated.modify_ts,
            0,
            &updated.strips,
            std::slice::from_ref(&consolidated),
            ChunkId { high: 11, low: 12 },
        )
        .await
        .unwrap();
    assert_eq!(consolidated.strips.len(), 1);
    assert_eq!(consolidated.capacity, chunk.capacity);
    assert_eq!(consolidated.next_strip_sequence, chunk.next_strip_sequence);
    assert_eq!(consolidated.cleanup_intents.len(), 2);
}

async fn assert_stale_replacement_conflicts(
    harness: &ChunkdbHarness,
    chunk_id: ChunkId,
    original: &crowdb_protocol::chunkdb::rpc::Chunk,
    installed: &crowdb_protocol::chunkdb::rpc::ChunkStrip,
) {
    let conflict = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            original.modify_ts,
            0,
            std::slice::from_ref(&original.strips[1]),
            std::slice::from_ref(installed),
            ChunkId { high: 10, low: 8 },
        )
        .await;
    assert!(matches!(conflict, Err(LifecycleError::StateConflict)));
}

#[tokio::test]
async fn mirror_range_is_atomically_replaced_by_tentative_ec_strip() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start_with_layout_validity(&cluster, Duration::from_millis(1)).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 8, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate mirror range");
    let chunk_id = chunk.id.expect("chunk id");
    let chunk = harness
        .handler
        .seal_chunk(&chunk_id, chunk.capacity)
        .await
        .expect("seal mirror range");

    let task_bindings = BindingCache::new();
    task_bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let task_store = Arc::new(TaskStore::new(cluster.make_crowdb_client(), task_bindings));
    let coordinator = ConversionCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&task_store));
    let prepared = coordinator
        .prepare(
            chunk_id,
            chunk.modify_ts,
            0,
            chunk.strips.clone(),
            8,
            4,
            7001,
            30_000,
            100,
        )
        .await
        .expect("prepare durable conversion task");
    let replacement = prepared.replacement_strip;
    let Some(Strip::EcStrip(ec)) = &replacement.strip else {
        panic!("expected EC replacement");
    };
    assert_eq!((ec.data_num, ec.code_num, ec.segments.len()), (8, 4, 12));
    assert_eq!(replacement.capacity, chunk.capacity);
    assert_eq!(replacement.chunk_offset, 0);

    let converted = coordinator
        .complete(chunk_id, prepared.task_id, 7001, 101)
        .await
        .expect("publish durable EC replacement");
    let mut durable_replacement = replacement.clone();
    let Some(Strip::EcStrip(ec)) = &mut durable_replacement.strip else {
        unreachable!();
    };
    ec.ec_state = crowdb_protocol::chunkdb::rpc::EcState::Parity as i32;
    assert_eq!(converted.strips, vec![durable_replacement.clone()]);
    assert_eq!(converted.last_strip_replacement, Some(prepared.operation_id));
    assert_eq!(converted.cleanup_intents.len(), 1);
    assert_eq!(converted.cleanup_intents[0].retired_segments.len(), 24);

    let retry = coordinator
        .complete(chunk_id, prepared.task_id, 7001, 102)
        .await
        .expect("idempotent task completion retry");
    assert_eq!(retry, converted);

    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(harness.handler.reconcile_pending_chunks().await.unwrap(), 1);
    let reclaimed = harness.handler.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(reclaimed.strips, vec![durable_replacement]);
    assert!(reclaimed.cleanup_intents.is_empty());
}

#[tokio::test]
async fn deletion_during_conversion_clears_task_ownership_before_tentative_cleanup() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 8, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate mirror range");
    let chunk_id = chunk.id.expect("chunk id");
    let chunk = harness
        .handler
        .seal_chunk(&chunk_id, chunk.capacity)
        .await
        .expect("seal mirror range");

    let task_bindings = BindingCache::new();
    task_bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let task_store = Arc::new(TaskStore::new(cluster.make_crowdb_client(), task_bindings));
    let coordinator = ConversionCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&task_store));
    let prepared = coordinator
        .prepare(
            chunk_id,
            chunk.modify_ts,
            0,
            chunk.strips,
            8,
            4,
            7002,
            30_000,
            100,
        )
        .await
        .expect("prepare conversion");
    let task = task_store
        .get(&chunk_id, TASK_KIND_MIRROR_TO_EC, &prepared.task_id)
        .await
        .unwrap()
        .expect("durable conversion task");
    assert!(decode_payload(&task.payload).unwrap().replacement_strip.is_some());

    harness
        .handler
        .delete_chunk(&chunk_id)
        .await
        .expect("delete chunk");
    let io = Arc::new(ConversionDiskIo::empty_for_tests());
    let mut registry = MetricsRegistry::new();
    let metrics = ChunkdbMetrics::register(&mut registry).conversion;
    let manager = Arc::new(TaskManager::new(Arc::clone(&task_store), 7002, 30_000));
    let executor = TaskExecutor::new(
        manager,
        1,
        vec![Arc::new(MirrorToEcTaskHandler::new(
            Arc::clone(&harness.handler),
            Arc::clone(&task_store),
            io,
            metrics,
            50,
            1,
        ))],
    )
    .unwrap();
    executor.execute(TaskClaim { task }).await.unwrap();

    let failed = task_store
        .get(&chunk_id, TASK_KIND_MIRROR_TO_EC, &prepared.task_id)
        .await
        .unwrap()
        .expect("terminal conversion task");
    assert_eq!(failed.state, ChunkTaskState::Failed);
    assert!(decode_payload(&failed.payload)
        .unwrap()
        .replacement_strip
        .is_none());
    harness
        .handler
        .discard_conversion_strip(&chunk_id, &prepared.replacement_strip)
        .await
        .expect("reclaim unreferenced tentative replacement");
    let deleted = harness.handler.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    assert!(deleted.strips.is_empty());
}

#[tokio::test]
async fn chunkdb_restart_reconciles_expired_replacement_cleanup_intent() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start_with_layout_validity(&cluster, Duration::from_millis(1)).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let old = chunk.strips[0].clone();
    let Some(Strip::MirrorStrip(mut mirror)) = old.strip.clone() else {
        panic!("expected mirror strip");
    };
    let failed = mirror.segments[0];
    let replacement = harness
        .handler
        .allocate_replacement_segment(
            &chunk_id,
            &failed,
            &mirror.segments[1..],
            &[failed.disk_id.unwrap()],
        )
        .await
        .unwrap();
    mirror.segments[0] = replacement;
    let mut installed = old.clone();
    installed.strip = Some(Strip::MirrorStrip(mirror));
    harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            chunk.modify_ts,
            0,
            std::slice::from_ref(&old),
            std::slice::from_ref(&installed),
            ChunkId { high: 31, low: 41 },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;

    let restarted = LifecycleHandler::new(
        Arc::clone(&harness.store),
        Arc::clone(&harness.allocator),
        harness.topology.clone(),
    )
    .with_layout_validity(Duration::from_millis(1))
    .with_locks(Arc::new(ChunkLockMap::new(
        10_000,
        Arc::new(LifecycleMetrics::new()),
        Duration::from_secs(60),
    )));
    assert_eq!(restarted.reconcile_pending_chunks().await.unwrap(), 1);
    let reconciled = restarted.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(reconciled.strips[0], installed);
    assert!(reconciled.cleanup_intents.is_empty());
    assert_eq!(
        reconciled.last_strip_replacement,
        Some(ChunkId { high: 31, low: 41 })
    );
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_append() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk.
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent appends on the same chunk — both should succeed
    // (serialized by the per-chunk lock, not corrupted).
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move {
        h1.append_chunk(&id1, 1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append 1")
    });
    let t2 = tokio::spawn(async move {
        h2.append_chunk(&id2, 1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append 2")
    });
    let r1 = t1.await.expect("task 1");
    let r2 = t2.await.expect("task 2");
    // One request appends; the other observes a stale revision and receives
    // the refreshed parent without allocating a duplicate strip.
    let outcomes = [&r1, &r2];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.strips.len() == 1)
            .count(),
        1
    );
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.chunk.is_some()).count(),
        1
    );
    let current = harness.handler.query_chunk(&chunk_id).await.expect("query chunk");
    assert_eq!(current.strips.len(), 2);
    assert_eq!(current.modify_ts, 2);
}

#[tokio::test]
async fn chunkdb_lock_no_deadlock_different_chunks() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate two chunks.
    let chunk_a = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate A");
    let chunk_b = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate B");
    let id_a = *chunk_a.id.as_ref().expect("chunk A id");
    let id_b = *chunk_b.id.as_ref().expect("chunk B id");

    // Concurrent append on different chunks — no deadlock, both succeed quickly.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let t1 = tokio::spawn(async move {
        h1.append_chunk(&id_a, 1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append A")
    });
    let t2 = tokio::spawn(async move {
        h2.append_chunk(&id_b, 1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append B")
    });
    // If there's a deadlock, this timeout will fire.
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task A"), t2.await.expect("task B"))
    })
    .await
    .expect("no deadlock — both appends completed within 10s");
    assert_eq!(r1.strips.len(), 1, "chunk A should return one appended strip");
    assert_eq!(r2.strips.len(), 1, "chunk B should return one appended strip");
    assert_eq!(r1.modify_ts, 2);
    assert_eq!(r2.modify_ts, 2);
    eprintln!("no deadlock: both chunks appended independently");
}

#[tokio::test]
async fn chunkdb_cache_hit_on_second_query() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (populates cache via populate_cache for auto-gen ID).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Append (should be a cache hit — no store round-trip for get_chunk).
    let appended = harness
        .handler
        .append_chunk(&chunk_id, 1, 1, StripType::Mirror, 0, 0, 3, 1)
        .await
        .expect("append_chunk");
    assert_eq!(appended.strips.len(), 1, "should return only the appended strip");
    assert_eq!(appended.modify_ts, 2);

    // Seal (should also be a cache hit after append refreshed the cache).
    let sealed = harness
        .handler
        .seal_chunk(&chunk_id, 100)
        .await
        .expect("seal_chunk");
    assert_eq!(sealed.state, ChunkState::Sealed as i32);

    // Check metrics — cache should have hits.
    if let Some(locks) = harness.handler.locks() {
        let snap = locks.metrics_snapshot();
        assert!(
            snap.cache_hit_count > 0 || snap.cache_miss_count > 0,
            "cache metrics should be non-zero (hits={}, misses={})",
            snap.cache_hit_count,
            snap.cache_miss_count
        );
        eprintln!(
            "cache metrics: hits={}, misses={}, size={}",
            snap.cache_hit_count, snap.cache_miss_count, snap.cache_size
        );
    }
    eprintln!("cache hit on second query verified");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_seal() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent seals on the same chunk — the lock serializes
    // them: exactly one succeeds (Sealed), the other sees Sealed
    // state and fails with InvalidStateTransition.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.seal_chunk(&id1, 100).await });
    let t2 = tokio::spawn(async move { h2.seal_chunk(&id2, 200).await });
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task 1"), t2.await.expect("task 2"))
    })
    .await
    .expect("no deadlock — both seals completed within 10s");

    let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok_count, 1, "exactly one seal should succeed, got {ok_count}");
    let sealed = if let Ok(c) = &r1 { c } else { r2.as_ref().unwrap() };
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    eprintln!("concurrent seal serialized: one Ok, one Err (InvalidStateTransition)");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_delete() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent deletes on the same chunk — the lock serializes
    // them and both return the same idempotent Deleted result.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.delete_chunk(&id1).await });
    let t2 = tokio::spawn(async move { h2.delete_chunk(&id2).await });
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task 1"), t2.await.expect("task 2"))
    })
    .await
    .expect("no deadlock — both deletes completed within 10s");

    assert_eq!(r1.expect("first delete").state, ChunkState::Deleted as i32);
    assert_eq!(r2.expect("second delete").state, ChunkState::Deleted as i32);
    eprintln!("concurrent delete serialized: both return Deleted");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_append_delete() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Concurrent append + delete on the same chunk — the lock
    // serializes them. Delete always wins (Active → Deleted). If
    // delete runs first, append sees Deleted → InvalidStateTransition.
    // If append runs first, append succeeds then delete → Deleted.
    // Invariant: delete always succeeds; final state is Deleted.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.append_chunk(&id1, 1, 1, StripType::Mirror, 0, 0, 3, 1).await });
    let t2 = tokio::spawn(async move { h2.delete_chunk(&id2).await });
    let (append_res, delete_res) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("append task"), t2.await.expect("delete task"))
    })
    .await
    .expect("no deadlock — both completed within 10s");

    let deleted = delete_res.expect("delete always succeeds on Active chunk");
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    // Append may succeed (if it ran first) or fail with
    // InvalidStateTransition (if delete ran first) — both are valid.
    match &append_res {
        Ok(outcome) => {
            assert_eq!(outcome.modify_ts, 2);
            assert_eq!(outcome.strips.len(), 1);
            assert!(outcome.chunk.is_none());
        }
        Err(LifecycleError::InvalidStateTransition(_)) => {
            eprintln!("delete ran first → append rejected (InvalidStateTransition)");
        }
        Err(e) => panic!("append failed with unexpected error: {e:?}"),
    }
    // Final state from the store must be Deleted.
    let final_chunk = harness.handler.query_chunk(&chunk_id).await.expect("query final");
    assert_eq!(final_chunk.state, ChunkState::Deleted as i32);
    eprintln!("concurrent append+delete serialized: final state Deleted");
}

#[tokio::test]
async fn chunkdb_shared_writer_cursor_is_fenced_and_orphan_is_sealed() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1024, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 99, 20)
        .await
        .expect("allocate shared chunk");
    let chunk_id = chunk.id.expect("chunk id");
    let advanced = harness
        .handler
        .advance_chunk_write(&chunk_id, 99, chunk.modify_ts, 1024 * 1024, Some(0), 20)
        .await
        .expect("advance cursor");
    assert_eq!(advanced.acknowledged_cursor, 1024 * 1024);
    assert_eq!(advanced.closed_strip_sequence, Some(0));
    assert!(matches!(
        harness
            .handler
            .advance_chunk_write(
                &chunk_id,
                100,
                advanced.modify_ts,
                1024 * 1024 + 4096,
                Some(0),
                20
            )
            .await,
        Err(LifecycleError::StateConflict)
    ));
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(harness.handler.seal_expired_writer_chunks().await.unwrap(), 1);
    let sealed = harness.handler.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.sealed_length, 1024);
    assert_eq!(sealed.acknowledged_cursor, 1024 * 1024);
    assert_eq!(sealed.closed_strip_sequence, Some(0));
}
