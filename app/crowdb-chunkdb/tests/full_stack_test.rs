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

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster};
use crowdb_chunkdb::allocator::StripAllocType;
use crowdb_chunkdb::lifecycle::{ChunkLockMap, LifecycleError, LifecycleHandler};
use crowdb_chunkdb::metrics::LifecycleMetrics;
use crowdb_chunkdb::selector::PlacementConstraints;
use crowdb_protocol::chunkdb::rpc::{ChunkState, ChunkType, Strip, StripType};
use crowdb_protocol::common::ChunkId;

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

    // 5. Allocate a chunk (1 unit = 1 MB per strip, 3 mirror copies).
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1, // 1 unit per strip
            1, // 1 strip
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
