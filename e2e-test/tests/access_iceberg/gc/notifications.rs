// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use super::seed::{seed_expired_backlog, seed_head, seed_table};
use crate::access_iceberg::{frontend::TestFrontend, native::TestNativeStorage, now_ms};

mod large;
mod matrix;
mod proof;
mod setup;
mod storage;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Access GC and real sealed ChunkDB ranges"]
async fn mixed_external_and_tree_notifications_complete_without_reclaiming_live_ranges() {
    let stack = TestNativeStorage::new();
    let (store, context, dead_table) = seed_table(&stack).await;
    let live_table = seed_head(store.as_ref(), context).await;
    let data = storage::write_payloads(&stack, store.clone(), context, dead_table, live_table).await;
    let ranges = proof::TestRangeProof::new(&stack, &data).await;
    setup::age_owned_intents(store.as_ref(), &data, now_ms()).await;
    // Files above have real external payloads. Reuse only the existing expired
    // marker helper; zero means no extra Inline files, not an empty GC workload.
    let marker = seed_expired_backlog(store.as_ref(), context, dead_table, 0).await;
    let (stale, live_head) =
        setup::late_stale_marker(store.as_ref(), context, &data, marker.dropped_ms).await;
    storage::assert_live(store.clone(), context, &data).await;
    let started = Instant::now();
    let first = TestFrontend::start(12).await;
    let before = tokio::time::timeout(
        Duration::from_secs(30),
        proof::wait_for_task(store.clone(), context, dead_table),
    )
    .await
    .unwrap();
    first.stop().await;
    let server = TestFrontend::start(12).await;
    let completed = tokio::time::timeout(
        Duration::from_secs(480),
        proof::wait_complete(store.clone(), context, dead_table, marker.dropped_ms),
    )
    .await;
    server.stop().await;
    let completed = completed.expect("real notification GC did not complete within 480 seconds");
    assert_eq!(before.created_ms, completed.created_ms);
    assert_eq!(before.not_before_ms, completed.not_before_ms);
    assert_eq!(before.head, completed.head);
    proof::verify_resources(store.clone(), context, &completed, &data, &stale, &live_head).await;
    let retirement = ranges.verify(&data).await;
    storage::assert_live(store, context, &data).await;
    println!(
        "gc_notification_convergence {}",
        serde_json::json!({
            "scope":"Access notification acceptance; chunk logical bytes, no physical capacity claim",
            "elapsed_ms":started.elapsed().as_millis(),"dead_files":2,"tree_blocks":data.tree_roots.len(),
            "task_id":completed.identity.to_string(),"task_revision":completed.revision,
            "created_ms":completed.created_ms,"not_before_ms":completed.not_before_ms,
            "first_observed_phase":format!("{:?}",before.phase),"first_observed_revision":before.revision,
            "deleted":completed.deleted,"logical_bytes":completed.reclaimed_bytes,
            "expected_chunk_logical_ranges":data.dead_ranges,"live_chunk_logical_range":data.live_range,
            "retirement":retirement,"raw_wire_reclaimed_boolean":"not_observed_in_release_Access",
        })
    );
}
