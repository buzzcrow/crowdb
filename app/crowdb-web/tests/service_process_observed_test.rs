// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/process_observed.rs"]
mod evidence;
#[path = "common/service_inventory.rs"]
mod fixture;

use crowdb_console_shared::{config::ServerEntry, ConsoleConfig};
use crowdb_web::AppState;
use evidence::{
    start, wait, ABSENT, ARRAY_MAP, EXHAUSTED, FOREIGN_BOOTSTRAP, FOREIGN_CLUSTER, FOREIGN_EXHAUSTION,
    FOREIGN_NODE, FOREIGN_OPERATION, MISSING_MAP, NULL_MAP, READ_ERROR, RUNNING, STRING_PID, ZERO_PID,
};
use fixture::TestServiceInventory;
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn unavailable_process_evidence_is_unknown_even_when_endpoint_health_is_up_then_same_pid_returns() {
    let mut fixture = TestServiceInventory::new(false);
    let initial = fixture.rows().await;
    assert_eq!(initial[0]["process_observed"], false);
    let mode = start(&mut fixture, false);
    assert_eq!(wait(&fixture, "running").await["pid"], 101);
    mode.store(READ_ERROR, Ordering::Relaxed);
    let row = wait(&fixture, "unknown").await;
    assert!(row["pid"].is_null());
    assert_eq!(row["health"], "up");
    mode.store(RUNNING, Ordering::Relaxed);
    assert_eq!(wait(&fixture, "running").await["pid"], 101);
}

#[tokio::test]
async fn fresh_empty_process_map_confirms_stopped_and_only_current_exhaustion_ends_pending_start() {
    let mut fixture = TestServiceInventory::new(false);
    let mode = start(&mut fixture, false);
    wait(&fixture, "running").await;
    mode.store(ABSENT, Ordering::Relaxed);
    let row = wait(&fixture, "stopped").await;
    assert!(row["pid"].is_null());
    assert_eq!(row["start_pending"], true);
    mode.store(EXHAUSTED, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let row = wait(&fixture, "stopped").await;
            if row["start_pending"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    mode.store(FOREIGN_EXHAUSTION, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let row = wait(&fixture, "stopped").await;
            if row["start_pending"] == true {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn foreign_live_operation_or_bootstrap_is_unknown_never_observed_stopped_or_adopted() {
    let mut fixture = TestServiceInventory::new(false);
    let mode = start(&mut fixture, false);
    wait(&fixture, "running").await;
    for wrong in [FOREIGN_OPERATION, FOREIGN_BOOTSTRAP] {
        mode.store(wrong, Ordering::Relaxed);
        let row = wait(&fixture, "unknown").await;
        assert!(row["pid"].is_null());
        mode.store(RUNNING, Ordering::Relaxed);
        assert_eq!(wait(&fixture, "running").await["pid"], 101);
    }
}

#[tokio::test]
async fn expired_process_evidence_is_unknown_and_never_retains_a_pid() {
    let mut fixture = TestServiceInventory::new(false);
    start(&mut fixture, false);
    wait(&fixture, "running").await;
    fixture.observer.take();
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(3050)).await;
    let row = wait(&fixture, "unknown").await;
    assert!(row["pid"].is_null());
    assert_eq!(row["start_pending"], false);
}

#[tokio::test]
async fn bound_bootstrap_pkv_without_intent_distinguishes_no_read_from_positive_process_absence() {
    let mut fixture = TestServiceInventory::new(false);
    let mode = start(&mut fixture, true);
    let row = wait(&fixture, "running").await;
    assert_eq!(row["pid"], 101);
    assert_eq!(row["start_pending"], false);
    mode.store(ABSENT, Ordering::Relaxed);
    let row = wait(&fixture, "stopped").await;
    assert!(row["pid"].is_null());
    assert_eq!(row["start_pending"], false);
    mode.store(READ_ERROR, Ordering::Relaxed);
    assert!(wait(&fixture, "unknown").await["pid"].is_null());
    mode.store(RUNNING, Ordering::Relaxed);
    assert_eq!(wait(&fixture, "running").await["pid"], 101);
}

#[tokio::test]
async fn missing_malformed_process_maps_and_invalid_pid_values_never_confirm_absence() {
    let mut fixture = TestServiceInventory::new(false);
    let mode = start(&mut fixture, false);
    wait(&fixture, "running").await;
    for malformed in [MISSING_MAP, NULL_MAP, ARRAY_MAP, ZERO_PID, STRING_PID] {
        mode.store(malformed, Ordering::Relaxed);
        assert!(wait(&fixture, "unknown").await["pid"].is_null());
        mode.store(RUNNING, Ordering::Relaxed);
        assert_eq!(wait(&fixture, "running").await["pid"], 101);
    }
}

#[tokio::test]
async fn wrong_monitor_node_or_cluster_is_unknown_not_observed_stopped() {
    let mut fixture = TestServiceInventory::new(false);
    let mode = start(&mut fixture, false);
    wait(&fixture, "running").await;
    for foreign in [FOREIGN_NODE, FOREIGN_CLUSTER] {
        mode.store(foreign, Ordering::Relaxed);
        assert!(wait(&fixture, "unknown").await["pid"].is_null());
        mode.store(RUNNING, Ordering::Relaxed);
        assert_eq!(wait(&fixture, "running").await["pid"], 101);
    }
}

#[tokio::test]
async fn accepted_no_pid_mutation_reply_does_not_manufacture_process_absence() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.start();
    fixture.wait_pid(1, Some(101)).await;
    fixture.source.hold_node.store(1, Ordering::Relaxed);
    TestServiceInventory::signalled(&fixture.source.process_entered).await;
    let intent = fixture.source.intents.load_full()[0].clone();
    let mutation =
        crowdb_web::invalidate_service_observation_for_tests(&fixture.state, 1, &intent.service_id);
    mutation.complete(
        intent.clone(),
        &json!({"operation_id": intent.operation_id, "pid": null}),
    );
    let row = wait(&fixture, "unknown").await;
    assert!(row["pid"].is_null());
    assert_eq!(row["start_pending"], true);
    fixture.source.hold_node.store(0, Ordering::Relaxed);
    fixture.source.process_release.notify_one();
    TestServiceInventory::signalled(&fixture.source.process_finished).await;
    fixture.wait_pid(1, Some(101)).await;
    assert_eq!(wait(&fixture, "running").await["pid"], 101);
}

#[tokio::test]
async fn accepted_actual_pid_mutation_reply_is_observed_and_late_sample_remains_fenced() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.start();
    fixture.wait_pid(1, Some(101)).await;
    fixture.source.hold_node.store(1, Ordering::Relaxed);
    TestServiceInventory::signalled(&fixture.source.process_entered).await;
    let intent = fixture.source.intents.load_full()[0].clone();
    let mutation =
        crowdb_web::invalidate_service_observation_for_tests(&fixture.state, 1, &intent.service_id);
    fixture.source.pid.store(500, Ordering::Relaxed);
    mutation.complete(
        intent.clone(),
        &json!({"operation_id": intent.operation_id, "pid": 501}),
    );
    let row = wait(&fixture, "running").await;
    assert_eq!(row["pid"], 501);
    fixture.source.hold_node.store(0, Ordering::Relaxed);
    fixture.source.process_release.notify_one();
    TestServiceInventory::signalled(&fixture.source.process_finished).await;
    assert_eq!(wait(&fixture, "running").await["pid"], 501);
}

#[tokio::test]
async fn changed_node_configuration_or_missing_binding_never_reuses_observed_process_evidence() {
    let mut fixture = TestServiceInventory::new(false);
    start(&mut fixture, false);
    wait(&fixture, "running").await;
    // Freeze the accepted old fact; a legitimate new-node read is outside this check.
    fixture.observer.take();
    tokio::task::yield_now().await;
    fixture.state.config.write().unwrap().nodes[0].host = "127.0.0.99".into();
    let row = wait(&fixture, "unknown").await;
    assert!(row["pid"].is_null());
    std::fs::remove_file(fixture.root.path().join("node-binding.json")).unwrap();
    for row in fixture.rows().await.as_array().unwrap() {
        assert_eq!(row["process_observed"], false);
        assert!(row["pid"].is_null());
    }
}

#[tokio::test]
async fn local_inventory_omits_monitor_observation_validity_for_legacy_consumers() {
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use tower::ServiceExt;
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("local-process-observed");
    let mut config = ConsoleConfig::default();
    config
        .servers
        .push(ServerEntry::new("external", "http://127.0.0.1:1"));
    let state = AppState::with_runtime_root(config, root.path().join("console"));
    let response = crowdb_web::router(state)
        .oneshot(
            Request::builder()
                .uri("/api/servers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    let rows: Value = serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert!(!rows[0].as_object().unwrap().contains_key("process_observed"));
}
