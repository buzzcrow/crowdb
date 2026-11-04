// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/service_inventory.rs"]
mod fixture;
use crowdb_protocol::mgmt::node::NodeServiceAction;
use fixture::TestServiceInventory;
use serde_json::json;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[tokio::test]
async fn inventory_readers_do_not_wait_for_an_unrelated_monitor_or_refresh_evidence_age() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.source.hold_node.store(2, Ordering::Relaxed);
    fixture.start();
    TestServiceInventory::signalled(&fixture.source.process_entered).await;
    let row = fixture.wait_pid(1, Some(101)).await;
    assert_eq!(row["pid_node_id"], 1);
    assert_eq!(row["process_observed"], true);
    assert_eq!(fixture.rows().await.as_array().unwrap().len(), 2);
    assert!(crowdb_web::start_service_observer(&fixture.state).is_err());
    fixture.observer.take();
    tokio::task::yield_now().await;
    let calls = fixture.source.calls.load(Ordering::Relaxed);
    for rows in futures::future::join_all((0..32).map(|_| fixture.rows())).await {
        assert_eq!(rows[0]["pid"], 101);
    }
    assert_eq!(fixture.source.calls.load(Ordering::Relaxed), calls);
    tokio::time::sleep(Duration::from_millis(3050)).await;
    let rows = fixture.rows().await;
    assert!(rows[0]["pid"].is_null());
    assert!(rows[0]["pid_node_id"].is_null());
    assert_eq!(rows[0]["health"], "unknown");
    assert_eq!(rows[0]["process_observed"], false);
    assert_eq!(fixture.source.calls.load(Ordering::Relaxed), calls);
}

#[tokio::test]
async fn a_late_pre_mutation_process_sample_cannot_replace_a_confirmed_pid_or_hide_siblings() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.start();
    fixture.wait_pid(1, Some(101)).await;
    fixture.wait_pid(2, Some(102)).await;
    fixture.source.hold_node.store(1, Ordering::Relaxed);
    TestServiceInventory::signalled(&fixture.source.process_entered).await;
    let intent = fixture.source.intents.load_full()[0].clone();
    let mutation =
        crowdb_web::invalidate_service_observation_for_tests(&fixture.state, 1, &intent.service_id);
    let invalidated = fixture.rows().await;
    assert!(invalidated
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["node_id"] == 1)
        .unwrap()["pid"]
        .is_null());
    assert_eq!(fixture.wait_pid(2, Some(102)).await["pid"], 102);
    // The same durable operation may resume with a replacement process. Its
    // epoch still fences a delayed observation with the identical operation ID.
    fixture.source.pid.store(500, Ordering::Relaxed);
    mutation.complete(
        intent.clone(),
        &json!({"operation_id":intent.operation_id,"pid":501}),
    );
    assert_eq!(fixture.wait_pid(1, Some(501)).await["pid"], 501);
    fixture.source.hold_node.store(0, Ordering::Relaxed);
    fixture.source.process_release.notify_one();
    TestServiceInventory::signalled(&fixture.source.process_finished).await;
    for _ in 0..32 {
        assert_eq!(fixture.wait_pid(1, Some(501)).await["pid"], 501);
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn failed_or_unknown_mutation_does_not_publish_an_assumed_pid() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.start();
    fixture.wait_pid(1, Some(101)).await;
    fixture.source.hold_node.store(1, Ordering::Relaxed);
    TestServiceInventory::signalled(&fixture.source.process_entered).await;
    let mutation = crowdb_web::invalidate_service_observation_for_tests(&fixture.state, 1, "chunkdb-1");
    drop(mutation);
    assert!(fixture
        .rows()
        .await
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["node_id"] == 1)
        .unwrap()["pid"]
        .is_null());
    fixture.source.pid.store(800, Ordering::Relaxed);
    fixture.source.hold_node.store(0, Ordering::Relaxed);
    fixture.source.process_release.notify_one();
    TestServiceInventory::signalled(&fixture.source.process_finished).await;
    fixture.wait_pid(1, Some(801)).await;
}

#[tokio::test]
async fn an_unknown_outcome_also_fences_samples_started_during_the_mutation() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.start();
    fixture.wait_pid(1, Some(101)).await;
    let mutation = crowdb_web::invalidate_service_observation_for_tests(&fixture.state, 1, "chunkdb-1");
    fixture.source.hold_node.store(1, Ordering::Relaxed);
    TestServiceInventory::signalled(&fixture.source.process_entered).await;
    fixture.source.pid.store(800, Ordering::Relaxed);
    drop(mutation);
    fixture.source.hold_node.store(0, Ordering::Relaxed);
    fixture.source.process_release.notify_one();
    TestServiceInventory::signalled(&fixture.source.process_finished).await;
    let rows = fixture.rows().await;
    assert!(rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["node_id"] == 1)
        .unwrap()["pid"]
        .is_null());
    fixture.wait_pid(1, Some(801)).await;
}

#[tokio::test]
async fn a_slow_sibling_health_probe_does_not_delay_target_process_or_health_resampling() {
    let mut fixture = TestServiceInventory::new(false);
    let mut sibling = fixture.state.config.read().unwrap().servers[0].clone();
    sibling.id = "chunkdb-sibling".into();
    fixture.state.config.write().unwrap().servers.push(sibling);
    fixture.source.hold_health.store(true, Ordering::Relaxed);
    fixture.start();
    TestServiceInventory::signalled(&fixture.source.health_entered).await;
    fixture.wait_pid(1, Some(101)).await;
    let intent = fixture.source.intents.load_full()[0].clone();
    let mutation =
        crowdb_web::invalidate_service_observation_for_tests(&fixture.state, 1, &intent.service_id);
    fixture.source.pid.store(500, Ordering::Relaxed);
    mutation.complete(
        intent.clone(),
        &json!({"operation_id":intent.operation_id,"pid":501}),
    );
    drop(mutation);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let row = fixture.wait_pid(1, Some(501)).await;
            if row["health"] == "up" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("target readiness waited behind a sibling health probe");
}

#[tokio::test]
async fn fresh_bound_processes_remain_observable_when_authority_expires_but_access_health_is_unknown() {
    let mut fixture = TestServiceInventory::new(true);
    fixture.start();
    fixture.wait_pid(1, Some(101)).await;
    fixture.source.fail_authority.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(3150)).await;
    let row = fixture.wait_pid(1, Some(101)).await;
    assert_eq!(row["pid_node_id"], 1);
    assert_eq!(row["process_observed"], true);
    assert_eq!(row["health"], "unknown");
    assert_eq!(row["start_pending"], false);
}

#[tokio::test]
async fn changed_endpoint_wrong_bootstrap_and_wrong_executed_operation_never_reuse_a_pid() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.start();
    fixture.wait_pid(1, Some(101)).await;
    fixture.source.wrong_bootstrap.store(true, Ordering::Relaxed);
    fixture.wait_pid(1, None).await;
    fixture.source.wrong_bootstrap.store(false, Ordering::Relaxed);
    fixture.source.wrong_operation.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(fixture.wait_pid(1, None).await["pid_node_id"].is_null());
    fixture.source.wrong_operation.store(false, Ordering::Relaxed);
    fixture.wait_pid(1, Some(101)).await;
    fixture.state.config.write().unwrap().nodes[0].host = "127.0.0.99".into();
    let rows = fixture.rows().await;
    let row = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["node_id"] == 1)
        .unwrap();
    assert!(row["pid"].is_null());
    assert_eq!(row["process_observed"], false);
    assert_eq!(row["health"], "unknown");
    std::fs::remove_file(fixture.root.path().join("node-binding.json")).unwrap();
    for row in fixture.rows().await.as_array().unwrap() {
        assert!(row["pid"].is_null());
    }
}

#[tokio::test]
async fn pending_start_uses_only_the_current_operation_exhaustion() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.source.pid.store(0, Ordering::Relaxed);
    fixture.source.exhausted.store(true, Ordering::Relaxed);
    fixture.source.wrong_operation.store(true, Ordering::Relaxed);
    fixture.start();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(fixture.wait_pid(1, None).await["start_pending"], true);
    fixture.source.wrong_operation.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(fixture.wait_pid(1, None).await["start_pending"], false);
}

#[tokio::test]
async fn a_late_authority_sample_cannot_resurrect_a_deleted_registration() {
    let mut fixture = TestServiceInventory::new(false);
    fixture.start();
    fixture.wait_pid(1, Some(101)).await;
    fixture.source.hold_authority.store(true, Ordering::Relaxed);
    TestServiceInventory::signalled(&fixture.source.authority_entered).await;
    let mutation = crowdb_web::invalidate_service_observation_for_tests(&fixture.state, 1, "chunkdb-1");
    let mut intents = fixture.source.intents.load_full().as_ref().clone();
    intents[0].action = NodeServiceAction::Delete;
    intents[0].operation_id = "delete-operation".into();
    fixture.source.intents.store(Arc::new(intents.clone()));
    mutation.complete(
        intents[0].clone(),
        &json!({"operation_id":"delete-operation","pid":null}),
    );
    assert_eq!(fixture.rows().await.as_array().unwrap().len(), 1);
    fixture.source.hold_authority.store(false, Ordering::Relaxed);
    fixture.source.authority_release.notify_one();
    TestServiceInventory::signalled(&fixture.source.authority_finished).await;
    for _ in 0..32 {
        assert_eq!(fixture.rows().await.as_array().unwrap().len(), 1);
        tokio::task::yield_now().await;
    }
}
