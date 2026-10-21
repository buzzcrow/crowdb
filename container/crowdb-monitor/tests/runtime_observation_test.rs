// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::runtime_observation_for_tests;
use crowdb_protocol::mgmt::{cluster::DependencyState, node::NodeServiceAction};
use std::time::Duration;

#[path = "common/runtime_observation.rs"]
mod runtime_observation;
use runtime_observation::TestObservation;

#[tokio::test]
async fn health_requires_the_executed_operation_and_current_owned_process() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("runtime-health");
    let mut fixture = TestObservation::start(root.path()).await;
    let report = runtime_observation_for_tests(&fixture.profile, &mut fixture.manager).unwrap();
    assert_eq!(report.services[0].state, DependencyState::Ready);
    fixture.save(false, 1, None);
    assert_eq!(
        runtime_observation_for_tests(&fixture.profile, &mut fixture.manager)
            .unwrap()
            .services[0]
            .state,
        DependencyState::Initializing
    );
    fixture.intent.operation_id = uuid::Uuid::new_v4().to_string();
    fixture.save(true, 0, None);
    let report = runtime_observation_for_tests(&fixture.profile, &mut fixture.manager).unwrap();
    assert_eq!(report.services[0].state, DependencyState::Initializing);
    assert_eq!(
        report.services[0].operation_id.as_ref(),
        Some(&fixture.intent.operation_id)
    );
    fixture.status.services.get_mut("chunkdb-1").unwrap().operation_id =
        Some(fixture.intent.operation_id.clone());
    fixture.store.publish(&mut fixture.status).unwrap();
    assert_eq!(
        runtime_observation_for_tests(&fixture.profile, &mut fixture.manager)
            .unwrap()
            .services[0]
            .state,
        DependencyState::Ready
    );
    fixture
        .manager
        .stop("chunkdb-1", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        runtime_observation_for_tests(&fixture.profile, &mut fixture.manager)
            .unwrap()
            .services[0]
            .state,
        DependencyState::Initializing
    );
    fixture.intent.action = NodeServiceAction::Stop;
    fixture.save(true, 0, None);
    assert_eq!(
        runtime_observation_for_tests(&fixture.profile, &mut fixture.manager)
            .unwrap()
            .services[0]
            .state,
        DependencyState::Stopped
    );
}

#[tokio::test]
async fn stale_health_and_terminal_failures_are_distinct_and_do_not_export_error_contents() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("runtime-failure");
    let mut fixture = TestObservation::start(root.path()).await;
    let path = fixture.profile.paths.run_root.join("status/monitor.json");
    let mut stale = serde_json::to_value(&fixture.status).unwrap();
    stale["updated_at_ms"] = serde_json::json!(0);
    std::fs::write(path, serde_json::to_vec(&stale).unwrap()).unwrap();
    assert_eq!(
        runtime_observation_for_tests(&fixture.profile, &mut fixture.manager)
            .unwrap()
            .services[0]
            .state,
        DependencyState::Initializing
    );
    fixture.save(false, 5, Some("private-error-sentinel"));
    let report = runtime_observation_for_tests(&fixture.profile, &mut fixture.manager).unwrap();
    assert_eq!(report.services[0].state, DependencyState::Failed);
    assert!(!serde_json::to_string(&report)
        .unwrap()
        .contains("private-error-sentinel"));
    fixture
        .manager
        .stop("chunkdb-1", Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn observation_never_creates_a_missing_identity_or_status_directory() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("observation-readonly");
    let profile = runtime_observation::profile(root.path());
    std::fs::create_dir(&profile.paths.log_root).unwrap();
    let mut manager =
        crowdb_monitor::ProcessManager::new(profile.paths.log_root.clone(), profile.logs.clone())
            .await
            .unwrap();
    assert!(runtime_observation_for_tests(&profile, &mut manager).is_err());
    assert!(!root.path().join("node-identity").exists());
    assert!(!profile.paths.run_root.exists());
    assert!(!root.path().join("node-policy.json").exists());
}
