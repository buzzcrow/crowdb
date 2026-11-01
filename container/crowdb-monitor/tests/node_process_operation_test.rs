// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/runtime_observation.rs"]
mod fixture;
use crowdb_monitor::NodeServiceForTests;
use crowdb_protocol::mgmt::{node::NodeBinding, SystemBootstrapIdentity};
use serde_json::{json, Value};
use std::{os::unix::fs::PermissionsExt, time::Duration};

#[tokio::test]
async fn process_operations_and_failures_require_the_actual_pid_and_current_bound_execution() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("process-operation");
    let mut test = fixture::TestObservation::start(directory.path()).await;
    let binding = NodeBinding {
        node_id: test.intent.node_id,
        bootstrap: SystemBootstrapIdentity {
            cluster_id: test.intent.cluster_id.clone(),
            operation_id: "bootstrap".into(),
            configuration_digest: "a".repeat(64),
        },
        management_seeds: vec!["http://127.0.0.1:1".into()],
    };
    std::fs::write(
        directory.path().join("node-binding.json"),
        serde_json::to_vec(&binding).unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(
        directory.path().join("node-binding.json"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let state = directory.path().join("services/chunkdb-1/execution.json");
    let mut record: Value = serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    record["bootstrap"] = json!(binding.bootstrap);
    record["attempts"] = json!(5);
    record["error"] = json!("retained diagnostic");
    std::fs::write(&state, serde_json::to_vec(&record).unwrap()).unwrap();
    let pid = test.manager.pid("chunkdb-1").unwrap();
    assert_eq!(
        test.store.read(Duration::from_secs(3)).unwrap().services["chunkdb-1"].pid,
        Some(pid)
    );
    let read = |manager: &mut crowdb_monitor::ProcessManager| {
        NodeServiceForTests::processes_for_tests(
            directory.path(),
            manager,
            &binding.bootstrap.cluster_id,
            binding.node_id,
        )
        .unwrap()
    };
    let observed = read(&mut test.manager);
    assert_eq!(observed["bootstrap"], json!(binding.bootstrap));
    assert_eq!(observed["processes"]["chunkdb-1"], pid);
    assert_eq!(
        observed["process_operations"]["chunkdb-1"],
        test.intent.operation_id
    );
    assert_eq!(observed["service_failures"]["chunkdb-1"]["exhausted"], false);
    for fault in ["legacy", "bootstrap", "cluster", "node", "pid"] {
        let mut invalid = record.clone();
        match fault {
            "legacy" => {
                invalid.as_object_mut().unwrap().remove("bootstrap");
            }
            "bootstrap" => invalid["bootstrap"]["operation_id"] = json!("old-bootstrap"),
            "cluster" => invalid["intent"]["cluster_id"] = json!("other-cluster"),
            "node" => invalid["intent"]["node_id"] = json!(99),
            "pid" => invalid["pid"] = json!(pid + 100_000),
            _ => unreachable!(),
        }
        std::fs::write(&state, serde_json::to_vec(&invalid).unwrap()).unwrap();
        let observed = read(&mut test.manager);
        assert_eq!(
            observed["processes"]["chunkdb-1"], pid,
            "actual children remain observable: {fault}"
        );
        assert!(
            observed["process_operations"]["chunkdb-1"].is_null(),
            "foreign execution labeled the live PID: {fault}"
        );
        if fault != "pid" {
            assert!(
                observed["service_failures"]["chunkdb-1"].is_null(),
                "foreign execution exhausted the current start: {fault}"
            );
        }
    }
    test.manager
        .stop("chunkdb-1", Duration::from_secs(1))
        .await
        .unwrap();
}
