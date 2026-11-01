// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/service_replacement.rs"]
mod fixture;
#[path = "common/node_setup.rs"]
mod node_setup;
use fixture::TestReplacement;
use serde_json::{json, Value};

#[tokio::test]
async fn supervised_kv_replacements_and_legacy_receipts_refresh_the_pid_without_another_restart() {
    let TestReplacement {
        directory: _directory,
        authority,
        mut worker,
        intent,
        original,
        health,
        state,
    } = TestReplacement::new().await;
    worker.stop_kv_for_tests().await.unwrap();
    let replacement = worker.start_kv_child_for_tests().await.unwrap();
    assert_ne!(replacement, original);
    assert!(worker.pending_for_tests(&intent).unwrap());
    assert!(worker
        .owned_processes_for_tests(&intent.cluster_id, intent.node_id)
        .unwrap()["process_operations"]["kv"]
        .is_null());
    let result = worker.execute_for_tests(intent.clone()).await.unwrap();
    assert_eq!(result["pid"], replacement);
    assert!(!worker.pending_for_tests(&intent).unwrap());
    let observed = worker
        .owned_processes_for_tests(&intent.cluster_id, intent.node_id)
        .unwrap();
    assert_eq!(observed["processes"]["kv"], replacement);
    assert_eq!(observed["process_operations"]["kv"], intent.operation_id);
    let mut record: Value = serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    record.as_object_mut().unwrap().remove("bootstrap");
    std::fs::write(&state, serde_json::to_vec(&record).unwrap()).unwrap();
    assert!(worker.pending_for_tests(&intent).unwrap());
    assert_eq!(
        worker.execute_for_tests(intent.clone()).await.unwrap()["pid"],
        replacement
    );
    let migrated: Value = serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    assert_eq!(migrated["bootstrap"], json!(authority.binding.bootstrap));
    let mut foreign = migrated.clone();
    foreign["bootstrap"]["operation_id"] = json!("foreign-bootstrap");
    std::fs::write(&state, serde_json::to_vec(&foreign).unwrap()).unwrap();
    assert!(worker.pending_for_tests(&intent).unwrap());
    assert!(worker
        .execute_for_tests(intent.clone())
        .await
        .unwrap_err()
        .to_string()
        .contains("another bootstrap"));
    assert_eq!(
        worker
            .owned_processes_for_tests(&intent.cluster_id, intent.node_id)
            .unwrap()["processes"]["kv"],
        replacement
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&state).unwrap()).unwrap(),
        foreign
    );
    worker.stop_kv_for_tests().await.unwrap();
    health.abort();
}
