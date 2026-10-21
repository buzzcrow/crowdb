// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::NodeSetupForTests;
use crowdb_protocol::mgmt::node::setup::{NodeSetupPhase, SetupStepPhase};
use node_setup_file::fail_file_request;
use std::path::Path;

#[path = "common/node_setup.rs"]
mod node_setup;
#[path = "common/node_setup_file.rs"]
mod node_setup_file;
#[path = "common/node_setup_groups.rs"]
mod node_setup_groups;

#[tokio::test]
async fn failed_file_is_terminal_and_explicit_retry_preserves_its_operation_and_geometry() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("file-fail");
    let root = directory.path();
    let (request, _authority, profile) = node_setup_file::prepare(root).await;
    let socket = profile.paths.run_root.join("node-control.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let worker = tokio::spawn(fail_file_request(listener, root.to_path_buf()));
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    let original = worker.await.unwrap();
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    let failed = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(failed.phase, NodeSetupPhase::Failed);
    assert_eq!(failed.disks[0].phase, SetupStepPhase::Failed);
    assert!(failed.disks[0]
        .detail
        .as_ref()
        .unwrap()
        .contains("insufficient backing space"));
    assert!(failed.error.unwrap().contains("explicit retry"));
    assert!(!Path::new(&original.path).exists());
    let operation = original.request.operation_id.clone();
    let path = root.join(format!("disk-file-{operation}.json"));
    let before = std::fs::read(&path).unwrap();
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let status = NodeSetupForTests::default()
        .accept_for_tests(root, request)
        .unwrap();
    assert_eq!(status.phase, NodeSetupPhase::Accepted);
    std::fs::remove_file(&socket).unwrap();
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let worker = tokio::spawn(fail_file_request(listener, root.to_path_buf()));
    NodeSetupForTests::advance_for_tests(profile.clone())
        .await
        .unwrap();
    let retried = worker.await.unwrap();
    assert_eq!(retried, original);
    let journal: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("node-setup.json")).unwrap()).unwrap();
    assert!(journal["retry_disks"].as_array().unwrap().is_empty());
    NodeSetupForTests::advance_for_tests(profile).await.unwrap();
    assert_eq!(
        NodeSetupForTests::status_for_tests(root).unwrap().unwrap().phase,
        NodeSetupPhase::Failed
    );
    assert!(!retried.request.sparse);
    assert!(!Path::new(&retried.path).exists());
}
