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
    let (request, _authority, profile) = node_setup_file::prepare(root, false).await;
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

#[tokio::test]
async fn storage_consumers_wait_for_disk_registration_after_prerequisite_services_are_ready() {
    use crowdb_protocol::mgmt::node::{setup::SetupServiceKind, NodeControl};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("setup-order");
    let root = directory.path();
    let (_request, _authority, profile) = node_setup_file::prepare(root, true).await;
    let path = root.join("node-setup.json");
    let mut state: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    for step in state["status"]["services"].as_array_mut().unwrap() {
        if ["paxos-kv", "diskdb", "diskio"].contains(&step["kind"].as_str().unwrap()) {
            step["phase"] = serde_json::json!("ready");
        }
    }
    std::fs::write(path, serde_json::to_vec(&state).unwrap()).unwrap();
    let listener = tokio::net::UnixListener::bind(profile.paths.run_root.join("node-control.sock")).unwrap();
    let data = root.to_path_buf();
    let worker = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            stream.read_to_end(&mut bytes).await.unwrap();
            let response = match serde_json::from_slice::<NodeControl>(&bytes).unwrap() {
                NodeControl::PrepareDiskFile { request } => {
                    crowdb_monitor::accept_file_for_tests(&data, &request).unwrap();
                    serde_json::json!({})
                }
                NodeControl::Processes { .. } => serde_json::json!({"processes":{},"service_failures":{}}),
                _ => panic!("unexpected setup control"),
            };
            stream
                .write_all(&serde_json::to_vec(&serde_json::json!({"ok":true,"value":response})).unwrap())
                .await
                .unwrap();
        }
    });
    NodeSetupForTests::advance_for_tests(profile).await.unwrap();
    worker.await.unwrap();
    let status = NodeSetupForTests::status_for_tests(root).unwrap().unwrap();
    assert_eq!(status.phase, NodeSetupPhase::Running);
    assert_ne!(status.disks[0].phase, SetupStepPhase::Ready);
    for kind in [
        SetupServiceKind::Chunkdb,
        SetupServiceKind::ChunkKv,
        SetupServiceKind::AccessServer,
    ] {
        let step = status.services.iter().find(|step| step.kind == kind).unwrap();
        assert!(
            step.operation_id.is_none(),
            "storage is still unregistered: {step:?}"
        );
    }
}
