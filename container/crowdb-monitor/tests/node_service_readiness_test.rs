// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/node_setup.rs"]
mod node_setup;

use crowdb_monitor::{DeploymentProfile, NodeIdentity, NodeServiceForTests};
use crowdb_protocol::mgmt::node::{setup::NodeSetupRequest, NodeServiceAction, NodeServiceIntent};
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
async fn readiness_timeout_preserves_the_live_process_and_start_budget() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("service-ready");
    let root = directory.path();
    let request = NodeSetupRequest {
        version: 1,
        discovery_id: NodeIdentity::load_or_create(root).unwrap().uuid().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: crowdb_protocol::mgmt::node::setup::NodeSetupChoices::default(),
    };
    let authority = node_setup::TestAuthority::start(root, &request).await;
    assert_eq!(authority.registry.nodes[0].node_id, 2);
    let mut profile = DeploymentProfile::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"),
    )
    .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.log_root = root.join("log");
    profile.paths.run_root = root.join("run");
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    let program = root.join("starting-service");
    std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "chunkdb")
        .unwrap()
        .program = program;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut intent = NodeServiceIntent {
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 2,
        service_id: "chunkdb-2".into(),
        kind: "chunkdb".into(),
        action: NodeServiceAction::Start,
        configuration: format!(
            "[server]\nhttp_listen_addr = '{}'\n",
            listener.local_addr().unwrap()
        ),
        environment: std::collections::BTreeMap::default(),
    };
    let key = b"/deployment/services/2/chunkdb-2";
    authority
        .kv
        .put(0, 0, key, &serde_json::to_vec(&intent).unwrap(), None)
        .await
        .unwrap();
    let mut worker = NodeServiceForTests::new_for_tests(profile).await.unwrap();
    let error = worker.execute_for_tests(intent.clone()).await.unwrap_err();
    assert!(error.to_string().contains("readiness failed"), "{error}");
    let state = root.join("services/chunkdb-2/execution.json");
    let mut pending: serde_json::Value = serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    assert_eq!(pending["attempts"], 1);
    let pid = pending["pid"].as_u64().expect("live process remains observable");
    // The last permitted launch can still be starting. Its live process must
    // remain observable without authorizing a sixth launch.
    pending["attempts"] = serde_json::json!(5);
    std::fs::write(&state, serde_json::to_vec(&pending).unwrap()).unwrap();
    assert!(worker.pending_for_tests(&intent).unwrap());
    assert_eq!(
        worker.failures_for_tests().unwrap()["chunkdb-2"]["exhausted"],
        false
    );
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route("/ready", axum::routing::get(|| async { "ready" })),
        )
        .await
        .unwrap();
    });
    let ready = worker.execute_for_tests(intent.clone()).await.unwrap();
    assert_eq!(
        ready["pid"], pid,
        "observing readiness must not restart the process"
    );
    assert!(!worker.pending_for_tests(&intent).unwrap());
    intent.action = NodeServiceAction::Stop;
    intent.operation_id = uuid::Uuid::new_v4().to_string();
    authority
        .kv
        .put(0, 0, key, &serde_json::to_vec(&intent).unwrap(), None)
        .await
        .unwrap();
    worker.execute_for_tests(intent).await.unwrap();
    server.abort();
}
