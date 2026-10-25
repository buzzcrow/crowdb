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
    std::fs::create_dir_all(&profile.paths.run_root).unwrap();
    let program = root.join("starting-service");
    std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    let child = profile
        .services
        .iter_mut()
        .find(|service| service.id == "chunkdb")
        .unwrap();
    child.program = program;
    // This probe test owns a dummy child; the dependency gate is covered below.
    child.dependencies = vec!["kv".into()];
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
    let pending_error = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        worker.execute_for_tests(intent.clone()),
    )
    .await
    .expect("a continuing readiness observation must not occupy another full startup window")
    .unwrap_err();
    assert!(
        pending_error.to_string().contains("readiness failed"),
        "{pending_error}"
    );
    let observed: serde_json::Value = serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    assert_eq!(observed["pid"], pid);
    assert_eq!(observed["attempts"], 5);
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

#[tokio::test]
async fn recovery_waits_for_live_dependencies_without_consuming_launches() {
    // Use the real Group-0 authority and process manager for recovery.

    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("service-dependencies");
    let data = directory.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let root = data.as_path();
    let request = NodeSetupRequest {
        version: 1,
        discovery_id: NodeIdentity::load_or_create(root).unwrap().uuid().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: crowdb_protocol::mgmt::node::setup::NodeSetupChoices::default(),
    };
    let authority = node_setup::TestAuthority::start(root, &request).await;
    let mut profile = DeploymentProfile::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"),
    )
    .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.log_root = root.join("log");
    profile.paths.install_root = directory.path().to_path_buf();
    profile.paths.bin_root = directory.path().join("bin");
    profile.paths.template_root = directory.path().join("templates");
    profile.paths.run_root = directory.path().join("run");
    std::fs::create_dir_all(&profile.paths.bin_root).unwrap();
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    std::fs::create_dir_all(&profile.paths.run_root).unwrap();
    let program = profile.paths.bin_root.join("starting-service");
    std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    for service in &mut profile.services {
        service.program = program.clone();
        if service.id == "chunkdb" {
            service.dependencies = vec!["diskdb".into()];
        }
    }
    let mut listeners = Vec::new();
    let mut intents = Vec::new();
    for kind in ["diskdb", "chunkdb"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let intent = NodeServiceIntent {
            cluster_id: authority.binding.bootstrap.cluster_id.clone(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            node_id: authority.binding.node_id,
            service_id: format!("{kind}-2"),
            kind: kind.into(),
            action: NodeServiceAction::Start,
            configuration: format!(
                "[server]\nhttp_listen_addr = '{}'\n",
                listener.local_addr().unwrap()
            ),
            environment: std::collections::BTreeMap::default(),
        };
        let key = format!("/deployment/services/2/{}", intent.service_id);
        authority
            .kv
            .put(0, 0, key.as_bytes(), &serde_json::to_vec(&intent).unwrap(), None)
            .await
            .unwrap();
        listeners.push(listener);
        intents.push(intent);
    }
    profile.validate().unwrap();
    let mut worker = NodeServiceForTests::new_for_tests(profile).await.unwrap();
    worker.reconcile_for_tests().await.unwrap();
    assert!(
        !root.join("services/chunkdb-2/execution.json").exists(),
        "waiting for a dependency must not launch a process or consume its budget"
    );
    let servers: Vec<_> = listeners
        .into_iter()
        .map(|listener| {
            tokio::spawn(async move {
                axum::serve(
                    listener,
                    axum::Router::new().route("/ready", axum::routing::get(|| async { "ready" })),
                )
                .await
                .unwrap();
            })
        })
        .collect();
    worker.reconcile_for_tests().await.unwrap();
    let execution: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("services/chunkdb-2/execution.json")).unwrap())
            .unwrap();
    assert_eq!(execution["complete"], true);
    assert_eq!(execution["intent"]["operation_id"], intents[1].operation_id);
    assert!(execution["pid"].is_number());
    for mut intent in intents {
        intent.action = NodeServiceAction::Stop;
        intent.operation_id = uuid::Uuid::new_v4().to_string();
        let key = format!("/deployment/services/2/{}", intent.service_id);
        authority
            .kv
            .put(0, 0, key.as_bytes(), &serde_json::to_vec(&intent).unwrap(), None)
            .await
            .unwrap();
    }
    worker.reconcile_for_tests().await.unwrap();
    for server in servers {
        server.abort();
    }
}
