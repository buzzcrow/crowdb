// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/member_trust.rs"]
mod trust;

use crowdb_monitor::{
    accept_member_trust_for_tests, accept_node_policy, DeploymentProfile, NodeServiceForTests, StatusStore,
};
use crowdb_protocol::mgmt::node::deployment::NodeDeploymentOverrides;
use std::{
    os::unix::fs::PermissionsExt,
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
    time::Duration,
};

#[tokio::test]
async fn retained_quorum_startup_does_not_block_discovery_or_confuse_liveness_with_health() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("kv-resume");
    let root = directory.path();
    accept_node_policy(
        root,
        &NodeDeploymentOverrides::default(),
        &NodeDeploymentOverrides {
            initial_voters: Some(3),
            ..Default::default()
        },
    )
    .unwrap();
    let trust = trust::fixture(root);
    accept_member_trust_for_tests(root, &trust).unwrap();
    let mut profile = DeploymentProfile::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"),
    )
    .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.log_root = root.join("log");
    profile.paths.run_root = root.join("run");
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    std::fs::create_dir_all(&profile.paths.run_root).unwrap();
    let program = root.join("live-process");
    std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    for service in &mut profile.services {
        service.program = program.clone();
        if service.id == "kv" {
            service.probe.target = format!("http://{}/health", listener.local_addr().unwrap());
            service.probe.timeout_ms = 100;
        }
    }
    let status = StatusStore::new(&profile.paths.run_root).unwrap();
    let mut worker = NodeServiceForTests::new_for_tests(profile).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), worker.resume_for_tests())
        .await
        .expect("discovery startup cannot wait for KV quorum health")
        .unwrap();
    worker.tick_for_tests().await.unwrap();
    let first = status.read(Duration::from_secs(3)).unwrap().services["kv"].clone();
    assert!(!first.healthy);
    assert!(first.pid.is_some());
    assert_eq!(first.restart_attempts, 0);
    for _ in 0..6 {
        worker.tick_for_tests().await.unwrap();
        assert_eq!(status.read(Duration::from_secs(3)).unwrap().services["kv"], first);
    }
    let ready = Arc::new(AtomicBool::new(true));
    let sampled = ready.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/health",
                axum::routing::get(move || {
                    let ready = sampled.clone();
                    async move {
                        if ready.load(Ordering::Acquire) {
                            axum::http::StatusCode::OK
                        } else {
                            axum::http::StatusCode::SERVICE_UNAVAILABLE
                        }
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    worker.tick_for_tests().await.unwrap();
    let recovered = status.read(Duration::from_secs(3)).unwrap().services["kv"].clone();
    assert!(recovered.healthy);
    assert_eq!(recovered.pid, first.pid);
    assert_eq!(recovered.restart_attempts, 0);
    ready.store(false, Ordering::Release);
    worker.stop_kv_for_tests().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), worker.tick_for_tests())
        .await
        .expect("restarting a retained replica cannot wait for quorum health")
        .unwrap();
    let restarted = status.read(Duration::from_secs(3)).unwrap().services["kv"].clone();
    assert!(!restarted.healthy);
    assert_ne!(restarted.pid, first.pid);
    assert_eq!(restarted.restart_attempts, 1);
    std::fs::write(root.join("ssh/ssh_host_ed25519_key.pub"), trust::key(4)).unwrap();
    assert!(
        worker.resume_for_tests().await.is_err(),
        "live processes cannot bypass retained key validation"
    );
    server.abort();
}
