// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{node_health_for_tests, DeploymentProfile, MonitorPhase, ProcessManager};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
async fn web_health_uses_its_probe_and_recovers_without_restarting_a_live_process() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-web-health");
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.path().to_path_buf();
    profile.paths.run_root = root.path().join("run");
    profile.paths.log_root = root.path().join("log");
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    std::fs::create_dir_all(&profile.paths.run_root).unwrap();
    let ready = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&ready);
    let router = axum::Router::new().route(
        "/healthz",
        axum::routing::get(move || {
            let ready = Arc::clone(&observed);
            async move {
                if ready.load(Ordering::Acquire) {
                    axum::http::StatusCode::OK
                } else {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut web = profile
        .services
        .iter()
        .find(|service| service.id == "web")
        .unwrap()
        .clone();
    web.program = "/bin/sleep".into();
    web.args = vec!["60".into()];
    web.probe.target = format!("http://{address}/healthz");
    let mut manager = ProcessManager::new(profile.paths.log_root.clone(), profile.logs.clone())
        .await
        .unwrap();
    let pid = manager.start(&web, &BTreeMap::new()).await.unwrap();
    let unhealthy = node_health_for_tests(&profile, &web, &mut manager).await.unwrap();
    ready.store(true, Ordering::Release);
    let recovered = node_health_for_tests(&profile, &web, &mut manager).await.unwrap();
    let retained_pid = manager.pid("web");
    manager.stop("web", Duration::from_secs(2)).await.unwrap();
    server.abort();
    assert!(!unhealthy.services["web"].healthy);
    assert_eq!(unhealthy.phase, MonitorPhase::Restarting);
    assert!(recovered.services["web"].healthy);
    assert_eq!(recovered.phase, MonitorPhase::Ready);
    assert_eq!(unhealthy.services["web"].pid, Some(pid));
    assert_eq!(recovered.services["web"].pid, Some(pid));
    assert_eq!(retained_pid, Some(pid));
    assert_eq!(recovered.services["web"].restart_attempts, 0);
}
