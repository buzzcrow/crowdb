// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{extract::State, http::StatusCode, routing::get, Router};
use crowdb_monitor::{DeploymentProfile, MonitorStatus, NodeServiceForTests, StatusStore};
use crowdb_protocol::mgmt::{node::NodeBinding, SystemBootstrapIdentity};
use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

pub struct TestHealth {
    pub worker: NodeServiceForTests,
    pub ready: Arc<AtomicBool>,
    pub entered: Arc<tokio::sync::Notify>,
    profile: DeploymentProfile,
    servers: Vec<tokio::task::JoinHandle<()>>,
    _directory: crowdb_test_harness::test_dirs::TestDir,
}

impl TestHealth {
    pub async fn new() -> Self {
        let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-health-authority");
        let root = directory.path();
        let ready = Arc::new(AtomicBool::new(true));
        let entered = Arc::new(tokio::sync::Notify::new());
        let (health, health_task) = health_server(Arc::clone(&ready)).await;
        let (authority, authority_task) = slow_authority(Arc::clone(&entered)).await;
        let profile = profile(root, &health);
        let binding = NodeBinding {
            node_id: 2,
            bootstrap: SystemBootstrapIdentity {
                cluster_id: uuid::Uuid::new_v4().to_string(),
                operation_id: uuid::Uuid::new_v4().to_string(),
                configuration_digest: "00".repeat(32),
            },
            management_seeds: vec![authority],
        };
        let binding_path = root.join("node-binding.json");
        std::fs::write(&binding_path, serde_json::to_vec(&binding).unwrap()).unwrap();
        std::fs::set_permissions(binding_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut worker = NodeServiceForTests::new_for_tests(profile.clone()).await.unwrap();
        worker.start_kv_child_for_tests().await.unwrap();
        Self {
            worker,
            ready,
            entered,
            profile,
            servers: vec![health_task, authority_task],
            _directory: directory,
        }
    }

    pub fn snapshot(&self) -> MonitorStatus {
        StatusStore::open(&self.profile.paths.run_root)
            .unwrap()
            .read(Duration::from_secs(3))
            .unwrap()
    }
}

impl Drop for TestHealth {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

fn profile(root: &Path, health: &str) -> DeploymentProfile {
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.log_root = root.join("log");
    profile.paths.run_root = root.join("run");
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    std::fs::create_dir_all(&profile.paths.run_root).unwrap();
    for service in &mut profile.services {
        if matches!(service.id.as_str(), "web" | "kv") {
            service.program = "/bin/sleep".into();
            service.args = vec!["60".into()];
            service.probe.target = format!("{health}/health");
            service.additional_probes.clear();
        }
    }
    profile
}

async fn health_server(ready: Arc<AtomicBool>) -> (String, tokio::task::JoinHandle<()>) {
    async fn health(State(ready): State<Arc<AtomicBool>>) -> StatusCode {
        if ready.load(Ordering::Acquire) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/health", get(health)).with_state(ready),
        )
        .await
        .unwrap();
    });
    (origin, task)
}

async fn slow_authority(entered: Arc<tokio::sync::Notify>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let app = Router::new().route(
            "/topology",
            get(move || {
                let entered = Arc::clone(&entered);
                async move {
                    entered.notify_one();
                    std::future::pending::<StatusCode>().await
                }
            }),
        );
        axum::serve(listener, app).await.unwrap();
    });
    (origin, task)
}
