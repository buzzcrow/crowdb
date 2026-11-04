// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::node_setup;
use crowdb_monitor::{DeploymentProfile, NodeIdentity, NodeServiceForTests};
use crowdb_protocol::mgmt::node::{
    setup::{NodeSetupChoices, NodeSetupRequest},
    NodeServiceAction, NodeServiceIntent,
};
use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub struct TestManagedHealth {
    pub worker: NodeServiceForTests,
    pub ready: Arc<AtomicBool>,
    pub slow: Arc<AtomicBool>,
    pub intent: NodeServiceIntent,
    pub profile: DeploymentProfile,
    _authority: node_setup::TestAuthority,
    server: tokio::task::JoinHandle<()>,
    _directory: crowdb_test_harness::test_dirs::TestDir,
}

impl TestManagedHealth {
    pub async fn new() -> Self {
        let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("managed-health");
        let data = directory.path().join("data");
        std::fs::create_dir(&data).unwrap();
        let root = data.as_path();
        let request = NodeSetupRequest {
            version: 1,
            discovery_id: NodeIdentity::load_or_create(root).unwrap().uuid().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            choices: NodeSetupChoices::default(),
        };
        let authority = node_setup::TestAuthority::start(root, &request).await;
        assert_eq!(authority.registry.nodes[0].node_id, 2);
        let ready = Arc::new(AtomicBool::new(true));
        let slow = Arc::new(AtomicBool::new(false));
        let (address, server) = health_server(Arc::clone(&ready), Arc::clone(&slow)).await;
        let mut profile = DeploymentProfile::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"),
        )
        .unwrap();
        profile.paths.data_root = root.to_path_buf();
        profile.paths.install_root = root.parent().unwrap().to_path_buf();
        profile.paths.bin_root = root.join("bin");
        profile.paths.template_root = root.join("templates");
        profile.paths.log_root = root.join("log");
        profile.paths.run_root = directory.path().join("run");
        std::fs::create_dir_all(&profile.paths.log_root).unwrap();
        std::fs::create_dir_all(&profile.paths.run_root).unwrap();
        std::fs::create_dir_all(&profile.paths.bin_root).unwrap();
        let program = profile.paths.bin_root.join("owned-child");
        std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        for service in &mut profile.services {
            service.program.clone_from(&program);
            service.config_template = None;
            if matches!(service.id.as_str(), "web" | "kv" | "chunkdb") {
                service.probe.target = format!("http://{address}/health");
                service.additional_probes.clear();
                // Keep the service executor and real Group-0 authority; this
                // fixture isolates readiness from storage dependency assembly.
                service.dependencies.clear();
            }
        }
        profile.validate().unwrap();
        let intent = NodeServiceIntent {
            cluster_id: authority.binding.bootstrap.cluster_id.clone(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            node_id: 2,
            service_id: "chunkdb-2".into(),
            kind: "chunkdb".into(),
            action: NodeServiceAction::Start,
            configuration: format!("[server]\nhttp_listen_addr = \"{address}\"\n"),
            environment: BTreeMap::new(),
        };
        authority
            .kv
            .put(
                0,
                0,
                b"/deployment/services/2/chunkdb-2",
                &serde_json::to_vec(&intent).unwrap(),
                None,
            )
            .await
            .unwrap();
        let mut worker = NodeServiceForTests::new_for_tests(profile.clone()).await.unwrap();
        worker.start_kv_child_for_tests().await.unwrap();
        worker.execute_for_tests(intent.clone()).await.unwrap();
        Self {
            worker,
            ready,
            slow,
            intent,
            profile,
            _authority: authority,
            server,
            _directory: directory,
        }
    }

    pub fn execution(&self) -> serde_json::Value {
        serde_json::from_slice(
            &std::fs::read(
                self.profile
                    .paths
                    .data_root
                    .join("services/chunkdb-2/execution.json"),
            )
            .unwrap(),
        )
        .unwrap()
    }

    pub async fn stop(&mut self) {
        for id in ["chunkdb-2", "kv", "web"] {
            self.worker.stop_child_for_tests(id).await.unwrap();
        }
    }
}

impl Drop for TestManagedHealth {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn health_server(
    ready: Arc<AtomicBool>,
    slow: Arc<AtomicBool>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let observed = Arc::clone(&ready);
    let health_slow = Arc::clone(&slow);
    let ready_slow = Arc::clone(&slow);
    let server = tokio::spawn(async move {
        let app = axum::Router::new()
            .route(
                "/health",
                axum::routing::get(move || {
                    let slow = Arc::clone(&health_slow);
                    async move {
                        if slow.load(Ordering::Acquire) {
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                        "healthy"
                    }
                }),
            )
            .route(
                "/ready",
                axum::routing::get(move || {
                    let ready = Arc::clone(&observed);
                    let slow = Arc::clone(&ready_slow);
                    async move {
                        if slow.load(Ordering::Acquire) {
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                        if ready.load(Ordering::Acquire) {
                            axum::http::StatusCode::OK
                        } else {
                            axum::http::StatusCode::SERVICE_UNAVAILABLE
                        }
                    }
                }),
            );
        axum::serve(listener, app).await.unwrap();
    });
    (address, server)
}
