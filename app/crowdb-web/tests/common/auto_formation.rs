// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{routing::get, Json, Router};
use crowdb_protocol::mgmt::node::{
    deployment::{DeploymentMode, FormationPolicy, NodeDeploymentOverrides, NodeDeploymentPolicy},
    CandidateNode, CandidateSnapshot, CandidateState, NodeAdvertisement, NodeHandshake, NodeHardware,
    NODE_PROTOCOL_VERSION,
};
use crowdb_web::{start_node_auto_join, AppState};

pub struct TestFormationFixture {
    pub root: std::path::PathBuf,
    pub state: AppState,
    monitor: tokio::task::JoinHandle<()>,
    runner: tokio::task::JoinHandle<()>,
}

pub fn policy(voters: u8, mode: DeploymentMode) -> NodeDeploymentPolicy {
    NodeDeploymentOverrides {
        initial_voters: Some(voters),
        mode: Some(mode),
        ..Default::default()
    }
    .resolve(&NodeDeploymentOverrides::default(), None)
    .unwrap()
}

impl TestFormationFixture {
    pub async fn start(
        policy: NodeDeploymentPolicy,
        peers: Vec<(FormationPolicy, CandidateState)>,
        local_id: u64,
    ) -> Self {
        Self::with_password(policy, peers, local_id, None).await
    }

    pub async fn with_password(
        policy: NodeDeploymentPolicy,
        peers: Vec<(FormationPolicy, CandidateState)>,
        local_id: u64,
        password: Option<(&[u8], u32)>,
    ) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("crowdb-formation-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let inputs = NodeDeploymentOverrides {
            mode: Some(policy.mode),
            cluster: Some(policy.cluster.clone()),
            initial_voters: Some(policy.initial_voters),
            storage: Some(policy.setup.storage.clone()),
            services: Some(policy.setup.services.clone()),
            ..Default::default()
        };
        crowdb_monitor::accept_node_policy(&root, &NodeDeploymentOverrides::default(), &inputs).unwrap();
        if let Some((bytes, mode)) = password {
            std::fs::create_dir(root.join("ssh")).unwrap();
            let path = root.join("ssh/local-admission-password");
            std::fs::write(&path, bytes).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        std::fs::create_dir(root.join("console")).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let local = NodeHandshake {
            advertisement: NodeAdvertisement {
                discovery_id: format!("00000000-0000-4000-8000-{local_id:012x}"),
                protocol_version: NODE_PROTOCOL_VERSION,
                monitor_endpoints: vec![endpoint.clone()],
                cluster_id: None,
                formation: Some(policy.formation()),
            },
            physical_host_id: "one-host".into(),
            rack_hint: None,
            hardware: NodeHardware::default(),
        };
        let peers: Vec<_> = peers
            .into_iter()
            .enumerate()
            .map(|(index, (formation, state))| CandidateNode {
                advertisement: NodeAdvertisement {
                    discovery_id: format!("00000000-0000-4000-8000-{:012x}", index + 2),
                    formation: Some(formation),
                    cluster_id: if state == CandidateState::ForeignCluster {
                        Some(uuid::Uuid::new_v4().to_string())
                    } else {
                        None
                    },
                    ..local.advertisement.clone()
                },
                state,
            })
            .collect();
        let server = Router::new()
            .route("/node", get(move || async move { Json(local) }))
            .route(
                "/candidates",
                get(move || async move {
                    Json(CandidateSnapshot {
                        nodes: peers,
                        discovery_diagnostics: vec![],
                    })
                }),
            );
        let monitor = tokio::spawn(async move {
            axum::serve(listener, server).await.unwrap();
        });
        let state = AppState::with_runtime_root(
            crowdb_console_shared::ConsoleConfig::default(),
            root.join("console"),
        )
        .with_node_monitor(&endpoint)
        .unwrap();
        let runner = start_node_auto_join(state.clone());
        Self {
            root,
            state,
            monitor,
            runner,
        }
    }

    pub async fn observation(&self) -> serde_json::Value {
        let path = self.state.runtime_root.join("auto-join-status.json");
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if let Ok(bytes) = std::fs::read(&path) {
                    break serde_json::from_slice(&bytes).unwrap();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    pub fn assert_no_admission(&self) {
        assert!(!self.state.runtime_root.join("prepared-bootstrap.json").exists());
        assert!(!std::fs::read_dir(self.state.runtime_root.as_ref())
            .unwrap()
            .any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("admission-")
            }));
        assert!(self.state.config.read().unwrap().nodes.is_empty());
        assert!(self.state.config.read().unwrap().racks.is_empty());
        assert!(!self.root.join("disks").exists());
    }
}

impl Drop for TestFormationFixture {
    fn drop(&mut self) {
        self.runner.abort();
        self.monitor.abort();
        if !std::thread::panicking() {
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }
}
