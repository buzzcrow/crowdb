// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::node_setup;
use crowdb_monitor::{DeploymentProfile, NodeIdentity, NodeServiceForTests};
use crowdb_protocol::mgmt::node::{
    setup::{NodeSetupChoices, NodeSetupRequest},
    NodeServiceAction, NodeServiceIntent,
};
use serde_json::json;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt};

pub struct TestReplacement {
    pub directory: crowdb_test_harness::test_dirs::TestDir,
    pub authority: node_setup::TestAuthority,
    pub worker: NodeServiceForTests,
    pub intent: NodeServiceIntent,
    pub original: u32,
    pub health: tokio::task::JoinHandle<()>,
    pub state: std::path::PathBuf,
}

impl TestReplacement {
    pub async fn new() -> Self {
        let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("service-replacement");
        let root = directory.path();
        let request = NodeSetupRequest {
            version: 1,
            discovery_id: NodeIdentity::load_or_create(root).unwrap().uuid().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            choices: NodeSetupChoices::default(),
        };
        let authority = node_setup::TestAuthority::start(root, &request).await;
        assert_eq!(authority.registry.nodes[0].node_id, authority.binding.node_id);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let health = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route("/health", axum::routing::get(|| async { "healthy" })),
            )
            .await
            .unwrap();
        });
        let mut profile = DeploymentProfile::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"),
        )
        .unwrap();
        profile.paths.data_root = root.to_path_buf();
        profile.paths.log_root = root.join("log");
        std::fs::create_dir_all(&profile.paths.log_root).unwrap();
        let program = root.join("owned-kv");
        std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        let service = profile
            .services
            .iter_mut()
            .find(|service| service.id == "kv")
            .unwrap();
        service.program = program;
        service.probe.target = format!("http://{address}/health");
        let intent = NodeServiceIntent {
            cluster_id: authority.binding.bootstrap.cluster_id.clone(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            node_id: authority.binding.node_id,
            service_id: "kv-2".into(),
            kind: "kv".into(),
            action: NodeServiceAction::Start,
            configuration: "node_id = 2\n".into(),
            environment: BTreeMap::new(),
        };
        authority
            .kv
            .put(
                0,
                0,
                b"/deployment/services/2/kv-2",
                &serde_json::to_vec(&intent).unwrap(),
                None,
            )
            .await
            .unwrap();
        let mut worker = NodeServiceForTests::new_for_tests(profile).await.unwrap();
        let original = worker.start_kv_child_for_tests().await.unwrap();
        let state = root.join("services/kv-2/execution.json");
        std::fs::create_dir_all(state.parent().unwrap()).unwrap();
        let record = json!({"intent":intent,"bootstrap":authority.binding.bootstrap,"complete":true,"pid":original,"attempts":0,"error":null});
        std::fs::write(&state, serde_json::to_vec(&record).unwrap()).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o600)).unwrap();
        Self {
            directory,
            authority,
            worker,
            intent,
            original,
            health,
            state,
        }
    }
}
