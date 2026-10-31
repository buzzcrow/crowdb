// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::node_setup;
use crowdb_monitor::{DeploymentProfile, NodeIdentity, NodeServiceForTests};
use crowdb_protocol::mgmt::node::{
    setup::{NodeSetupChoices, NodeSetupRequest},
    NodeServiceAction, NodeServiceIntent,
};
use crowdb_test_harness::test_dirs::TestDir;
use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

pub struct TestStoppedKv {
    pub directory: TestDir,
    pub worker: NodeServiceForTests,
    pub intent: NodeServiceIntent,
}

impl TestStoppedKv {
    pub async fn new() -> Self {
        let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("completed-kv-stop");
        let root = directory.path();
        let request = NodeSetupRequest {
            version: 1,
            discovery_id: NodeIdentity::load_or_create(root).unwrap().uuid().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            choices: NodeSetupChoices::default(),
        };
        let authority = node_setup::TestAuthority::start(root, &request).await;
        assert!(authority.registry.nodes[0].confirmed);
        std::fs::write(
            root.join("accepted-node.json"),
            serde_json::to_vec(&serde_json::json!({
                "node_id":authority.binding.node_id,"bootstrap":authority.binding.bootstrap,"prepared":true,
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(
            root.join("accepted-node.json"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let intent = NodeServiceIntent {
            cluster_id: authority.binding.bootstrap.cluster_id.clone(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            node_id: authority.binding.node_id,
            service_id: "kv-2".into(),
            kind: "kv".into(),
            action: NodeServiceAction::Stop,
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
        let mut worker = NodeServiceForTests::new_for_tests(profile(root)).await.unwrap();
        let pid = worker.start_kv_child_for_tests().await.unwrap();
        let first = worker.execute_for_tests(intent.clone()).await.unwrap();
        assert!(first["pid"].is_null());
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "the acknowledged stop must reap its owned child"
        );
        drop(authority);
        Self {
            directory,
            worker,
            intent,
        }
    }

    pub fn state(&self) -> PathBuf {
        self.directory.path().join("services/kv-2/execution.json")
    }
}

fn profile(root: &Path) -> DeploymentProfile {
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.log_root = root.join("log");
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    let program = root.join("owned-kv");
    std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    profile
        .services
        .iter_mut()
        .find(|s| s.id == "kv")
        .unwrap()
        .program = program;
    profile
}
