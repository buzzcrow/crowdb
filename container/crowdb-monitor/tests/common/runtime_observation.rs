// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{
    DeploymentProfile, MonitorPhase, MonitorStatus, NodeIdentity, ProcessManager, ServiceStatus, StatusStore,
};
use crowdb_protocol::mgmt::node::{
    deployment::NodeDeploymentOverrides, NodeServiceAction, NodeServiceIntent,
};
use std::os::unix::fs::PermissionsExt;
use std::{collections::BTreeMap, path::Path};

pub struct TestObservation {
    pub profile: DeploymentProfile,
    pub manager: ProcessManager,
    pub store: StatusStore,
    pub status: MonitorStatus,
    pub intent: NodeServiceIntent,
}

pub fn profile(root: &Path) -> DeploymentProfile {
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.run_root = root.join("run");
    profile.paths.log_root = root.join("log");
    profile
}

impl TestObservation {
    pub async fn start(root: &Path) -> Self {
        let inputs = NodeDeploymentOverrides::default();
        crowdb_monitor::accept_node_policy(root, &inputs, &inputs).unwrap();
        let identity = NodeIdentity::load_or_create(root).unwrap();
        let profile = profile(root);
        std::fs::create_dir(&profile.paths.run_root).unwrap();
        std::fs::create_dir(&profile.paths.log_root).unwrap();
        let mut manager = ProcessManager::new(profile.paths.log_root.clone(), profile.logs.clone())
            .await
            .unwrap();
        let mut service = profile
            .services
            .iter()
            .find(|service| service.id == "chunkdb")
            .unwrap()
            .clone();
        service.id = "chunkdb-1".into();
        service.program = "/bin/sh".into();
        service.args = vec!["-c".into(), "exec sleep 60".into()];
        let pid = manager.start(&service, &BTreeMap::new()).await.unwrap();
        let intent = NodeServiceIntent {
            cluster_id: uuid::Uuid::new_v4().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            node_id: 1,
            service_id: service.id.clone(),
            kind: "chunkdb".into(),
            action: NodeServiceAction::Start,
            configuration: "[server]\n".into(),
            environment: BTreeMap::new(),
        };
        let mut status = MonitorStatus::new(identity.uuid(), MonitorPhase::Ready);
        status.services.insert(
            service.id,
            ServiceStatus {
                operation_id: Some(intent.operation_id.clone()),
                pid: Some(pid),
                generation: 1,
                healthy: true,
                restart_attempts: 0,
            },
        );
        let store = StatusStore::new(&profile.paths.run_root).unwrap();
        store.publish(&mut status).unwrap();
        let result = Self {
            profile,
            manager,
            store,
            status,
            intent,
        };
        result.save(true, 0, None);
        result
    }

    pub fn save(&self, complete: bool, attempts: u32, error: Option<&str>) {
        let directory = self
            .profile
            .paths
            .data_root
            .join("services")
            .join(&self.intent.service_id);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("execution.json"),
            serde_json::to_vec(&serde_json::json!({
                "intent":self.intent, "complete":complete, "attempts":attempts,
                "pid":self.status.services[&self.intent.service_id].pid, "error":error,
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(
            directory.join("execution.json"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
}
