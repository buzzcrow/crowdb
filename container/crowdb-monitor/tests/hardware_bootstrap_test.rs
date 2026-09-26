// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::path::{Path, PathBuf};

use crowdb_monitor::{
    hardware_step_names, BootstrapSession, DeploymentProfile, HardwareBootstrap, MonitorLog,
};
use crowdb_protocol::common::{HwStatus, RackValue};
use crowdb_test_harness::cluster::KvCluster;
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-hardware-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join("data")).unwrap();
        Self(root.canonicalize().unwrap())
    }

    fn profile(&self) -> DeploymentProfile {
        let mut profile = DeploymentProfile::load(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-preview/profile.toml"),
        )
        .unwrap();
        profile.paths.install_root.clone_from(&self.0);
        profile.paths.bin_root = self.0.join("bin");
        profile.paths.template_root = self.0.join("templates");
        profile.paths.data_root = self.0.join("data");
        profile.paths.run_root = self.0.join("run");
        profile.paths.log_root = self.0.join("data/log");
        for disk in &mut profile.disks {
            disk.path = self.0.join("data/disks").join(disk.path.file_name().unwrap());
        }
        for service in &mut profile.services {
            service.program = self.0.join("bin").join(service.program.file_name().unwrap());
            service.config_template = service
                .config_template
                .as_ref()
                .map(|path| self.0.join("templates").join(path.file_name().unwrap()));
        }
        profile.validate().unwrap();
        profile
    }

    fn session(&self) -> BootstrapSession {
        let names = hardware_step_names();
        let steps = names.iter().map(String::as_str).collect::<Vec<_>>();
        BootstrapSession::open(&self.0.join("data"), b"profile", b"config", &steps).unwrap()
    }

    async fn events(&self, profile: &DeploymentProfile) -> MonitorLog {
        let root = self.0.join("data/log");
        fs::create_dir_all(&root).unwrap();
        MonitorLog::open(&root, profile.logs.clone()).await.unwrap()
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[tokio::test]
async fn writes_and_validates_hardware_with_real_group_zero() {
    if crowdb_test_harness::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping real KV bootstrap: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    let root = TestRoot::new();
    let profile = root.profile();
    let mut session = root.session();
    let mut events = root.events(&profile).await;
    let bootstrap = HardwareBootstrap::new(cluster.mgmt_endpoints[0].clone());
    bootstrap
        .reconcile(&mut session, &profile, &mut events)
        .await
        .unwrap();
    assert_eq!(session.manifest().next_step(), None);
    session.mark_ready().unwrap();
    drop(session);
    let mut restart = root.session();
    bootstrap
        .reconcile(&mut restart, &profile, &mut events)
        .await
        .unwrap();
    let hardware = cluster.make_hardware_client();
    let disks = hardware.list_all_disks().await.unwrap();
    assert_eq!(disks.len(), 4);
    assert!(disks.iter().all(|disk| disk.value.zone_count == 1));
    assert!(disks
        .iter()
        .all(|disk| disk.capacity_bytes() == 16 * 1024 * 1024 * 1024));
    let body = fs::read_to_string(root.0.join("data/log/monitor/monitor.log")).unwrap();
    assert_eq!(body.matches("bootstrap_step_completed").count(), 1);
}

#[tokio::test]
async fn conflicting_group_zero_record_rejects_without_creating_hardware() {
    if crowdb_test_harness::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping real KV bootstrap: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    let hardware = cluster.make_hardware_client();
    hardware
        .add_rack(
            1,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: vec![99],
            },
        )
        .await
        .unwrap();
    let root = TestRoot::new();
    let profile = root.profile();
    let mut session = root.session();
    let mut events = root.events(&profile).await;
    let bootstrap = HardwareBootstrap::new(cluster.mgmt_endpoints[0].clone());
    assert!(bootstrap
        .reconcile(&mut session, &profile, &mut events)
        .await
        .is_err());
    assert!(hardware.list_nodes().await.unwrap().is_empty());
    assert!(hardware.list_all_disks().await.unwrap().is_empty());
    assert_eq!(session.manifest().next_step(), Some("hardware-topology"));
}
