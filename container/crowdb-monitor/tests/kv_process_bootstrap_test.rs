// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use crowdb_monitor::{kv_step_names, BootstrapSession, DeploymentProfile, KvBootstrap, Supervisor};
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-real-kv-{}", Uuid::new_v4()));
        for path in ["bin", "data", "run"] {
            fs::create_dir_all(root.join(path)).unwrap();
        }
        Self(root.canonicalize().unwrap())
    }

    fn profile(&self, binary: &Path, management_port: u16, rpc_port: u16) -> DeploymentProfile {
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
        profile.services.retain(|service| service.id == "kv");
        let service = &mut profile.services[0];
        service.program = self.0.join("bin/crowdb-kv-server");
        symlink(binary, &service.program).unwrap();
        service.config_template = None;
        service.args = vec![
            "--root".into(),
            self.0.join("data/kv/node-1").to_string_lossy().into_owned(),
            "--management-addr".into(),
            "127.0.0.1".into(),
            "--management-port".into(),
            management_port.to_string(),
            "--ports".into(),
            rpc_port.to_string(),
        ];
        service.probe.target = format!("http://127.0.0.1:{management_port}/health");
        profile.validate().unwrap();
        profile
    }

    fn session(&self, profile: &DeploymentProfile) -> BootstrapSession {
        let names = kv_step_names(profile).unwrap();
        let steps = names.iter().map(String::as_str).collect::<Vec<_>>();
        BootstrapSession::open(&self.0.join("data"), b"profile", b"config", &steps).unwrap()
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
async fn monitor_creates_real_kv_groups_then_validates_after_restart() {
    let Some(binary) = crowdb_test_harness::cluster::crowdb_kv_server_bin() else {
        eprintln!("skipping real KV bootstrap: crowdb-kv-server binary is unavailable");
        return;
    };
    let root = TestRoot::new();
    let management = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management_port = management.local_addr().unwrap().port();
    drop(management);
    let rpc = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_port = rpc.local_addr().unwrap().port();
    drop(rpc);
    let profile = root.profile(&binary, management_port, rpc_port);
    let mut session = root.session(&profile);
    fs::create_dir_all(root.0.join("data/kv/node-1")).unwrap();
    fs::create_dir_all(root.0.join("data/log")).unwrap();
    let endpoint = format!("http://127.0.0.1:{management_port}");
    let bootstrap = KvBootstrap::new(&endpoint).unwrap();
    let mut supervisor = Supervisor::new(
        profile.clone(),
        session.manifest().deployment_id(),
        &root.0.join("data/log"),
        &root.0.join("run"),
    )
    .await
    .unwrap();
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    bootstrap
        .reconcile(&mut session, &profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    session.mark_ready().unwrap();
    supervisor.mark_ready().await.unwrap();
    supervisor.shutdown().await.unwrap();
    drop(supervisor);
    let mut restart = root.session(&profile);
    let mut supervisor = Supervisor::new(
        profile.clone(),
        restart.manifest().deployment_id(),
        &root.0.join("data/log"),
        &root.0.join("run"),
    )
    .await
    .unwrap();
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    bootstrap
        .reconcile(&mut restart, &profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    supervisor.mark_ready().await.unwrap();
    supervisor.shutdown().await.unwrap();
}
