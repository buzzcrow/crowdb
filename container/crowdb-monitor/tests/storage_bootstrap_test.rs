// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use crowdb_monitor::{
    disk_step_names, ensure_disk_files, hardware_step_names, kv_step_names, render_configs,
    verify_diskio_disks, BootstrapSession, DeploymentProfile, HardwareBootstrap, KvBootstrap, Supervisor,
};
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-storage-{}", Uuid::new_v4()));
        for path in ["bin", "templates", "data", "run"] {
            fs::create_dir_all(root.join(path)).unwrap();
        }
        Self(root.canonicalize().unwrap())
    }

    fn profile(&self, ports: &Ports, binaries: &[(&str, &Path)]) -> DeploymentProfile {
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
        profile
            .services
            .retain(|service| ["kv", "diskdb", "diskio"].contains(&service.id.as_str()));
        for service in &mut profile.services {
            let name = service.program.file_name().unwrap();
            let binary = binaries.iter().find(|(id, _)| *id == service.id).unwrap().1;
            service.program = self.0.join("bin").join(name);
            symlink(binary, &service.program).unwrap();
            service.config_template = service
                .config_template
                .as_ref()
                .map(|path| self.0.join("templates").join(path.file_name().unwrap()));
            service.args = match service.id.as_str() {
                "kv" => vec![
                    "--root".into(),
                    self.0.join("data/kv/node-1").to_string_lossy().into_owned(),
                    "--config".into(),
                    self.0.join("run/config/kv.toml").to_string_lossy().into_owned(),
                    "--management-addr".into(),
                    "127.0.0.1".into(),
                    "--management-port".into(),
                    ports.kv_management.to_string(),
                    "--ports".into(),
                    ports.kv_rpc.to_string(),
                ],
                "diskdb" | "diskio" => vec![
                    "--config".into(),
                    self.0
                        .join(format!("run/config/{}.toml", service.id))
                        .to_string_lossy()
                        .into_owned(),
                ],
                _ => unreachable!(),
            };
            service.fence_listeners = match service.id.as_str() {
                "kv" => vec![ports.kv_management, ports.kv_rpc],
                "diskdb" => vec![ports.diskdb_listen, ports.diskdb_http, ports.diskdb_rpc],
                "diskio" => vec![ports.diskio_rpc],
                _ => unreachable!(),
            }
            .into_iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect();
            service.probe.target = match service.id.as_str() {
                "kv" => format!("http://127.0.0.1:{}/health", ports.kv_management),
                "diskdb" => format!("http://127.0.0.1:{}/ready", ports.diskdb_http),
                "diskio" => format!("127.0.0.1:{}", ports.diskio_rpc),
                _ => unreachable!(),
            };
        }
        profile.validate().unwrap();
        profile
    }

    fn templates(&self, ports: &Ports) {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-preview/templates");
        for name in ["kv.toml", "diskdb.toml", "diskio.toml"] {
            let body = fs::read_to_string(source.join(name)).unwrap();
            let body = body
                .replace("127.0.0.1:10000", &format!("127.0.0.1:{}", ports.kv_management))
                .replace("127.0.0.1:11000", &format!("127.0.0.1:{}", ports.diskdb_listen))
                .replace("127.0.0.1:11100", &format!("127.0.0.1:{}", ports.diskdb_http))
                .replace("127.0.0.1:11200", &format!("127.0.0.1:{}", ports.diskdb_rpc))
                .replace(
                    "listen_port = 13000",
                    &format!("listen_port = {}", ports.diskio_rpc),
                );
            fs::write(self.0.join("templates").join(name), body).unwrap();
        }
    }

    fn session(&self, profile: &DeploymentProfile) -> BootstrapSession {
        let names = kv_step_names(profile)
            .unwrap()
            .into_iter()
            .chain(disk_step_names(profile))
            .chain(hardware_step_names())
            .collect::<Vec<_>>();
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

struct Ports {
    kv_management: u16,
    kv_rpc: u16,
    diskdb_listen: u16,
    diskdb_http: u16,
    diskdb_rpc: u16,
    diskio_rpc: u16,
}

impl Ports {
    async fn allocate() -> Self {
        let mut listeners = Vec::new();
        for _ in 0..6 {
            listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let ports = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect::<Vec<_>>();
        Self {
            kv_management: ports[0],
            kv_rpc: ports[1],
            diskdb_listen: ports[2],
            diskdb_http: ports[3],
            diskdb_rpc: ports[4],
            diskio_rpc: ports[5],
        }
    }
}

#[tokio::test]
async fn four_file_disks_are_ready_through_real_diskdb_and_diskio() {
    let Some(kv_binary) = crowdb_test_harness::cluster::crowdb_kv_server_bin() else {
        eprintln!("skipping storage process test: KV binary unavailable");
        return;
    };
    let diskdb_binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/crowdb-diskdb");
    let diskio_binary =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../app/crowdb-diskio/build/crowdb-diskio");
    if !diskdb_binary.exists() || !diskio_binary.exists() {
        eprintln!("skipping storage process test: DiskDB or DiskIO binary unavailable");
        return;
    }
    let root = TestRoot::new();
    let ports = Ports::allocate().await;
    root.templates(&ports);
    let profile = root.profile(
        &ports,
        &[
            ("kv", kv_binary.as_path()),
            ("diskdb", diskdb_binary.as_path()),
            ("diskio", diskio_binary.as_path()),
        ],
    );
    let mut session = root.session(&profile);
    fs::create_dir_all(root.0.join("data/kv/node-1")).unwrap();
    fs::create_dir_all(root.0.join("data/log")).unwrap();
    render_configs(&profile, &root.0.join("templates"), &root.0.join("run")).unwrap();
    let management_seed = format!("http://127.0.0.1:{}", ports.kv_management);
    let mut supervisor = Supervisor::new(
        profile.clone(),
        session.manifest().deployment_id(),
        &root.0.join("data/log"),
        &root.0.join("run"),
    )
    .await
    .unwrap();
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    KvBootstrap::new(&management_seed)
        .unwrap()
        .reconcile(&mut session, &profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    ensure_disk_files(&mut session, &profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    HardwareBootstrap::new(management_seed.clone())
        .reconcile(&mut session, &profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    supervisor.start_service("diskdb", BTreeMap::new()).await.unwrap();
    supervisor.start_service("diskio", BTreeMap::new()).await.unwrap();
    verify_diskio_disks(&management_seed, &profile).await.unwrap();
    session.mark_ready().unwrap();
    supervisor.mark_ready().await.unwrap();
    supervisor.shutdown().await.unwrap();

    let mut restarted_session = root.session(&profile);
    let mut restarted = Supervisor::new(
        profile.clone(),
        restarted_session.manifest().deployment_id(),
        &root.0.join("data/log"),
        &root.0.join("run"),
    )
    .await
    .unwrap();
    restarted.start_service("kv", BTreeMap::new()).await.unwrap();
    KvBootstrap::new(&management_seed)
        .unwrap()
        .reconcile(&mut restarted_session, &profile, restarted.monitor_log_mut())
        .await
        .unwrap();
    ensure_disk_files(&mut restarted_session, &profile, restarted.monitor_log_mut())
        .await
        .unwrap();
    HardwareBootstrap::new(management_seed.clone())
        .reconcile(&mut restarted_session, &profile, restarted.monitor_log_mut())
        .await
        .unwrap();
    restarted.start_service("diskdb", BTreeMap::new()).await.unwrap();
    restarted.start_service("diskio", BTreeMap::new()).await.unwrap();
    verify_diskio_disks(&management_seed, &profile).await.unwrap();
    restarted.mark_ready().await.unwrap();
    restarted.shutdown().await.unwrap();
}
