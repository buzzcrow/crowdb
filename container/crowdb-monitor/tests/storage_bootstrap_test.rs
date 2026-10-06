// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient};
use crowdb_monitor::{
    disk_step_names, ensure_disk_files, hardware_step_names, iceberg_step_names, kv_step_names,
    logical_step_names, render_configs, verify_chunk_services, verify_diskio_disks, BootstrapSession,
    DeploymentProfile, HardwareBootstrap, IcebergBootstrap, KvBootstrap, LogicalBootstrap, ServerCredentials,
    Supervisor,
};
use crowdb_protocol::{port::alloc as port_alloc, ServicePort};
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let root = crowdb_test_harness::test_dirs::ephemeral_root()
            .join(format!("monitor-storage-{}", Uuid::new_v4()));
        for path in ["bin", "templates", "data", "run"] {
            fs::create_dir_all(root.join(path)).unwrap();
        }
        Self(root.canonicalize().unwrap())
    }

    fn profile(&self, ports: &Ports, binaries: &[(&str, &Path)]) -> DeploymentProfile {
        let mut profile = DeploymentProfile::load(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml"),
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
        for group in &mut profile.groups {
            group.rpc_endpoint = format!("127.0.0.1:{}", ports.kv_rpc);
        }
        profile
            .services
            .retain(|service| binaries.iter().any(|(id, _)| *id == service.id));
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
                    "--binding-monitor-interval".into(),
                    "1".into(),
                ],
                "diskdb" | "diskio" | "chunkdb" | "chunk-kv" => vec![
                    "--config".into(),
                    self.0
                        .join(format!("run/config/{}.toml", service.id))
                        .to_string_lossy()
                        .into_owned(),
                    "--log-dir".into(),
                    self.0
                        .join(format!("data/log/{}", service.id))
                        .to_string_lossy()
                        .into_owned(),
                ],
                "access" => vec![
                    "--config".into(),
                    format!("{}/run/config/access.toml", self.0.display()),
                ],
                _ => unreachable!(),
            };
            if service.id == "access" {
                self.configure_access_env(service, ports);
            }
            service.fence_listeners = match service.id.as_str() {
                "kv" => vec![ports.kv_management, ports.kv_rpc],
                "diskdb" => vec![ports.diskdb_listen, ports.diskdb_http, ports.diskdb_rpc],
                "diskio" => vec![ports.diskio_rpc],
                "chunkdb" => vec![ports.chunkdb_http, ports.chunkdb_rpc],
                "chunk-kv" => vec![ports.chunk_kv_http, ports.chunk_kv_rpc],
                "access" => vec![ports.iceberg, ports.s3, ports.health],
                _ => unreachable!(),
            }
            .into_iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect();
            service.probe.target = match service.id.as_str() {
                "kv" => format!("http://127.0.0.1:{}/health", ports.kv_management),
                "diskdb" => format!("http://127.0.0.1:{}/ready", ports.diskdb_http),
                "diskio" => format!("127.0.0.1:{}", ports.diskio_rpc),
                "chunkdb" => format!("http://127.0.0.1:{}/ready", ports.chunkdb_http),
                "chunk-kv" => format!("http://127.0.0.1:{}/ready", ports.chunk_kv_http),
                "access" => format!("http://127.0.0.1:{}/v1/config", ports.iceberg),
                _ => unreachable!(),
            };
            if service.id == "access" {
                service.additional_probes[0].target =
                    format!("http://127.0.0.1:{}/_crowdb/health/ready", ports.health);
            }
        }
        profile.validate().unwrap();
        profile
    }

    fn configure_access_env(&self, service: &mut crowdb_monitor::ServiceProfile, ports: &Ports) {
        service.env.insert(
            "CROWDB_ACCESS_HEALTH_LISTEN".into(),
            format!("127.0.0.1:{}", ports.health),
        );
        service.env.insert(
            "CROWDB_MANAGEMENT_SEEDS".into(),
            format!("http://127.0.0.1:{}", ports.kv_management),
        );
        service.env.insert(
            "CROWDB_ICEBERG_LISTEN".into(),
            format!("127.0.0.1:{}", ports.iceberg),
        );
        service.env.insert(
            "CROWDB_ICEBERG_PUBLIC_URI".into(),
            format!("http://127.0.0.1:{}", ports.iceberg),
        );
        service.env.insert(
            "CROWDB_ACCESS_LOG_DIR".into(),
            self.0.join("data/log/access").to_string_lossy().into_owned(),
        );
    }

    fn templates(&self, ports: &Ports) {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/templates");
        for name in [
            "kv.toml",
            "diskdb.toml",
            "diskio.toml",
            "chunkdb.toml",
            "chunk-kv.toml",
            "access.toml",
        ] {
            let body = fs::read_to_string(source.join(name)).unwrap();
            let body = body
                .replace("127.0.0.1:10000", &format!("127.0.0.1:{}", ports.kv_management))
                .replace("127.0.0.1:11000", &format!("127.0.0.1:{}", ports.diskdb_listen))
                .replace("127.0.0.1:11100", &format!("127.0.0.1:{}", ports.diskdb_http))
                .replace("127.0.0.1:11200", &format!("127.0.0.1:{}", ports.diskdb_rpc))
                .replace(
                    "listen_port = 13000",
                    &format!("listen_port = {}", ports.diskio_rpc),
                )
                .replace("127.0.0.1:12100", &format!("127.0.0.1:{}", ports.chunkdb_http))
                .replace("127.0.0.1:12200", &format!("127.0.0.1:{}", ports.chunkdb_rpc))
                .replace("127.0.0.1:15100", &format!("127.0.0.1:{}", ports.chunk_kv_http))
                .replace("127.0.0.1:15200", &format!("127.0.0.1:{}", ports.chunk_kv_rpc))
                .replace("0.0.0.0:9092", &format!("127.0.0.1:{}", ports.iceberg))
                .replace("0.0.0.0:9091", &format!("127.0.0.1:{}", ports.s3));
            fs::write(self.0.join("templates").join(name), body).unwrap();
        }
    }

    fn session(&self, profile: &DeploymentProfile) -> BootstrapSession {
        let names = kv_step_names(profile)
            .unwrap()
            .into_iter()
            .chain(disk_step_names(profile))
            .chain(hardware_step_names())
            .chain(logical_step_names().map(str::to_owned))
            .chain(
                profile
                    .services
                    .iter()
                    .any(|service| service.id == "access")
                    .then(iceberg_step_names)
                    .into_iter()
                    .flatten()
                    .map(str::to_owned),
            )
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
    chunkdb_http: u16,
    chunkdb_rpc: u16,
    chunk_kv_http: u16,
    chunk_kv_rpc: u16,
    iceberg: u16,
    s3: u16,
    health: u16,
}

impl Ports {
    fn allocate() -> Self {
        // Listener ranges stay outside the OS ephemeral client-port range.
        // Retain process-owned claims until both bootstrap/restart phases finish.
        Self {
            kv_management: port_alloc::alloc_test_port(ServicePort::KvServerMgmt),
            kv_rpc: port_alloc::alloc_test_port(ServicePort::KvServerListen),
            diskdb_listen: port_alloc::alloc_test_port(ServicePort::DiskdbListen),
            diskdb_http: port_alloc::alloc_test_port(ServicePort::DiskdbHttp),
            diskdb_rpc: port_alloc::alloc_test_port(ServicePort::DiskdbRpc),
            diskio_rpc: port_alloc::alloc_test_port(ServicePort::DiskioRpc),
            chunkdb_http: port_alloc::alloc_test_port(ServicePort::ChunkdbHttp),
            chunkdb_rpc: port_alloc::alloc_test_port(ServicePort::ChunkdbRpc),
            chunk_kv_http: port_alloc::alloc_test_port(ServicePort::ChunkKvHttp),
            chunk_kv_rpc: port_alloc::alloc_test_port(ServicePort::ChunkKvRpc),
            iceberg: port_alloc::alloc_test_port(ServicePort::AccessServerIcebergHttp),
            s3: port_alloc::alloc_test_port(ServicePort::AccessServerHttp),
            health: port_alloc::alloc_test_port(ServicePort::AccessServerHealthHttp),
        }
    }
}

#[tokio::test]
async fn preview_chunk_services_start_and_recover() {
    let Some(kv_binary) = crowdb_test_harness::cluster::crowdb_kv_server_bin() else {
        eprintln!("skipping storage process test: KV binary unavailable");
        return;
    };
    let diskdb_binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/crowdb-diskdb");
    let diskio_binary =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../app/crowdb-diskio/build/crowdb-diskio");
    let chunkdb_binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/crowdb-chunkdb");
    let chunk_kv_binary =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/crowdb-chunk-kv-server");
    if !diskdb_binary.exists()
        || !diskio_binary.exists()
        || !chunkdb_binary.exists()
        || !chunk_kv_binary.exists()
    {
        eprintln!("skipping storage process test: storage or chunk binary unavailable");
        return;
    }
    let root = TestRoot::new();
    let ports = Ports::allocate();
    root.templates(&ports);
    let profile = root.profile(
        &ports,
        &[
            ("kv", kv_binary.as_path()),
            ("diskdb", diskdb_binary.as_path()),
            ("diskio", diskio_binary.as_path()),
            ("chunkdb", chunkdb_binary.as_path()),
            ("chunk-kv", chunk_kv_binary.as_path()),
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
    start_preview_storage(&mut supervisor, &mut session, &profile, &management_seed).await;
    let chunkdb_config = root.0.join("run/config/chunkdb.toml");
    let original = fs::read_to_string(&chunkdb_config).unwrap();
    fs::write(
        &chunkdb_config,
        original.replace("instance_id = \"1\"", "instance_id = \"2\""),
    )
    .unwrap();
    assert!(verify_chunk_services(&management_seed, &profile)
        .await
        .unwrap_err()
        .to_string()
        .contains("ChunkDB registration conflicts"));
    fs::write(&chunkdb_config, original).unwrap();
    let chunk_kv_config = root.0.join("run/config/chunk-kv.toml");
    let original = fs::read_to_string(&chunk_kv_config).unwrap();
    fs::write(
        &chunk_kv_config,
        original.replace("instance_id = 1", "instance_id = 2"),
    )
    .unwrap();
    assert!(verify_chunk_services(&management_seed, &profile)
        .await
        .unwrap_err()
        .to_string()
        .contains("Chunk-KV registration conflicts"));
    fs::write(&chunk_kv_config, original).unwrap();
    verify_chunk_services(&management_seed, &profile).await.unwrap();
    assert_logical_topology_and_conflict(&management_seed, &profile, &mut session, &mut supervisor).await;
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
    start_preview_storage(&mut restarted, &mut restarted_session, &profile, &management_seed).await;
    restarted.mark_ready().await.unwrap();
    restarted.shutdown().await.unwrap();
}

async fn assert_logical_topology_and_conflict(
    management_seed: &str,
    profile: &DeploymentProfile,
    session: &mut BootstrapSession,
    supervisor: &mut Supervisor,
) {
    let sysmd = CrowdbSysmdClient::new(CrowdbKvClient::new(ClientConfig::new(vec![
        management_seed.to_owned()
    ])));
    sysmd.kv().refresh_topology().await.unwrap();
    assert_eq!(sysmd.list_stores().await.unwrap().len(), 1);
    assert_eq!(sysmd.list_groups_in_store(0).await.unwrap().len(), 2);
    assert_eq!(sysmd.list_replicas_in_group(0, 0).await.unwrap().len(), 1);
    assert_eq!(sysmd.list_replicas_in_group(0, 1).await.unwrap().len(), 1);
    sysmd.add_group(0, 99).await.unwrap();
    assert!(LogicalBootstrap::new(management_seed.to_owned())
        .reconcile(session, profile, supervisor.monitor_log_mut())
        .await
        .is_err());
    assert_eq!(sysmd.list_groups_in_store(0).await.unwrap().len(), 3);
    sysmd.remove_group(0, 99).await.unwrap();
}

async fn start_preview_storage(
    supervisor: &mut Supervisor,
    session: &mut BootstrapSession,
    profile: &DeploymentProfile,
    management_seed: &str,
) {
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    KvBootstrap::new(management_seed)
        .unwrap()
        .reconcile(session, profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    ensure_disk_files(session, profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    HardwareBootstrap::new(management_seed.to_owned())
        .reconcile(session, profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    LogicalBootstrap::new(management_seed.to_owned())
        .reconcile(session, profile, supervisor.monitor_log_mut())
        .await
        .unwrap();
    supervisor.start_service("diskdb", BTreeMap::new()).await.unwrap();
    supervisor.start_service("diskio", BTreeMap::new()).await.unwrap();
    verify_diskio_disks(management_seed, profile).await.unwrap();
    supervisor
        .start_service("chunkdb", BTreeMap::new())
        .await
        .unwrap();
    supervisor
        .start_service("chunk-kv", BTreeMap::new())
        .await
        .unwrap();
    verify_chunk_services(management_seed, profile).await.unwrap();
}

#[tokio::test]
async fn preview_real_iceberg_catalog_and_listener_survive_restart() {
    let Some(kv_binary) = crowdb_test_harness::cluster::crowdb_kv_server_bin() else {
        eprintln!("skipping real Iceberg bootstrap: KV binary unavailable");
        return;
    };
    let binary_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug");
    let diskio_binary =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../app/crowdb-diskio/build/crowdb-diskio");
    let binaries = [
        ("diskdb", binary_root.join("crowdb-diskdb")),
        ("diskio", diskio_binary),
        ("chunkdb", binary_root.join("crowdb-chunkdb")),
        ("chunk-kv", binary_root.join("crowdb-chunk-kv-server")),
        ("access", binary_root.join("crowdb-access-server")),
    ];
    if binaries.iter().any(|(_, binary)| !binary.exists()) {
        eprintln!("skipping real Iceberg bootstrap: storage or Iceberg binary unavailable");
        return;
    }
    let root = TestRoot::new();
    let ports = Ports::allocate();
    root.templates(&ports);
    let mut links = vec![("kv", kv_binary.as_path())];
    links.extend(binaries.iter().map(|(id, path)| (*id, path.as_path())));
    let profile = root.profile(&ports, &links);
    let mut session = root.session(&profile);
    let credentials = ServerCredentials::load_or_create(&root.0.join("data")).unwrap();
    fs::create_dir_all(root.0.join("data/kv/node-1")).unwrap();
    fs::create_dir_all(root.0.join("data/log")).unwrap();
    render_configs(&profile, &root.0.join("templates"), &root.0.join("run")).unwrap();
    let seed = format!("http://127.0.0.1:{}", ports.kv_management);
    let mut supervisor = Supervisor::new(
        profile.clone(),
        session.manifest().deployment_id(),
        &root.0.join("data/log"),
        &root.0.join("run"),
    )
    .await
    .unwrap();
    start_preview_storage(&mut supervisor, &mut session, &profile, &seed).await;
    IcebergBootstrap::reconcile(&mut session, &profile, &credentials, supervisor.monitor_log_mut())
        .await
        .unwrap();
    let mut environment = iceberg_environment(&credentials);
    environment.insert("CROWDB_S3_MASTER_KEY".into(), credentials.s3_master_key().into());
    supervisor
        .start_service("access", environment.clone())
        .await
        .unwrap();
    session.mark_ready().unwrap();
    supervisor.mark_ready().await.unwrap();
    supervisor.shutdown().await.unwrap();
    drop(supervisor);

    let mut restarted_session = root.session(&profile);
    let mut restarted = Supervisor::new(
        profile.clone(),
        restarted_session.manifest().deployment_id(),
        &root.0.join("data/log"),
        &root.0.join("run"),
    )
    .await
    .unwrap();
    start_preview_storage(&mut restarted, &mut restarted_session, &profile, &seed).await;
    IcebergBootstrap::reconcile(
        &mut restarted_session,
        &profile,
        &credentials,
        restarted.monitor_log_mut(),
    )
    .await
    .unwrap();
    restarted.start_service("access", environment).await.unwrap();
    restarted.mark_ready().await.unwrap();
    restarted.shutdown().await.unwrap();
}

fn iceberg_environment(credentials: &ServerCredentials) -> BTreeMap<String, String> {
    credentials
        .server_env()
        .lines()
        .filter_map(|line| line.split_once('='))
        .filter(|(key, _)| key.starts_with("CROWDB_ICEBERG_"))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}
