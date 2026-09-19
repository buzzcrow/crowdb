// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persistent, local S3 mini-cluster lifecycle and thin HTTP operations.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use crowdb_protocol::port::alloc::{alloc_port, PortAllocConfig};
use crowdb_protocol::ServicePort;
use serde::Serialize;

use crate::config::{ConsoleConfig, LocalLaunchSpec, ServerEntry, ServiceType};
use crate::error::{Error, Result};
use crate::lifecycle;
use crate::ops::cluster::{
    self, KvDeployTunables, LocalChunkdbDeployConfig, LocalDiskdbDeployConfig,
};
use crate::ops::OpContext;

const CONFIG_FILE: &str = "console.toml";
const MARKER_FILE: &str = "s3-mini-cluster.json";
const MASTER_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct MiniClusterRecord {
    pub version: u32,
    pub endpoint: String,
    pub tenant: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MiniClusterStatus {
    pub created: bool,
    pub endpoint: String,
    pub data_dir: PathBuf,
    pub running_services: usize,
    pub total_services: usize,
}

#[must_use]
pub fn config_path(data_dir: &Path) -> PathBuf {
    data_dir.join(CONFIG_FILE)
}

/// Load a persisted mini-cluster record and console configuration.
///
/// # Errors
/// Returns an error for an unrecognized directory or invalid persisted data.
pub fn load(data_dir: &Path) -> Result<(ConsoleConfig, MiniClusterRecord)> {
    let marker = std::fs::read(data_dir.join(MARKER_FILE)).map_err(|error| Error::Validation {
        field: "data_dir".into(),
        message: format!("{} is not a CROWDB S3 mini-cluster: {error}", data_dir.display()),
    })?;
    let record = serde_json::from_slice(&marker).map_err(|error| Error::Config(error.to_string()))?;
    Ok((ConsoleConfig::load(&config_path(data_dir))?, record))
}

/// Create or restart a persistent local S3 mini-cluster.
///
/// # Errors
/// Returns an error for an unsafe directory, missing binary, failed service,
/// or failed readiness condition.
pub async fn start(data_dir: &Path) -> Result<MiniClusterStatus> {
    validate_location(data_dir)?;
    let marker_path = data_dir.join(MARKER_FILE);
    if marker_path.exists() {
        return restart(data_dir).await;
    }

    std::fs::create_dir_all(data_dir)?;
    let config = ConsoleConfig::default();
    let ctx = OpContext::new(
        "127.0.0.1:10000".into(),
        vec!["http://127.0.0.1:10000".into()],
        config,
    );
    let disk = LocalDiskdbDeployConfig {
        disk_groups_per_node: 1,
        disks_per_group: 1,
        capacity_bytes: 16 * 1024 * 1024 * 1024,
        zone_size_bytes: 16 * 1024 * 1024 * 1024,
        unit_size_bytes: 1024 * 1024,
        data_groups: vec![1],
        rpc_workers: None,
        kv_connections: None,
        kv_client_rpc_workers: None,
        free_batch_enabled: None,
        free_flush_max_batch: None,
    };
    let chunk = LocalChunkdbDeployConfig {
        instance_count: 3,
        // This loopback fixture colocates its simulated nodes in one rack.
        // Production planning still requires distinct failure domains.
        allow_unsafe_ec: true,
        rpc_workers: None,
        diskio_rpc_workers: None,
        kv_connections: None,
        kv_client_rpc_workers: None,
        diskdb_connections: None,
        diskdb_client_rpc_workers: None,
        metrics_interval: None,
    };
    cluster::local_deploy_combined_file_backed(
        &ctx,
        data_dir,
        Some(&KvDeployTunables::default()),
        &disk,
        &chunk,
    )
    .await?;
    let seeds = management_seeds(&ctx.config());
    ctx.config().save(&config_path(data_dir))?;
    let mut record = MiniClusterRecord {
        version: 1,
        endpoint: String::new(),
        tenant: "local".into(),
    };
    save_record(&marker_path, &record)?;
    let chunk_kv = spawn_chunk_kv(data_dir, &seeds).await?;
    add_service(&ctx, chunk_kv)?;
    let access = spawn_access(data_dir, &seeds).await?;
    let endpoint = access.entry.url.clone();
    add_service(&ctx, access)?;
    ctx.config().save(&config_path(data_dir))?;
    record.endpoint.clone_from(&endpoint);
    save_record(&marker_path, &record)?;
    let status = status_from(data_dir, true, &ctx.config(), endpoint);
    Ok(status)
}

async fn restart(data_dir: &Path) -> Result<MiniClusterStatus> {
    let (config, mut record) = load(data_dir)?;
    let seeds = management_seeds(&config);
    let group0 = config
        .servers
        .iter()
        .find(|server| server.service_type == ServiceType::Kv)
        .and_then(|server| server.rpc_url.as_deref())
        .unwrap_or("http://127.0.0.1:10000")
        .trim_start_matches("http://")
        .to_owned();
    let ctx = OpContext::new(group0, seeds.clone(), config);
    let node_ids = ctx.config().nodes.iter().map(|node| node.id).collect::<Vec<_>>();
    for node_id in node_ids {
        let server_dir = data_dir
            .join("rack1")
            .join(format!("node{node_id}"))
            .join(format!("kv-server-{node_id}"));
        crate::ops::kv_server::restart(&ctx, node_id, Some(&server_dir)).await?;
    }
    cluster::restart_storage_services(&ctx).await?;
    ctx.config().save(&config_path(data_dir))?;
    for kind in [ServiceType::ChunkKv, ServiceType::AccessServer] {
        let server = ctx
            .config()
            .servers
            .iter()
            .find(|server| server.service_type == kind)
            .cloned();
        let Some(server) = server else {
            let spawned = match kind {
                ServiceType::ChunkKv => spawn_chunk_kv(data_dir, &seeds).await?,
                ServiceType::AccessServer => spawn_access(data_dir, &seeds).await?,
                _ => unreachable!(),
            };
            if kind == ServiceType::AccessServer {
                record.endpoint.clone_from(&spawned.entry.url);
            }
            add_service(&ctx, spawned)?;
            ctx.config().save(&config_path(data_dir))?;
            continue;
        };
        let launch = ctx
            .config()
            .local_launches
            .get(&server.id)
            .cloned()
            .ok_or_else(|| Error::Config(format!("{} has no launch specification", server.id)))?;
        let pid = lifecycle::restart_local_service(&server.id, server.pid.unwrap_or(0), &launch).await?;
        if let Some(entry) = ctx.config_mut().servers.iter_mut().find(|entry| entry.id == server.id) {
            entry.pid = Some(pid);
        }
        ctx.config().save(&config_path(data_dir))?;
    }
    ctx.config().save(&config_path(data_dir))?;
    save_record(&data_dir.join(MARKER_FILE), &record)?;
    let status = status_from(data_dir, false, &ctx.config(), record.endpoint);
    Ok(status)
}

fn save_record(path: &Path, record: &MiniClusterRecord) -> Result<()> {
    std::fs::write(
        path,
        serde_json::to_vec_pretty(record).map_err(|error| Error::Config(error.to_string()))?,
    )?;
    Ok(())
}

/// Inspect persisted process liveness without mutating the cluster.
///
/// # Errors
/// Returns an error when the directory is not a valid mini-cluster.
pub fn status(data_dir: &Path) -> Result<MiniClusterStatus> {
    let (config, record) = load(data_dir)?;
    Ok(status_from(data_dir, false, &config, record.endpoint))
}

/// Stop all recorded processes while preserving data and configuration.
///
/// # Errors
/// Returns an error when persisted state cannot be loaded or saved.
pub fn stop(data_dir: &Path) -> Result<MiniClusterStatus> {
    let (mut config, record) = load(data_dir)?;
    for server in &mut config.servers {
        if let Some(pid) = server.pid.take() {
            let _ = lifecycle::stop_pid_with_timeout(pid, Duration::from_secs(5));
        }
    }
    config.save(&config_path(data_dir))?;
    Ok(status_from(data_dir, false, &config, record.endpoint))
}

fn validate_location(data_dir: &Path) -> Result<()> {
    if !data_dir.exists() {
        return Ok(());
    }
    let mut entries = std::fs::read_dir(data_dir)?;
    if entries.next().transpose()?.is_none() || data_dir.join(MARKER_FILE).exists() {
        return Ok(());
    }
    Err(Error::Validation {
        field: "data_dir".into(),
        message: format!(
            "{} is non-empty and has no {MARKER_FILE}; refusing to overwrite it",
            data_dir.display()
        ),
    })
}

fn management_seeds(config: &ConsoleConfig) -> Vec<String> {
    config
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Kv)
        .map(|server| server.url.clone())
        .collect()
}

struct SpawnedService {
    entry: ServerEntry,
    launch: LocalLaunchSpec,
}

fn add_service(ctx: &OpContext, service: SpawnedService) -> Result<()> {
    ctx.config_mut()
        .local_launches
        .insert(service.entry.id.clone(), service.launch);
    ctx.config_mut().add_server(service.entry)
}

async fn spawn_chunk_kv(data_dir: &Path, seeds: &[String]) -> Result<SpawnedService> {
    let binary = find_binary("CROWDB_CHUNK_KV_SERVER_BIN", "crowdb-chunk-kv-server")?;
    let ports = PortAllocConfig::new(data_dir);
    let rpc_port = alloc_port(ServicePort::ChunkKvRpc, 0, &ports).map_err(port_error)?;
    let http_port = alloc_port(ServicePort::ChunkKvHttp, 0, &ports).map_err(port_error)?;
    let workdir = data_dir.join("services/chunk-kv-1");
    let log_dir = workdir.join("log");
    std::fs::create_dir_all(&log_dir)?;
    let seed_toml = seeds.iter().map(|s| format!("{s:?}")).collect::<Vec<_>>().join(", ");
    let config_path = workdir.join("chunk-kv.toml");
    std::fs::write(
        &config_path,
        format!(
            "instance_id = 10000\nrpc_listen_addr = \"127.0.0.1:{rpc_port}\"\nrpc_advertise_addr = \"127.0.0.1:{rpc_port}\"\nhttp_listen_addr = \"127.0.0.1:{http_port}\"\ngroup0_mgmt_seeds = [{seed_toml}]\ncatalog_refresh_interval_ms = 200\n\n[balance]\ntarget_partitions_per_owner = 1\ntarget_partition_bytes = 9223372036854775807\nminimum_weighted_improvement_percent = 100\ncooldown_ms = 9223372036854775807\nmax_owner_request_rate = 0\n\n[storage]\nmetadata_store_id = 0\nstream_mirror_copies = 1\n\n[bootstrap_partition]\npartition_id = {{ high = 1, low = 1 }}\ntree_id = 1\nstream_name = {{ high = 2, low = 1 }}\nowner_epoch = 1\nmetadata_group_id = 1\n"
        ),
    )?;
    let launch = LocalLaunchSpec {
        program: binary.to_string_lossy().into_owned(),
        args: vec![
            "--config".into(),
            config_path.to_string_lossy().into_owned(),
            "--log-dir".into(),
            log_dir.to_string_lossy().into_owned(),
        ],
        workdir: workdir.to_string_lossy().into_owned(),
        env: BTreeMap::new(),
        readiness_url: Some(format!("http://127.0.0.1:{http_port}/ready")),
    };
    let pid = spawn(&launch, "chunk-kv-1").await?;
    Ok(SpawnedService {
        entry: server_entry("chunk-kv-1", ServiceType::ChunkKv, http_port, rpc_port, pid),
        launch,
    })
}

async fn spawn_access(data_dir: &Path, seeds: &[String]) -> Result<SpawnedService> {
    let binary = find_binary("CROWDB_ACCESS_SERVER_BIN", "crowdb-access-server")?;
    let port = alloc_port(ServicePort::Web, 1, &PortAllocConfig::new(data_dir)).map_err(port_error)?;
    let workdir = data_dir.join("services/access-server-1");
    std::fs::create_dir_all(workdir.join("log"))?;
    let mut env = BTreeMap::new();
    env.insert("CROWDB_S3_LISTEN".into(), format!("127.0.0.1:{port}"));
    env.insert("CROWDB_MANAGEMENT_SEEDS".into(), seeds.join(","));
    env.insert("CROWDB_S3_TENANT".into(), "local".into());
    env.insert("CROWDB_S3_MASTER_KEY".into(), MASTER_KEY.into());
    env.insert("CROWDB_S3_REGION".into(), "us-east-1".into());
    env.insert("CROWDB_S3_TRUSTED_NETWORK".into(), "true".into());
    env.insert("CROWDB_S3_EC_DATA".into(), "2".into());
    env.insert("CROWDB_S3_EC_CODE".into(), "1".into());
    let launch = LocalLaunchSpec {
        program: binary.to_string_lossy().into_owned(),
        args: Vec::new(),
        workdir: workdir.to_string_lossy().into_owned(),
        env,
        readiness_url: Some(format!("http://127.0.0.1:{port}/_crowdb/health/ready")),
    };
    let pid = spawn(&launch, "access-server-1").await?;
    Ok(SpawnedService {
        entry: server_entry("access-server-1", ServiceType::AccessServer, port, port, pid),
        launch,
    })
}

fn server_entry(id: &str, service_type: ServiceType, rest_port: u16, rpc_port: u16, pid: u32) -> ServerEntry {
    ServerEntry {
        id: id.into(),
        url: format!("http://127.0.0.1:{rest_port}"),
        node_id: None,
        rpc_url: Some(format!("http://127.0.0.1:{rpc_port}")),
        rest_port: Some(rest_port),
        rpc_port: Some(rpc_port),
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: Some(pid),
        service_type,
        rpc_workers: None,
        no_fsync: false,
    }
}

async fn spawn(spec: &LocalLaunchSpec, id: &str) -> Result<u32> {
    let log_path = Path::new(&spec.workdir).join("log").join(format!("{id}.stdout.log"));
    let log = std::fs::OpenOptions::new().create(true).append(true).open(&log_path)?;
    let mut child = lifecycle::detached_command(&spec.program)
        .args(&spec.args)
        .envs(&spec.env)
        .current_dir(&spec.workdir)
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(false)
        .spawn()?;
    let pid = child.id().ok_or_else(|| Error::Config(format!("{id} has no pid")))?;
    if let Some(url) = &spec.readiness_url {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .map_err(|error| http_error(&error))?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = child.try_wait()? {
                return Err(Error::UpstreamRpc { node_id: id.into(), status: format!("exited before ready: {status}; log={}", log_path.display()) });
            }
            if client.get(url).send().await.is_ok_and(|response| response.status().is_success()) {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill().await;
                return Err(Error::UpstreamRpc { node_id: id.into(), status: format!("readiness timeout; log={}", log_path.display()) });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    std::mem::forget(child);
    Ok(pid)
}

fn find_binary(env: &str, name: &str) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os(env).map(PathBuf::from).filter(|path| path.is_file()) {
        return Ok(path);
    }
    if let Ok(exe) = std::env::current_exe() {
        for root in exe.ancestors().take(5) {
            let candidate = root.join(name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    Err(Error::NotFound { kind: "binary".into(), id: format!("{name} (set {env})") })
}

fn status_from(data_dir: &Path, created: bool, config: &ConsoleConfig, endpoint: String) -> MiniClusterStatus {
    MiniClusterStatus {
        created,
        endpoint,
        data_dir: data_dir.to_path_buf(),
        running_services: config.servers.iter().filter(|server| server.pid.is_some_and(lifecycle::process_is_alive)).count(),
        total_services: config.servers.len(),
    }
}

fn port_error(error: impl std::fmt::Display) -> Error {
    Error::Validation { field: "port".into(), message: error.to_string() }
}

fn http_error(error: &reqwest::Error) -> Error {
    Error::UpstreamRpc { node_id: "s3".into(), status: error.to_string() }
}

/// Send one thin S3 HTTP operation to the endpoint persisted in `data_dir`.
///
/// # Errors
/// Returns an error for invalid cluster state, transport/protocol failure, or
/// any non-success S3 response.
pub async fn request(
    data_dir: &Path,
    method: reqwest::Method,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
    body: Option<Vec<u8>>,
) -> Result<(u16, Vec<u8>)> {
    let (_, record) = load(data_dir)?;
    let mut url = record.endpoint;
    if let Some(bucket) = bucket {
        url.push('/');
        url.push_str(&encode_path(bucket));
    }
    if let Some(object) = object {
        url.push('/');
        url.push_str(&object.split('/').map(encode_path).collect::<Vec<_>>().join("/"));
    }
    if !query.is_empty() {
        url.push('?');
        url.push_str(&form_urlencoded::Serializer::new(String::new()).extend_pairs(query.iter().map(|(k, v)| (*k, v.as_str()))).finish());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| http_error(&error))?;
    let mut request = client.request(method, url);
    if let Some(body) = body {
        request = request.body(body);
    }
    let response = request.send().await.map_err(|error| http_error(&error))?;
    let status = response.status().as_u16();
    let bytes = response.bytes().await.map_err(|error| http_error(&error))?.to_vec();
    if !(200..300).contains(&status) {
        return Err(Error::UpstreamRpc { node_id: "s3".into(), status: format!("HTTP {status}: {}", String::from_utf8_lossy(&bytes)) });
    }
    Ok((status, bytes))
}

fn encode_path(value: &str) -> String {
    value.bytes().map(|byte| {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            char::from(byte).to_string()
        } else {
            format!("%{byte:02X}")
        }
    }).collect()
}
