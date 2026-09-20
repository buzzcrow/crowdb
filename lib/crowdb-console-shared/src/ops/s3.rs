// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persistent, local S3 mini-cluster lifecycle and thin HTTP operations.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use crowdb_protocol::port::namespace::{assign_process_ports, RuntimeNamespace};
use crowdb_protocol::ServicePort;
use serde::Serialize;

use crate::config::{ConsoleConfig, LocalLaunchSpec, ServerEntry, ServiceType};
use crate::error::{Error, Result};
use crate::lifecycle;
use crate::ops::cluster::{self, KvDeployTunables, LocalChunkdbDeployConfig, LocalDiskdbDeployConfig};
use crate::ops::OpContext;

const CONFIG_FILE: &str = "console.toml";
const MARKER_FILE: &str = "s3-mini-cluster.json";
const INITIALIZING_FILE: &str = "s3-mini-cluster.initializing.json";
const MASTER_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const NAMESPACE_ID: &str = "s3-mini-cluster";
const BODY_PREVIEW_LIMIT: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageProfile {
    #[default]
    Persistent,
    Memory,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct MiniClusterRecord {
    pub version: u32,
    pub endpoint: String,
    pub tenant: String,
    #[serde(default)]
    pub storage_profile: StorageProfile,
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
        field: "root".into(),
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
    start_with_profile(data_dir, StorageProfile::Persistent, 16 * 1024 * 1024 * 1024).await
}

/// Create a fresh memory-backed cluster for an S3 benchmark.
///
/// # Errors
/// Returns an error unless the location is missing or empty, or when a service
/// cannot start. Memory clusters are intentionally not restartable.
pub async fn start_memory(data_dir: &Path, memory_budget_bytes: u64) -> Result<MiniClusterStatus> {
    if memory_budget_bytes < 64 * 1024 * 1024 {
        return Err(Error::Validation {
            field: "memory_budget_bytes".into(),
            message: "must be at least 64 MiB".into(),
        });
    }
    start_with_profile(data_dir, StorageProfile::Memory, memory_budget_bytes).await
}

async fn start_with_profile(
    data_dir: &Path,
    storage_profile: StorageProfile,
    capacity_bytes: u64,
) -> Result<MiniClusterStatus> {
    archive_incomplete_attempt(data_dir)?;
    validate_location(data_dir)?;
    let marker_path = data_dir.join(MARKER_FILE);
    if marker_path.exists() {
        let (_, record) = load(data_dir)?;
        if record.storage_profile != storage_profile {
            return Err(Error::Validation {
                field: "storage_profile".into(),
                message: format!(
                    "existing cluster uses {:?}, requested {:?}",
                    record.storage_profile, storage_profile
                ),
            });
        }
        if storage_profile == StorageProfile::Memory {
            return Err(Error::Validation {
                field: "root".into(),
                message: "memory benchmark clusters cannot be restarted".into(),
            });
        }
        return restart(data_dir).await;
    }

    std::fs::create_dir_all(data_dir)?;
    if storage_profile == StorageProfile::Persistent {
        RuntimeNamespace::persistent(data_dir, NAMESPACE_ID).map_err(namespace_error)?;
    }
    let config = ConsoleConfig::default();
    let ctx = OpContext::new(
        "127.0.0.1:10000".into(),
        vec!["http://127.0.0.1:10000".into()],
        config,
    );
    let logical_capacity = if storage_profile == StorageProfile::Memory {
        16 * 1024 * 1024 * 1024
    } else {
        capacity_bytes
    };
    let per_node_capacity = logical_capacity
        .checked_div(3)
        .unwrap_or(capacity_bytes)
        .max(64 * 1024 * 1024)
        / (1024 * 1024)
        * (1024 * 1024);
    let disk = LocalDiskdbDeployConfig {
        disk_groups_per_node: 1,
        disks_per_group: 1,
        capacity_bytes: per_node_capacity,
        zone_size_bytes: per_node_capacity,
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
    let mut record = MiniClusterRecord {
        version: 1,
        endpoint: String::new(),
        tenant: "local".into(),
        storage_profile,
    };
    save_record(&data_dir.join(INITIALIZING_FILE), &record)?;
    let initialized = initialize_new(&ctx, data_dir, &disk, &chunk, storage_profile).await;
    let endpoint = match initialized {
        Ok(endpoint) => endpoint,
        Err(error) => {
            stop_config_processes(&mut ctx.config_mut());
            let _ = ctx.config().save(&config_path(data_dir));
            return Err(error);
        }
    };
    record.endpoint.clone_from(&endpoint);
    save_record(&marker_path, &record)?;
    let _ = std::fs::remove_file(data_dir.join(INITIALIZING_FILE));
    let status = status_from(data_dir, true, &ctx.config(), endpoint);
    Ok(status)
}

fn archive_incomplete_attempt(data_dir: &Path) -> Result<()> {
    if !data_dir.join(INITIALIZING_FILE).exists() || data_dir.join(MARKER_FILE).exists() {
        return Ok(());
    }
    let name = data_dir
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| Error::Validation {
            field: "root".into(),
            message: "incomplete cluster directory has no archiveable name".into(),
        })?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    let archived = data_dir.with_file_name(format!("{name}.failed-{timestamp}"));
    if data_dir.join("namespace.json").is_file() {
        RuntimeNamespace::persistent(data_dir, NAMESPACE_ID)
            .map_err(namespace_error)?
            .release()
            .map_err(namespace_error)?;
    }
    std::fs::rename(data_dir, &archived)?;
    Ok(())
}

async fn initialize_new(
    ctx: &OpContext,
    data_dir: &Path,
    disk: &LocalDiskdbDeployConfig,
    chunk: &LocalChunkdbDeployConfig,
    storage_profile: StorageProfile,
) -> Result<String> {
    let tunables = KvDeployTunables {
        kv_backend: (storage_profile == StorageProfile::Memory).then(|| "mem-block".into()),
        wal_backend: (storage_profile == StorageProfile::Memory).then(|| "mem-block".into()),
        no_fsync: (storage_profile == StorageProfile::Memory).then_some(true),
        ..KvDeployTunables::default()
    };
    match storage_profile {
        StorageProfile::Persistent => {
            cluster::local_deploy_combined_file_backed(ctx, data_dir, Some(&tunables), disk, chunk).await?;
        }
        StorageProfile::Memory => {
            cluster::local_deploy_combined(ctx, data_dir, Some(&tunables), disk, chunk, "mem").await?;
        }
    }
    let seeds = management_seeds(&ctx.config());
    ctx.config().save(&config_path(data_dir))?;
    let chunk_kv = spawn_chunk_kv(data_dir, &seeds).await?;
    add_service(ctx, chunk_kv)?;
    ctx.config().save(&config_path(data_dir))?;
    let access = spawn_access(data_dir, &seeds).await?;
    let endpoint = access.entry.url.clone();
    add_service(ctx, access)?;
    ctx.config().save(&config_path(data_dir))?;
    Ok(endpoint)
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
        let mut launch = ctx
            .config()
            .local_launches
            .get(&server.id)
            .cloned()
            .ok_or_else(|| Error::Config(format!("{} has no launch specification", server.id)))?;
        if kind == ServiceType::AccessServer {
            launch
                .env
                .insert("CROWDB_S3_MASTER_KEY".into(), MASTER_KEY.into());
        }
        let pid = lifecycle::restart_local_service(&server.id, server.pid.unwrap_or(0), &launch).await?;
        if let Some(entry) = ctx
            .config_mut()
            .servers
            .iter_mut()
            .find(|entry| entry.id == server.id)
        {
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
    stop_config_processes(&mut config);
    config.save(&config_path(data_dir))?;
    Ok(status_from(data_dir, false, &config, record.endpoint))
}

/// Stop and permanently delete a persistent local S3 mini-cluster.
///
/// # Errors
/// Returns an error when the location is not a recognized cluster, process
/// state cannot be persisted, claims cannot be released, or files cannot be
/// removed.
pub fn delete(data_dir: &Path) -> Result<MiniClusterStatus> {
    let status = stop(data_dir)?;
    RuntimeNamespace::persistent(data_dir, NAMESPACE_ID)
        .map_err(namespace_error)?
        .delete()
        .map_err(namespace_error)?;
    Ok(status)
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
        field: "root".into(),
        message: format!(
            "{} is non-empty and has no {MARKER_FILE}; refusing to overwrite it",
            data_dir.display()
        ),
    })
}

fn stop_config_processes(config: &mut ConsoleConfig) {
    for server in &mut config.servers {
        if let Some(pid) = server.pid.take() {
            let _ = lifecycle::stop_pid_with_timeout(pid, Duration::from_secs(5));
        }
    }
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
    let rpc_port = assign_cluster_port(data_dir, ServicePort::ChunkKvRpc, "chunk-kv-1-rpc")?;
    let http_port = assign_cluster_port(data_dir, ServicePort::ChunkKvHttp, "chunk-kv-1-http")?;
    let workdir = data_dir.join("services/chunk-kv-1");
    let log_dir = workdir.join("log");
    std::fs::create_dir_all(&log_dir)?;
    let seed_toml = seeds
        .iter()
        .map(|s| format!("{s:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    let config_path = workdir.join("chunk-kv.toml");
    std::fs::write(
        &config_path,
        format!(
            "instance_id = 10000\nrpc_listen_addr = \"127.0.0.1:{rpc_port}\"\nrpc_advertise_addr = \"127.0.0.1:{rpc_port}\"\nhttp_listen_addr = \"127.0.0.1:{http_port}\"\ngroup0_mgmt_seeds = [{seed_toml}]\ncatalog_refresh_interval_ms = 200\n\n[balance]\nenabled = true\ntarget_partitions_per_owner = 1\ntarget_partition_bytes = 9223372036854775807\nminimum_weighted_improvement_percent = 100\ncooldown_ms = 9223372036854775807\nmax_owner_request_rate = 0\n\n[storage]\nmetadata_store_id = 0\nstream_mirror_copies = 1\n\n[bootstrap_partition]\npartition_id = {{ high = 1, low = 1 }}\ntree_id = 1\nstream_name = {{ high = 2, low = 1 }}\nowner_epoch = 1\nmetadata_group_id = 1\n"
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
    let port = assign_cluster_port(data_dir, ServicePort::AccessServerHttp, "access-server-1")?;
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
    let runtime_launch = LocalLaunchSpec {
        program: binary.to_string_lossy().into_owned(),
        args: Vec::new(),
        workdir: workdir.to_string_lossy().into_owned(),
        env,
        readiness_url: Some(format!("http://127.0.0.1:{port}/_crowdb/health/ready")),
    };
    let pid = spawn(&runtime_launch, "access-server-1").await?;
    let mut persisted_launch = runtime_launch;
    persisted_launch.env.remove("CROWDB_S3_MASTER_KEY");
    Ok(SpawnedService {
        entry: server_entry("access-server-1", ServiceType::AccessServer, port, port, pid),
        launch: persisted_launch,
    })
}

fn assign_cluster_port(data_dir: &Path, service: ServicePort, identity: &str) -> Result<u16> {
    if data_dir.join("namespace.json").is_file() {
        return RuntimeNamespace::persistent(data_dir, NAMESPACE_ID)
            .map_err(namespace_error)?
            .assign_named_port(service, identity)
            .map_err(namespace_error);
    }
    assign_process_ports(service, 0, 1)
        .map_err(namespace_error)?
        .into_iter()
        .next()
        .ok_or_else(|| Error::Validation {
            field: "port_alloc".into(),
            message: "port allocator returned no assignment".into(),
        })
}

fn namespace_error(error: impl std::fmt::Display) -> Error {
    Error::Validation {
        field: "runtime_namespace".into(),
        message: error.to_string(),
    }
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
    let log_path = Path::new(&spec.workdir)
        .join("log")
        .join(format!("{id}.stdout.log"));
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let mut child = lifecycle::detached_command(&spec.program)
        .args(&spec.args)
        .envs(&spec.env)
        .current_dir(&spec.workdir)
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(false)
        .spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| Error::Config(format!("{id} has no pid")))?;
    if let Some(url) = &spec.readiness_url {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .map_err(|error| http_error(&error))?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = child.try_wait()? {
                return Err(Error::UpstreamRpc {
                    node_id: id.into(),
                    status: format!("exited before ready: {status}; log={}", log_path.display()),
                });
            }
            if client
                .get(url)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill().await;
                return Err(Error::UpstreamRpc {
                    node_id: id.into(),
                    status: format!("readiness timeout; log={}", log_path.display()),
                });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    std::mem::forget(child);
    Ok(pid)
}

fn find_binary(env: &str, name: &str) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os(env)
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
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
    Err(Error::NotFound {
        kind: "binary".into(),
        id: format!("{name} (set {env})"),
    })
}

fn status_from(
    data_dir: &Path,
    created: bool,
    config: &ConsoleConfig,
    endpoint: String,
) -> MiniClusterStatus {
    MiniClusterStatus {
        created,
        endpoint,
        data_dir: data_dir.to_path_buf(),
        running_services: config
            .servers
            .iter()
            .filter(|server| server.pid.is_some_and(lifecycle::process_is_alive))
            .count(),
        total_services: config.servers.len(),
    }
}

fn http_error(error: &reqwest::Error) -> Error {
    Error::UpstreamRpc {
        node_id: "s3".into(),
        status: error.to_string(),
    }
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
    request_with_range(data_dir, method, bucket, object, query, body, None).await
}

/// Send one thin S3 HTTP operation with an optional inclusive byte range.
///
/// # Errors
/// Returns an error for a malformed range or any cluster, transport, protocol,
/// or S3 response failure.
pub async fn request_with_range(
    data_dir: &Path,
    method: reqwest::Method,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
    body: Option<Vec<u8>>,
    range: Option<(u64, u64)>,
) -> Result<(u16, Vec<u8>)> {
    let (_, record) = load(data_dir)?;
    S3HttpClient::new(record.endpoint)?
        .request(method, bucket, object, query, body, range)
        .await
}

/// Send one S3 operation and retain its HTTP request and response metadata.
///
/// Unlike [`request_with_range`], an HTTP error status is returned as an
/// exchange so an interactive caller can show the response headers and body.
/// Transport and local configuration failures still return [`Error`].
///
/// # Errors
/// Returns an error for malformed input, invalid cluster state, or transport
/// failure.
pub async fn request_exchange_with_range(
    data_dir: &Path,
    method: reqwest::Method,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
    body: Option<Vec<u8>>,
    range: Option<(u64, u64)>,
) -> Result<S3HttpExchange> {
    request_exchange_with_headers(
        data_dir,
        method,
        bucket,
        object,
        query,
        body,
        range,
        reqwest::header::HeaderMap::new(),
    )
    .await
}

/// Send one S3 operation with caller-supplied request headers and retain the
/// complete HTTP exchange metadata.
///
/// # Errors
/// Returns an error for malformed input, invalid cluster state, or transport
/// failure.
#[allow(clippy::too_many_arguments)]
pub async fn request_exchange_with_headers(
    data_dir: &Path,
    method: reqwest::Method,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
    body: Option<Vec<u8>>,
    range: Option<(u64, u64)>,
    headers: reqwest::header::HeaderMap,
) -> Result<S3HttpExchange> {
    let (_, record) = load(data_dir)?;
    S3HttpClient::new(record.endpoint)?
        .request_exchange_with_headers(method, bucket, object, query, body, range, headers)
        .await
}

/// Captured metadata for an outbound S3 HTTP request.
#[derive(Debug)]
pub struct S3HttpRequest {
    pub method: reqwest::Method,
    pub url: reqwest::Url,
    pub headers: reqwest::header::HeaderMap,
    /// `None` means no body was supplied; `Some(0)` is an explicit empty body.
    pub body_bytes: Option<usize>,
    /// Bounded copy of a textual request body for interactive display.
    pub body_preview: Option<Vec<u8>>,
    pub body_preview_truncated: bool,
}

/// One completed S3 HTTP request/response exchange.
#[derive(Debug)]
pub struct S3HttpExchange {
    pub request: S3HttpRequest,
    pub status: reqwest::StatusCode,
    pub response_headers: reqwest::header::HeaderMap,
    pub response_body: Vec<u8>,
}

impl S3HttpExchange {
    /// Convert the captured exchange to the compact result used by non-CLI
    /// callers, preserving the existing non-success error behavior.
    ///
    /// # Errors
    /// Returns an upstream error for a non-2xx HTTP response.
    pub fn into_result(self) -> Result<(u16, Vec<u8>)> {
        if !self.status.is_success() {
            return Err(Error::UpstreamRpc {
                node_id: "s3".into(),
                status: format!(
                    "HTTP {}: {}",
                    self.status.as_u16(),
                    String::from_utf8_lossy(&self.response_body)
                ),
            });
        }
        Ok((self.status.as_u16(), self.response_body))
    }
}

/// Reusable thin client for one local S3 endpoint.
#[derive(Clone)]
pub struct S3HttpClient {
    endpoint: String,
    client: reqwest::Client,
}

impl S3HttpClient {
    /// Build a client with the standard per-request timeout.
    ///
    /// # Errors
    /// Returns an error when the HTTP client cannot be constructed.
    pub fn new(endpoint: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| http_error(&error))?;
        Ok(Self { endpoint, client })
    }

    /// Load the endpoint persisted for a mini-cluster.
    ///
    /// # Errors
    /// Returns an error when the cluster record or HTTP client is invalid.
    pub fn from_data_dir(data_dir: &Path) -> Result<Self> {
        let (_, record) = load(data_dir)?;
        Self::new(record.endpoint)
    }

    /// Send one S3 request, optionally with an inclusive byte range.
    ///
    /// # Errors
    /// Returns an error for an invalid range or transport/protocol/S3 failure.
    pub async fn request(
        &self,
        method: reqwest::Method,
        bucket: Option<&str>,
        object: Option<&str>,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        range: Option<(u64, u64)>,
    ) -> Result<(u16, Vec<u8>)> {
        self.request_exchange(method, bucket, object, query, body, range)
            .await?
            .into_result()
    }

    /// Send one S3 request and retain HTTP metadata even for non-2xx status.
    ///
    /// # Errors
    /// Returns an error for an invalid range, request construction failure, or
    /// transport/protocol failure.
    pub async fn request_exchange(
        &self,
        method: reqwest::Method,
        bucket: Option<&str>,
        object: Option<&str>,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        range: Option<(u64, u64)>,
    ) -> Result<S3HttpExchange> {
        self.request_exchange_with_headers(
            method,
            bucket,
            object,
            query,
            body,
            range,
            reqwest::header::HeaderMap::new(),
        )
        .await
    }

    /// Send one S3 request with caller-supplied headers and retain HTTP
    /// metadata even for a non-2xx status.
    ///
    /// # Errors
    /// Returns an error for an invalid range, request construction failure, or
    /// transport/protocol failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn request_exchange_with_headers(
        &self,
        method: reqwest::Method,
        bucket: Option<&str>,
        object: Option<&str>,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        range: Option<(u64, u64)>,
        headers: reqwest::header::HeaderMap,
    ) -> Result<S3HttpExchange> {
        let mut url = self.endpoint.clone();
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
            url.push_str(
                &form_urlencoded::Serializer::new(String::new())
                    .extend_pairs(query.iter().map(|(k, v)| (*k, v.as_str())))
                    .finish(),
            );
        }
        let mut request = self.client.request(method, url).headers(headers);
        if let Some((start, end)) = range {
            if start > end {
                return Err(Error::Validation {
                    field: "range".into(),
                    message: "range start must not exceed end".into(),
                });
            }
            request = request.header(reqwest::header::RANGE, format!("bytes={start}-{end}"));
        }
        let body_bytes = body.as_ref().map(Vec::len);
        let textual_body = request
            .try_clone()
            .and_then(|builder| builder.build().ok())
            .and_then(|request| request.headers().get(reqwest::header::CONTENT_TYPE).cloned())
            .and_then(|value| value.to_str().ok().map(str::to_ascii_lowercase))
            .is_some_and(|value| {
                value.starts_with("text/") || value.contains("json") || value.contains("xml")
            });
        let body_preview = body
            .as_ref()
            .filter(|_| textual_body)
            .map(|body| body.iter().copied().take(BODY_PREVIEW_LIMIT).collect::<Vec<_>>());
        let body_preview_truncated =
            body_bytes.is_some_and(|bytes| textual_body && bytes > BODY_PREVIEW_LIMIT);
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_LENGTH, body.len())
                .body(body);
        }
        let mut request = request.build().map_err(|error| http_error(&error))?;
        if !request.headers().contains_key(reqwest::header::HOST) {
            let host = request.url().host_str().ok_or_else(|| Error::Validation {
                field: "endpoint".into(),
                message: "S3 endpoint has no host".into(),
            })?;
            let host = if host.contains(':') {
                format!("[{host}]")
            } else {
                host.to_string()
            };
            let authority = request
                .url()
                .port()
                .map_or(host.clone(), |port| format!("{host}:{port}"));
            let value =
                reqwest::header::HeaderValue::from_str(&authority).map_err(|error| Error::Validation {
                    field: "endpoint".into(),
                    message: format!("invalid S3 endpoint authority: {error}"),
                })?;
            request.headers_mut().insert(reqwest::header::HOST, value);
        }
        let request_summary = S3HttpRequest {
            method: request.method().clone(),
            url: request.url().clone(),
            headers: request.headers().clone(),
            body_bytes,
            body_preview,
            body_preview_truncated,
        };
        let response = self
            .client
            .execute(request)
            .await
            .map_err(|error| http_error(&error))?;
        let status = response.status();
        let response_headers = response.headers().clone();
        let response_body = response
            .bytes()
            .await
            .map_err(|error| http_error(&error))?
            .to_vec();
        Ok(S3HttpExchange {
            request: request_summary,
            status,
            response_headers,
            response_body,
        })
    }
}

fn encode_path(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}
