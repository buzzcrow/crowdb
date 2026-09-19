// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunkdb test harness: subprocess management, binary discovery, and
//! client construction for chunkdb E2E tests.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport, RetryConfig};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::ServicePort;

use crate::hardware::INSTANCE_ID;

// Re-export hardware helpers for convenience.
pub use crate::cluster::crowdb_kv_server_bin;
pub use crate::hardware::{seed_hardware, standard_disk_ids_4};

/// Find the crowdb-chunkdb binary.
pub fn crowdb_chunkdb_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CROWDB_CHUNKDB_BIN") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crowdb-chunkdb");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    None
}

// ── chunkdb subprocess ───────────────────────────────────────────

pub struct ChunkdbProcess {
    pub child: std::process::Child,
    pub listen_port: i32,
    pub http_port: i32,
    pub config_path: std::path::PathBuf,
    pub log_path: std::path::PathBuf,
    runtime: Option<crate::test_dirs::TestRuntime>,
}

struct ChunkdbRuntime {
    listen_port: i32,
    rpc_port: i32,
    http_port: i32,
    config_path: std::path::PathBuf,
    log_path: std::path::PathBuf,
}

fn prepare_runtime(runtime: &mut crate::test_dirs::TestRuntime) -> ChunkdbRuntime {
    let assign = |runtime: &mut crate::test_dirs::TestRuntime, service, label| {
        i32::from(
            runtime
                .assign_named_port(service, "instance-1")
                .unwrap_or_else(|error| panic!("assign ChunkDB {label} port: {error}")),
        )
    };
    let listen_port = assign(runtime, ServicePort::ChunkdbListen, "listen");
    let rpc_port = assign(runtime, ServicePort::ChunkdbRpc, "RPC");
    let http_port = assign(runtime, ServicePort::ChunkdbHttp, "HTTP");
    let root = runtime
        .service_dir("chunkdb", "instance-1")
        .unwrap_or_else(|error| panic!("create ChunkDB service root: {error}"));
    ChunkdbRuntime {
        listen_port,
        rpc_port,
        http_port,
        config_path: root.join("config").join("chunkdb.toml"),
        log_path: root.join("log").join("chunkdb.log"),
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub enum ChunkdbPlacementMode {
    #[default]
    Protected,
    UnsafeColocated,
}

impl ChunkdbPlacementMode {
    fn as_config(self) -> &'static str {
        match self {
            Self::Protected => "protected",
            Self::UnsafeColocated => "unsafe_colocated",
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct ChunkdbStartOptions {
    pub placement_mode: ChunkdbPlacementMode,
    pub allow_unsafe_ec: bool,
    pub allow_degraded_failure_domains: bool,
    pub conversion_enabled: bool,
    pub conversion_min_seal_age_secs: u64,
    pub conversion_scan_interval_secs: u64,
    pub conversion_max_bandwidth_mbps: u64,
    pub conversion_task_lease_secs: u64,
    pub repair_enabled: bool,
    pub repair_allow_unsafe_placement: bool,
    pub repair_scan_interval_secs: u64,
    pub repair_max_concurrency: usize,
    pub repair_memory_bytes: usize,
}

impl Default for ChunkdbStartOptions {
    fn default() -> Self {
        Self {
            placement_mode: ChunkdbPlacementMode::Protected,
            allow_unsafe_ec: false,
            allow_degraded_failure_domains: false,
            conversion_enabled: false,
            conversion_min_seal_age_secs: 3_600,
            conversion_scan_interval_secs: 30,
            conversion_max_bandwidth_mbps: 50,
            conversion_task_lease_secs: 30,
            repair_enabled: true,
            repair_allow_unsafe_placement: false,
            repair_scan_interval_secs: 1,
            repair_max_concurrency: 4,
            repair_memory_bytes: 64 * 1024 * 1024,
        }
    }
}

impl ChunkdbProcess {
    pub fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Start crowdb-chunkdb with a generated config pointing at the
    /// kv-server management seeds.
    pub fn start(kv_seeds: &[String]) -> Self {
        Self::start_with_options(kv_seeds, ChunkdbStartOptions::default())
    }

    /// Start crowdb-chunkdb and optionally permit single-rack EC placement.
    pub fn start_with_unsafe_ec(kv_seeds: &[String], allow_unsafe_ec: bool) -> Self {
        Self::start_with_options(
            kv_seeds,
            ChunkdbStartOptions {
                allow_unsafe_ec,
                allow_degraded_failure_domains: allow_unsafe_ec,
                ..ChunkdbStartOptions::default()
            },
        )
    }

    pub fn start_with_options(kv_seeds: &[String], options: ChunkdbStartOptions) -> Self {
        let mut runtime = crate::test_dirs::TestRuntime::new("chunkdb")
            .unwrap_or_else(|error| panic!("create ChunkDB runtime namespace: {error}"));
        let mut process = Self::start_with_options_in(&mut runtime, kv_seeds, options);
        process.runtime = Some(runtime);
        process
    }

    /// Start ChunkDB inside a shared runtime namespace.
    pub fn start_with_options_in(
        runtime: &mut crate::test_dirs::TestRuntime,
        kv_seeds: &[String],
        options: ChunkdbStartOptions,
    ) -> Self {
        let bin = crowdb_chunkdb_bin().unwrap_or_else(|| {
            panic!("crowdb-chunkdb binary not found; set CROWDB_CHUNKDB_BIN or build app/crowdb-chunkdb")
        });

        // Allocate listen/rpc/http ports from the fixed chunkdb ranges
        // (12000-12999) via the port allocator. These ranges sit below
        // the Linux ephemeral range (32768-60999), so the kernel never
        // reassigns them between the probe and the subprocess bind — the
        // TOCTOU that plagues `bind(:0)`-style ephemeral port selection
        // under load. The shared per-process claim file keeps the three
        // ports pairwise distinct. ChunkdbListen and ChunkdbRpc bases
        // differ by 200, so rpc_port = listen_port + 200, the offset
        // the client derives (without it the subprocess falls back to
        // the hardcoded default 0.0.0.0:9961 and collides across tests).
        let paths = prepare_runtime(runtime);
        let listen_port = paths.listen_port;
        let rpc_port = paths.rpc_port;
        debug_assert_eq!(
            rpc_port - listen_port,
            i32::from(crowdb_protocol::CHUNKDB_RPC_BASE) - i32::from(crowdb_protocol::CHUNKDB_LISTEN_BASE),
            "allocator must preserve the listen->rpc offset"
        );
        let http_port = paths.http_port;

        let config_content = format!(
            r#"[server]
rpc_workers = 2
listen_addr = "127.0.0.1:{listen_port}"
rpc_listen_addr = "127.0.0.1:{rpc_port}"
http_listen_addr = "127.0.0.1:{http_port}"
instance_id = "{INSTANCE_ID}"
kv_server_mgmt_seeds = [{seeds}]
keepalive_interval_secs = 2

[topology]
refresh_interval_secs = 2

[range_guard]
allow_all_when_empty = true

[placement]
mode = "{placement_mode}"
allow_unsafe_ec = {allow_unsafe_ec}
allow_degraded_failure_domains = {allow_degraded_failure_domains}

[conversion]
enabled = {conversion_enabled}
data_num = 8
code_num = 4
min_seal_age_secs = {conversion_min_seal_age_secs}
min_mirror_strips = 8
max_concurrency = 4
max_bandwidth_mbps = {conversion_max_bandwidth_mbps}
scan_interval_secs = {conversion_scan_interval_secs}
task_lease_secs = {conversion_task_lease_secs}

[repair]
enabled = {repair_enabled}
allow_unsafe_placement = {repair_allow_unsafe_placement}
scan_interval_secs = {repair_scan_interval_secs}
max_concurrency = {repair_max_concurrency}
memory_bytes = {repair_memory_bytes}

[lifecycle]
cache_capacity = 1000
sweep_chunk_lock_interval_secs = 10
lock_hold_warn_threshold_ms = 1000
"#,
            placement_mode = options.placement_mode.as_config(),
            allow_unsafe_ec = options.allow_unsafe_ec,
            allow_degraded_failure_domains = options.allow_degraded_failure_domains,
            conversion_enabled = options.conversion_enabled,
            conversion_min_seal_age_secs = options.conversion_min_seal_age_secs,
            conversion_scan_interval_secs = options.conversion_scan_interval_secs,
            conversion_max_bandwidth_mbps = options.conversion_max_bandwidth_mbps,
            conversion_task_lease_secs = options.conversion_task_lease_secs,
            repair_enabled = options.repair_enabled,
            repair_allow_unsafe_placement = options.repair_allow_unsafe_placement,
            repair_scan_interval_secs = options.repair_scan_interval_secs,
            repair_max_concurrency = options.repair_max_concurrency,
            repair_memory_bytes = options.repair_memory_bytes,
            seeds = kv_seeds
                .iter()
                .map(|s| format!("\"{s}\""))
                .collect::<Vec<_>>()
                .join(", "),
        );

        let config_path = paths.config_path;
        std::fs::write(&config_path, &config_content).expect("write config");

        let log_path = paths.log_path;
        let log_file = std::fs::File::create(&log_path).expect("create log file");
        let log_file2 = log_file.try_clone().expect("clone log file");

        let mut cmd = Command::new(&bin);
        let test_log_dir = log_path.parent().expect("ChunkDB log directory");
        cmd.args(["--config", config_path.to_str().unwrap()])
            .arg("--log-dir")
            .arg(test_log_dir.to_str().unwrap())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file2));

        let child = cmd.spawn().expect("start crowdb-chunkdb");
        runtime
            .record_process(child.id())
            .unwrap_or_else(|error| panic!("record ChunkDB process: {error}"));
        eprintln!("crowdb-chunkdb log: {}", log_path.display());

        Self {
            child,
            listen_port,
            http_port,
            config_path,
            log_path,
            runtime: None,
        }
    }

    /// Wait for the chunkdb HTTP `/ready` endpoint to return 200.
    pub async fn wait_for_ready(&self) {
        let url = format!("http://127.0.0.1:{}/ready", self.http_port);
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    eprintln!("crowdb-chunkdb ready (phase=up)");
                    return;
                }
            }
            if Instant::now() > deadline {
                let log = self.log_content();
                panic!("crowdb-chunkdb did not become ready within 30s. Log:\n{log}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until this process publishes its RPC endpoint to group-0.
    ///
    /// HTTP readiness only proves the local service has started. Clients
    /// discover chunkdb through the service registry, so an end-to-end test
    /// must wait for this publication before issuing its first RPC.
    pub async fn wait_for_registry_ready(&self, service_registry: &ServiceRegistryClient) {
        let endpoint = format!("http://127.0.0.1:{}", self.listen_port + 200);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let registered = service_registry
                .read_instance("chunkdb", INSTANCE_ID)
                .await
                .ok()
                .flatten()
                .is_some_and(|value| value.rpc_endpoint == endpoint);
            if registered {
                eprintln!("crowdb-chunkdb registry ready at {endpoint}");
                return;
            }
            if Instant::now() > deadline {
                let observed = service_registry
                    .read_instance("chunkdb", INSTANCE_ID)
                    .await
                    .ok()
                    .flatten();
                panic!("chunkdb registry not ready at {endpoint} within 30s; observed={observed:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn conversion_metrics(&self) -> serde_json::Value {
        reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/conversion_metrics", self.http_port))
            .send()
            .await
            .expect("fetch conversion metrics")
            .error_for_status()
            .expect("conversion metrics status")
            .json()
            .await
            .expect("decode conversion metrics")
    }

    pub async fn repair_metrics(&self) -> serde_json::Value {
        reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/repair_metrics", self.http_port))
            .send()
            .await
            .expect("fetch repair metrics")
            .error_for_status()
            .expect("repair metrics status")
            .json()
            .await
            .expect("decode repair metrics")
    }

    pub fn crash(&mut self) {
        self.child.kill().expect("kill chunkdb process");
        self.child.wait().expect("reap chunkdb process");
    }
}

impl Drop for ChunkdbProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Check that all required binaries are available for E2E tests.
pub fn check_binaries() -> bool {
    let bin = crowdb_chunkdb_bin();
    crate::hardware::check_binaries(bin.as_deref())
}

/// Build a `ChunkdbClient` with standard retry config.
pub fn make_client(svc: crowdb_kv_client::ServiceRegistryClient) -> Arc<ChunkdbClient> {
    let transport = Arc::new(ChunkdbRpcTransport::new());
    Arc::new(ChunkdbClient::with_retry_config(
        svc,
        RetryConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
        },
        transport,
    ))
}
