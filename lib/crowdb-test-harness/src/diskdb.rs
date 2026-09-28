// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb test harness: subprocess management, binary discovery, and
//! concurrent benchmark for diskdb-client E2E tests.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport, RetryConfig};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::ServicePort;

use crate::hardware::{DG_ID, INSTANCE_ID, STORE_ID, UNIT_SIZE_BYTES, ZONE_SIZE_UNITS};

// Re-export hardware helpers for convenience.
pub use crate::cluster::crowdb_kv_server_bin;
pub use crate::hardware::{seed_hardware, standard_disk_ids_3};

pub fn make_chunk_id(high: u64, low: u64) -> ChunkId {
    ChunkId { high, low }
}

/// Find the crowdb-diskdb binary.
pub fn crowdb_diskdb_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CROWDB_DISKDB_BIN") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crowdb-diskdb");
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

// ── diskdb subprocess ────────────────────────────────────────────

pub struct DiskdbProcess {
    pub child: std::process::Child,
    pub instance_id: u64,
    pub listen_port: i32,
    pub rpc_port: i32,
    pub http_port: i32,
    pub config_path: std::path::PathBuf,
    pub log_path: std::path::PathBuf,
    runtime: Option<crate::test_dirs::TestRuntime>,
}

impl DiskdbProcess {
    pub fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Start crowdb-diskdb with a generated config pointing at the
    /// kv-server management seeds. `small_storage` enables compact test zones.
    pub fn start(kv_seeds: &[String], small_storage: bool) -> Self {
        let zone_size_units = small_storage.then_some(ZONE_SIZE_UNITS);
        Self::start_with_zone_size(kv_seeds, zone_size_units)
    }

    /// Start crowdb-diskdb with an explicit test zone size.
    pub fn start_with_zone_size(kv_seeds: &[String], zone_size_units: Option<u64>) -> Self {
        Self::start_for_instance(kv_seeds, INSTANCE_ID, zone_size_units)
    }

    /// Start one diskdb owner with an explicit group-0 instance identity.
    pub fn start_for_instance(kv_seeds: &[String], instance_id: u64, zone_size_units: Option<u64>) -> Self {
        let mut runtime = crate::test_dirs::TestRuntime::new("diskdb")
            .unwrap_or_else(|error| panic!("create DiskDB runtime namespace: {error}"));
        let mut process = Self::start_for_instance_in(&mut runtime, kv_seeds, instance_id, zone_size_units);
        process.runtime = Some(runtime);
        process
    }

    /// Start one DiskDB owner inside a shared runtime namespace.
    pub fn start_for_instance_in(
        runtime: &mut crate::test_dirs::TestRuntime,
        kv_seeds: &[String],
        instance_id: u64,
        zone_size_units: Option<u64>,
    ) -> Self {
        let bin = crowdb_diskdb_bin().unwrap_or_else(|| {
            panic!("crowdb-diskdb binary not found; set CROWDB_DISKDB_BIN or build app/crowdb-diskdb")
        });

        // Allocate listen/rpc/http ports from the fixed diskdb ranges
        // (11000-11699) via the port allocator. These ranges sit below
        // the Linux ephemeral range (32768-60999), so the kernel never
        // reassigns them between the probe and the subprocess bind — the
        // TOCTOU that plagues `bind(:0)`-style ephemeral port selection
        // under load. The shared per-process claim file keeps the three
        // independently assigned ports pairwise distinct.
        let logical_identity = format!("instance-{instance_id}");
        let listen_port = i32::from(
            runtime
                .assign_named_port(ServicePort::DiskdbListen, &logical_identity)
                .unwrap_or_else(|error| panic!("assign DiskDB listen port: {error}")),
        );
        let rpc_port = i32::from(
            runtime
                .assign_named_port(ServicePort::DiskdbRpc, &logical_identity)
                .unwrap_or_else(|error| panic!("assign DiskDB RPC port: {error}")),
        );
        let http_port = i32::from(
            runtime
                .assign_named_port(ServicePort::DiskdbHttp, &logical_identity)
                .unwrap_or_else(|error| panic!("assign DiskDB HTTP port: {error}")),
        );

        let storage_section = if let Some(zone_size_units) = zone_size_units {
            let zone_size_bytes = zone_size_units * u64::from(UNIT_SIZE_BYTES);
            format!(
                "\n[storage]\nzone_size_bytes = {zone_size_bytes}\nblock_size_bytes = {UNIT_SIZE_BYTES}\nallocate_granularity = {UNIT_SIZE_BYTES}\nzone_rotate_count = 4\ncas_retry_limit = 100\n"
            )
        } else {
            String::new()
        };
        let config_content = format!(
            r#"[server]
rpc_workers = 2
listen_addr = "127.0.0.1:{listen_port}"
rpc_listen_addr = "127.0.0.1:{rpc_port}"
http_listen_addr = "127.0.0.1:{http_port}"
instance_id = "{instance_id}"
kv_server_mgmt_seeds = [{seeds}]
{storage_section}
[sync]
group0_store_id = {STORE_ID}
group0_group_id = 0
sync_interval_secs = 2

[heartbeat]
interval_secs = 2
miss_threshold = 3
temp_failure_timeout_secs = 900

[reporting]
interval_secs = 2
"#,
            seeds = kv_seeds
                .iter()
                .map(|s| format!("\"{s}\""))
                .collect::<Vec<_>>()
                .join(", "),
        );

        let service_root = runtime
            .service_dir("diskdb", &logical_identity)
            .unwrap_or_else(|error| panic!("create DiskDB service root: {error}"));
        let config_path = service_root.join("config").join("diskdb.toml");
        std::fs::write(&config_path, &config_content).expect("write config");

        let log_path = service_root.join("log").join("diskdb.log");
        let log_file = std::fs::File::create(&log_path).expect("create log file");
        let log_file2 = log_file.try_clone().expect("clone log file");

        let mut cmd = Command::new(&bin);
        let test_log_dir = service_root.join("log");
        cmd.args(["--config", config_path.to_str().unwrap()])
            .arg("--log-dir")
            .arg(test_log_dir.to_str().unwrap())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file2));

        let child = cmd.spawn().expect("start crowdb-diskdb");
        runtime
            .record_process(child.id())
            .unwrap_or_else(|error| panic!("record DiskDB process: {error}"));
        eprintln!("crowdb-diskdb log: {}", log_path.display());

        Self {
            child,
            instance_id,
            listen_port,
            rpc_port,
            http_port,
            config_path,
            log_path,
            runtime: None,
        }
    }

    /// Wait for the diskdb HTTP `/ready` endpoint to return 200.
    pub async fn wait_for_ready(&self) {
        let url = format!("http://127.0.0.1:{}/ready", self.http_port);
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    eprintln!("crowdb-diskdb ready (phase=up)");
                    return;
                }
            }
            if Instant::now() > deadline {
                let log = self.log_content();
                panic!("crowdb-diskdb did not become ready within 30s. Log:\n{log}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until this process has published its current RPC endpoint to
    /// group-0 and that endpoint accepts an ownership read.
    pub async fn wait_for_registry_ready(
        &self,
        service_registry: &ServiceRegistryClient,
        disk_group_id: u64,
        not_before_ms: u64,
    ) {
        let endpoint = format!("127.0.0.1:{}", self.rpc_port);
        let deadline = Instant::now() + Duration::from_secs(30);
        let transport = DiskdbRpcTransport::new();
        loop {
            let registered = service_registry
                .read_instance("diskdb", self.instance_id)
                .await
                .ok()
                .flatten()
                .is_some_and(|value| {
                    value.rpc_endpoint == endpoint
                        && value.last_heartbeat_ms >= not_before_ms
                        && value
                            .extra
                            .as_ref()
                            .and_then(|extra| extra.diskdb.as_ref())
                            .is_some_and(|diskdb| diskdb.owned_dg_ids.contains(&disk_group_id))
                });
            if registered
                && transport
                    .get_disk_group_info(&endpoint, disk_group_id)
                    .await
                    .is_ok()
            {
                eprintln!("crowdb-diskdb registry ready at {endpoint}");
                return;
            }
            if Instant::now() > deadline {
                let observed = service_registry
                    .read_instance("diskdb", self.instance_id)
                    .await
                    .ok()
                    .flatten();
                panic!("diskdb registry/RPC not ready at {endpoint} within 30s; observed={observed:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Restart the same logical server with its original identity and ports.
    pub async fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let binary = crowdb_diskdb_bin().unwrap_or_else(|| {
            panic!("crowdb-diskdb binary not found; set CROWDB_DISKDB_BIN or build app/crowdb-diskdb")
        });
        let log_file = std::fs::File::create(&self.log_path).expect("recreate DiskDB log");
        let log_error = log_file.try_clone().expect("clone DiskDB log");
        self.child = Command::new(binary)
            .args(["--config", self.config_path.to_str().expect("UTF-8 config path")])
            .arg("--log-dir")
            .arg(self.log_path.parent().expect("DiskDB log directory"))
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_error))
            .spawn()
            .expect("restart crowdb-diskdb");
        eprintln!("crowdb-diskdb restarted; log: {}", self.log_path.display());
        self.wait_for_ready().await;
    }
}

impl Drop for DiskdbProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── concurrent benchmark ─────────────────────────────────────────

const BENCH_THREADS: usize = 4;
const BENCH_CYCLES: usize = 100;

/// Run a concurrent allocate/free benchmark: `BENCH_THREADS` tasks
/// each doing `BENCH_CYCLES` allocate-1-block + free-1-block cycles.
#[allow(clippy::cast_precision_loss)]
pub async fn run_concurrent_benchmark(client: &Arc<DiskdbClient>) {
    use crowdb_protocol::diskdb::rpc::{AllocateBlocksRequest, FreeBlocksRequest};

    let start = Instant::now();
    let mut handles = Vec::with_capacity(BENCH_THREADS);

    for tid in 0..BENCH_THREADS {
        let client = Arc::clone(client);
        handles.push(tokio::spawn(async move {
            let mut ok = 0usize;
            let mut errors = 0usize;
            for i in 0..BENCH_CYCLES {
                let owner = make_chunk_id(u64::try_from(tid).unwrap(), u64::try_from(i).unwrap());

                let alloc_req = AllocateBlocksRequest {
                    disk_group_id: DG_ID,
                    unit_count: 1,
                    count: 1,
                    exclude_disk_ids: vec![],
                    owner_chunk: Some(owner),
                    allow_disk_reuse: false,
                };
                let alloc_resp = match client.allocate_blocks(alloc_req).await {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("  bench tid={tid} cycle={i}: allocate error: {e}");
                        errors += 1;
                        continue;
                    }
                };
                if alloc_resp.segments.is_empty() {
                    eprintln!("  bench tid={tid} cycle={i}: allocate returned 0 segments");
                    errors += 1;
                    continue;
                }

                let free_req = FreeBlocksRequest {
                    segments: alloc_resp.segments,
                };
                match client.free_blocks(free_req).await {
                    Ok(r) => {
                        if r.freed_count > 0 {
                            ok += 1;
                        } else {
                            errors += 1;
                        }
                    }
                    Err(e) => {
                        eprintln!("  bench tid={tid} cycle={i}: free error: {e}");
                        errors += 1;
                    }
                }
            }
            (ok, errors)
        }));
    }

    let mut total_ok = 0usize;
    let mut total_err = 0usize;
    for h in handles {
        let (ok, err) = h.await.expect("benchmark task panicked");
        total_ok += ok;
        total_err += err;
    }

    let elapsed = start.elapsed();
    let total_ops = total_ok + total_err;
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = if secs > 0.0 { total_ops as f64 / secs } else { 0.0 };

    eprintln!(
        "  concurrent benchmark: {BENCH_THREADS} threads × {BENCH_CYCLES} cycles — {total_ops} ops in {elapsed:.2?} ({ops_per_sec:.0} ops/s, {total_ok} ok, {total_err} errors)"
    );
    assert_eq!(total_err, 0, "benchmark should have 0 errors");
    assert_eq!(
        total_ok,
        BENCH_THREADS * BENCH_CYCLES,
        "all benchmark ops should succeed"
    );
}

/// Require both subprocess binaries used by diskdb component tests.
pub fn require_binaries() {
    let bin = crowdb_diskdb_bin();
    assert!(
        crate::hardware::check_binaries(bin.as_deref()),
        "diskdb component tests require built crowdb-kv-server and crowdb-diskdb binaries"
    );
}

/// Build a `DiskdbClient` with standard retry config.
pub fn make_client(svc: crowdb_kv_client::ServiceRegistryClient) -> Arc<DiskdbClient> {
    let transport = Arc::new(DiskdbRpcTransport::new());
    Arc::new(DiskdbClient::new(svc, transport).with_retry_config(RetryConfig {
        max_retries: 5,
        initial_backoff: Duration::from_millis(100),
    }))
}
