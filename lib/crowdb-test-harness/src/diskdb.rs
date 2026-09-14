// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb test harness: subprocess management, binary discovery, and
//! concurrent benchmark for diskdb-client E2E tests.

use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport, RetryConfig};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::port::alloc::alloc_test_port;
use crowdb_protocol::ServicePort;

use crate::hardware::{DG_ID, INSTANCE_ID, STORE_ID, UNIT_SIZE_BYTES, ZONE_SIZE_UNITS};

// Re-export hardware helpers for convenience.
pub use crate::cluster::crowdb_kv_server_bin;
pub use crate::hardware::{seed_hardware, standard_disk_ids_3};

static DISKDB_INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    pub listen_port: i32,
    pub rpc_port: i32,
    pub http_port: i32,
    pub config_path: std::path::PathBuf,
    pub log_path: std::path::PathBuf,
}

impl DiskdbProcess {
    pub fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Start crowdb-diskdb with a generated config pointing at the
    /// kv-server management seeds. `small_storage` enables compact test zones.
    pub fn start(kv_seeds: &[String], small_storage: bool) -> Self {
        let bin = crowdb_diskdb_bin().unwrap_or_else(|| {
            panic!("crowdb-diskdb binary not found; set CROWDB_DISKDB_BIN or build app/crowdb-diskdb")
        });

        // Allocate listen/rpc/http ports from the fixed diskdb ranges
        // (11000-11699) via the port allocator. These ranges sit below
        // the Linux ephemeral range (32768-60999), so the kernel never
        // reassigns them between the probe and the subprocess bind — the
        // TOCTOU that plagues `bind(:0)`-style ephemeral port selection
        // under load. The shared per-process claim file keeps the three
        // ports pairwise distinct. DiskdbListen and DiskdbRpc bases
        // differ by 200, so rpc_port = listen_port + 200, the offset
        // the client derives.
        let listen_port = i32::from(alloc_test_port(ServicePort::DiskdbListen));
        let rpc_port = i32::from(alloc_test_port(ServicePort::DiskdbRpc));
        debug_assert_eq!(
            rpc_port - listen_port,
            i32::from(crowdb_protocol::DISKDB_RPC_BASE) - i32::from(crowdb_protocol::DISKDB_LISTEN_BASE),
            "allocator must preserve the listen->rpc offset"
        );
        let http_port = i32::from(alloc_test_port(ServicePort::DiskdbHttp));

        let zone_size_bytes = ZONE_SIZE_UNITS * u64::from(UNIT_SIZE_BYTES);
        let storage_section = if small_storage {
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
instance_id = "{INSTANCE_ID}"
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

        let inst = DISKDB_INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let config_path = crate::test_dirs::test_data_dir()
            .join(format!("diskdb-config-{}-{inst}.toml", std::process::id()));
        std::fs::write(&config_path, &config_content).expect("write config");

        let log_path = crate::test_dirs::test_log_dir()
            .join(format!("crowdb-diskdb-e2e-{}-{inst}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path).expect("create log file");
        let log_file2 = log_file.try_clone().expect("clone log file");

        let mut cmd = Command::new(&bin);
        let test_log_dir = crate::test_dirs::test_log_dir();
        cmd.args(["--config", config_path.to_str().unwrap()])
            .arg("--log-dir")
            .arg(test_log_dir.to_str().unwrap())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file2));

        let child = cmd.spawn().expect("start crowdb-diskdb");
        eprintln!("crowdb-diskdb log: {}", log_path.display());

        Self {
            child,
            listen_port,
            rpc_port,
            http_port,
            config_path,
            log_path,
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
