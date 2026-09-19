// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk-KV server subprocess management for full-stack tests.

use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crowdb_protocol::ServicePort;

static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn crowdb_chunk_kv_server_bin() -> Option<std::path::PathBuf> {
    if let Ok(value) = std::env::var("CROWDB_CHUNK_KV_SERVER_BIN") {
        let path = std::path::PathBuf::from(value);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(executable) = std::env::current_exe() {
        if let Some(directory) = executable.parent() {
            let mut path = directory.to_path_buf();
            for _ in 0..3 {
                let candidate = path.join("crowdb-chunk-kv-server");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !path.pop() {
                    break;
                }
            }
        }
    }
    None
}

pub struct ChunkKvProcess {
    child: std::process::Child,
    pub rpc_port: u16,
    pub http_port: u16,
    pub config_path: std::path::PathBuf,
    pub log_path: std::path::PathBuf,
    runtime: Option<crate::test_dirs::TestRuntime>,
}

impl ChunkKvProcess {
    pub fn start(kv_seeds: &[String]) -> Self {
        let mut runtime = crate::test_dirs::TestRuntime::new("chunk-kv")
            .unwrap_or_else(|error| panic!("create Chunk-KV runtime namespace: {error}"));
        let mut process = Self::start_in(&mut runtime, kv_seeds);
        process.runtime = Some(runtime);
        process
    }

    /// Start Chunk-KV inside a shared runtime namespace.
    pub fn start_in(runtime: &mut crate::test_dirs::TestRuntime, kv_seeds: &[String]) -> Self {
        let binary = crowdb_chunk_kv_server_bin().unwrap_or_else(|| {
            panic!("crowdb-chunk-kv-server binary not found; build it or set CROWDB_CHUNK_KV_SERVER_BIN")
        });
        let instance = INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let instance_id = 10_000 + instance;
        let logical_identity = format!("instance-{instance_id}");
        let rpc_port = runtime
            .assign_named_port(ServicePort::ChunkKvRpc, &logical_identity)
            .unwrap_or_else(|error| panic!("assign Chunk-KV RPC port: {error}"));
        let http_port = runtime
            .assign_named_port(ServicePort::ChunkKvHttp, &logical_identity)
            .unwrap_or_else(|error| panic!("assign Chunk-KV HTTP port: {error}"));
        let seeds = kv_seeds
            .iter()
            .map(|value| format!("\"{value}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let config = format!(
            r#"instance_id = {instance_id}
rpc_listen_addr = "127.0.0.1:{rpc_port}"
rpc_advertise_addr = "127.0.0.1:{rpc_port}"
http_listen_addr = "127.0.0.1:{http_port}"
group0_mgmt_seeds = [{seeds}]
catalog_refresh_interval_ms = 200

[balance]
enabled = true
target_partitions_per_owner = 1
target_partition_bytes = 9223372036854775807
minimum_weighted_improvement_percent = 100
cooldown_ms = 9223372036854775807
max_owner_request_rate = 0

[storage]
metadata_store_id = 0
stream_mirror_copies = 1

[bootstrap_partition]
partition_id = {{ high = 1, low = 1 }}
tree_id = 1
stream_name = {{ high = 2, low = 1 }}
owner_epoch = 1
metadata_group_id = 1
"#
        );
        let service_root = runtime
            .service_dir("chunk-kv", &logical_identity)
            .unwrap_or_else(|error| panic!("create Chunk-KV service root: {error}"));
        let config_path = service_root.join("config").join("chunk-kv.toml");
        std::fs::write(&config_path, config).expect("write Chunk-KV config");
        let log_path = service_root.join("log").join("chunk-kv.log");
        let log_file = std::fs::File::create(&log_path).expect("create Chunk-KV log");
        let log_error = log_file.try_clone().expect("clone Chunk-KV log");
        let child = Command::new(binary)
            .args(["--config", config_path.to_str().expect("UTF-8 config path")])
            .arg("--log-dir")
            .arg(service_root.join("log"))
            .arg("--log")
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_error))
            .spawn()
            .expect("start crowdb-chunk-kv-server");
        runtime
            .record_process(child.id())
            .unwrap_or_else(|error| panic!("record Chunk-KV process: {error}"));
        eprintln!("crowdb-chunk-kv-server log: {}", log_path.display());
        Self {
            child,
            rpc_port,
            http_port,
            config_path,
            log_path,
            runtime: None,
        }
    }

    pub fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    pub async fn wait_for_ready(&mut self) {
        let url = format!("http://127.0.0.1:{}/ready", self.http_port);
        let deadline = Instant::now() + Duration::from_secs(60);
        let client = reqwest::Client::new();
        loop {
            if let Some(status) = self.child.try_wait().expect("poll crowdb-chunk-kv-server") {
                panic!(
                    "crowdb-chunk-kv-server exited before readiness ({status}). Log:\n{}",
                    self.log_content()
                );
            }
            if client
                .get(&url)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                eprintln!("crowdb-chunk-kv-server ready");
                return;
            }
            assert!(
                Instant::now() <= deadline,
                "crowdb-chunk-kv-server did not become ready within 60s. Log:\n{}",
                self.log_content()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Restart the same logical server with its original identity and ports.
    pub async fn restart(&mut self) {
        self.restart_child().await;
        if let Some(runtime) = &mut self.runtime {
            runtime
                .record_process(self.child.id())
                .unwrap_or_else(|error| panic!("record restarted Chunk-KV process: {error}"));
        }
    }

    /// Restart a server that belongs to a caller-owned shared namespace.
    pub async fn restart_in(&mut self, runtime: &mut crate::test_dirs::TestRuntime) {
        self.restart_child().await;
        runtime
            .record_process(self.child.id())
            .unwrap_or_else(|error| panic!("record restarted Chunk-KV process: {error}"));
    }

    async fn restart_child(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let binary = crowdb_chunk_kv_server_bin().unwrap_or_else(|| {
            panic!("crowdb-chunk-kv-server binary not found; build it or set CROWDB_CHUNK_KV_SERVER_BIN")
        });
        let log_file = std::fs::File::create(&self.log_path).expect("recreate Chunk-KV log");
        let log_error = log_file.try_clone().expect("clone Chunk-KV log");
        self.child = Command::new(binary)
            .args(["--config", self.config_path.to_str().expect("UTF-8 config path")])
            .arg("--log-dir")
            .arg(self.log_path.parent().expect("Chunk-KV log directory"))
            .arg("--log")
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_error))
            .spawn()
            .expect("restart crowdb-chunk-kv-server");
        eprintln!(
            "crowdb-chunk-kv-server restarted; log: {}",
            self.log_path.display()
        );
        self.wait_for_ready().await;
    }
}

impl Drop for ChunkKvProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
