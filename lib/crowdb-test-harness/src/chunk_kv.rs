// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk-KV server subprocess management for full-stack tests.

use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crowdb_protocol::port::alloc::alloc_test_port;
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
}

impl ChunkKvProcess {
    pub fn start(kv_seeds: &[String]) -> Self {
        let binary = crowdb_chunk_kv_server_bin().unwrap_or_else(|| {
            panic!("crowdb-chunk-kv-server binary not found; build it or set CROWDB_CHUNK_KV_SERVER_BIN")
        });
        let rpc_port = alloc_test_port(ServicePort::ChunkKvRpc);
        let http_port = alloc_test_port(ServicePort::ChunkKvHttp);
        let instance = INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let instance_id = 10_000 + instance;
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
        let config_path = crate::test_dirs::test_data_dir()
            .join(format!("chunk-kv-config-{}-{instance}.toml", std::process::id()));
        std::fs::write(&config_path, config).expect("write Chunk-KV config");
        let log_path = crate::test_dirs::test_log_dir().join(format!(
            "crowdb-chunk-kv-e2e-{}-{instance}.log",
            std::process::id()
        ));
        let log_file = std::fs::File::create(&log_path).expect("create Chunk-KV log");
        let log_error = log_file.try_clone().expect("clone Chunk-KV log");
        let child = Command::new(binary)
            .args(["--config", config_path.to_str().expect("UTF-8 config path")])
            .arg("--log-dir")
            .arg(crate::test_dirs::test_log_dir())
            .arg("--log")
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_error))
            .spawn()
            .expect("start crowdb-chunk-kv-server");
        eprintln!("crowdb-chunk-kv-server log: {}", log_path.display());
        Self {
            child,
            rpc_port,
            http_port,
            config_path,
            log_path,
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
}

impl Drop for ChunkKvProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
