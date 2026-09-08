// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Configuration for the chunkdb server.

use std::net::SocketAddr;

use crowdb_common::config::BaseConfig;
use crowdb_protocol::{CHUNKDB_HTTP_BASE, CHUNKDB_RPC_BASE, KV_SERVER_MGMT_BASE};
use serde::{Deserialize, Serialize};

/// Top-level configuration for a chunkdb instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkdbConfig {
    pub server: ServerConfig,
    pub topology: TopologyConfig,
    #[serde(default)]
    pub range_guard: RangeGuardConfig,
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
    #[serde(default)]
    pub placement: PlacementConfig,
    #[serde(default)]
    pub conversion: ConversionConfig,
    #[serde(default)]
    pub repair: RepairConfig,
}

/// Placement safety policy.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlacementConfig {
    /// Permit EC layouts that exceed the safe per-node failure bound.
    pub allow_unsafe_ec: bool,
}

impl BaseConfig for ChunkdbConfig {
    fn validate(&self) -> Result<(), String> {
        if self.server.kv_server_mgmt_seeds.is_empty() {
            return Err("server.kv_server_mgmt_seeds must not be empty".into());
        }
        if self.server.http_listen_addr.parse::<SocketAddr>().is_err() {
            return Err(format!(
                "server.http_listen_addr {:?} is not a valid SocketAddr",
                self.server.http_listen_addr,
            ));
        }
        if self.server.rpc_listen_addr.parse::<SocketAddr>().is_err() {
            return Err(format!(
                "server.rpc_listen_addr {:?} is not a valid SocketAddr",
                self.server.rpc_listen_addr,
            ));
        }
        if self.topology.refresh_interval_secs == 0 {
            return Err("topology.refresh_interval_secs must be > 0".into());
        }
        if self.server.kv_pool_size == 0
            || self.server.kv_rpc_workers == 0
            || self.server.diskdb_pool_size == 0
            || self.server.diskdb_rpc_workers == 0
        {
            return Err("server client pool sizes and RPC workers must be > 0".into());
        }
        self.lifecycle.validate()?;
        self.conversion.validate()?;
        self.repair.validate()?;
        Ok(())
    }
}

/// Background repair policy for strips marked unavailable by readers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairConfig {
    pub enabled: bool,
    /// Test/development escape hatch for clusters without a spare failure domain.
    pub allow_unsafe_placement: bool,
    pub max_concurrency: usize,
    pub memory_bytes: usize,
    pub scan_interval_secs: u64,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_unsafe_placement: false,
            max_concurrency: 4,
            memory_bytes: 64 * 1024 * 1024,
            scan_interval_secs: 1,
        }
    }
}

impl RepairConfig {
    fn validate(&self) -> Result<(), String> {
        if self.max_concurrency == 0 {
            return Err("repair.max_concurrency must be > 0".into());
        }
        if self.memory_bytes == 0 || self.memory_bytes > u32::MAX as usize {
            return Err("repair.memory_bytes must be in 1..=u32::MAX".into());
        }
        if self.scan_interval_secs == 0 {
            return Err("repair.scan_interval_secs must be > 0".into());
        }
        Ok(())
    }
}

/// Background mirror-to-EC conversion policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionConfig {
    /// Run automatic conversion scans. Manual triggers remain available.
    pub enabled: bool,
    pub data_num: u32,
    pub code_num: u32,
    pub min_seal_age_secs: u64,
    pub min_mirror_strips: u32,
    pub max_concurrency: usize,
    pub max_bandwidth_mbps: u64,
    pub scan_interval_secs: u64,
    #[serde(default = "default_conversion_task_lease_secs")]
    pub task_lease_secs: u64,
}

const fn default_conversion_task_lease_secs() -> u64 {
    30
}

impl Default for ConversionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            data_num: 8,
            code_num: 4,
            min_seal_age_secs: 3_600,
            min_mirror_strips: 8,
            max_concurrency: 4,
            max_bandwidth_mbps: 50,
            scan_interval_secs: 30,
            task_lease_secs: 30,
        }
    }
}

impl ConversionConfig {
    fn validate(&self) -> Result<(), String> {
        if self.data_num == 0 || self.code_num == 0 {
            return Err("conversion data_num and code_num must be > 0".into());
        }
        if self.min_mirror_strips < self.data_num {
            return Err("conversion.min_mirror_strips must be >= data_num".into());
        }
        if self.max_concurrency == 0 {
            return Err("conversion.max_concurrency must be > 0".into());
        }
        if self.max_bandwidth_mbps == 0 {
            return Err("conversion.max_bandwidth_mbps must be > 0".into());
        }
        if self.scan_interval_secs == 0 {
            return Err("conversion.scan_interval_secs must be > 0".into());
        }
        if self.task_lease_secs == 0 {
            return Err("conversion.task_lease_secs must be > 0".into());
        }
        Ok(())
    }
}

/// Range guard configuration (R99 sharded mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeGuardConfig {
    /// When `true` (default), an empty range guard allows all
    /// requests — preserving v1 single-instance behavior before the
    /// binding table is loaded. When `false`, an empty guard rejects
    /// all mutating requests until the binding table is loaded.
    pub allow_all_when_empty: bool,
}

impl Default for RangeGuardConfig {
    fn default() -> Self {
        Self {
            allow_all_when_empty: true,
        }
    }
}

/// HTTP + crowdb-rpc listen addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub http_listen_addr: String,
    /// crowdb-rpc listen address (R116 migration — runs alongside the
    /// HTTP listener).
    #[serde(default = "default_rpc_listen_addr")]
    pub rpc_listen_addr: String,
    pub instance_id: Option<String>,
    pub kv_server_mgmt_seeds: Vec<String>,
    /// KV client connections kept per endpoint.
    #[serde(default = "default_client_pool_size")]
    pub kv_pool_size: usize,
    /// crowdb-rpc I/O workers used by the KV client.
    #[serde(default = "default_client_rpc_workers")]
    pub kv_rpc_workers: u32,
    /// DiskDB client connections kept per endpoint.
    #[serde(default = "default_client_pool_size")]
    pub diskdb_pool_size: usize,
    /// crowdb-rpc I/O workers used by the DiskDB client.
    #[serde(default = "default_client_rpc_workers")]
    pub diskdb_rpc_workers: u32,
    /// Service-registry keep-alive interval in seconds. 0 disables
    /// registration (the binding monitor will not see this instance).
    /// Default: 10.
    #[serde(default = "default_keepalive_interval_secs")]
    pub keepalive_interval_secs: u32,
}

const fn default_client_pool_size() -> usize {
    1
}

const fn default_client_rpc_workers() -> u32 {
    2
}

fn default_keepalive_interval_secs() -> u32 {
    10
}

fn default_rpc_listen_addr() -> String {
    format!("0.0.0.0:{CHUNKDB_RPC_BASE}")
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_listen_addr: format!("0.0.0.0:{CHUNKDB_HTTP_BASE}"),
            rpc_listen_addr: default_rpc_listen_addr(),
            instance_id: None,
            kv_server_mgmt_seeds: vec![format!("http://127.0.0.1:{KV_SERVER_MGMT_BASE}")],
            kv_pool_size: default_client_pool_size(),
            kv_rpc_workers: default_client_rpc_workers(),
            diskdb_pool_size: default_client_pool_size(),
            diskdb_rpc_workers: default_client_rpc_workers(),
            keepalive_interval_secs: default_keepalive_interval_secs(),
        }
    }
}

/// Topology cache refresh configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConfig {
    pub refresh_interval_secs: u32,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            refresh_interval_secs: 30,
        }
    }
}

/// Lifecycle lock + cache configuration (R100).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// Max entries in the chunk payload cache.
    pub cache_capacity: usize,
    /// Reap idle chunk locks every N seconds.
    pub sweep_chunk_lock_interval_secs: u32,
    /// Warn if a chunk lock is held longer than N milliseconds.
    pub lock_hold_warn_threshold_ms: u64,
    /// Minimum lifetime of a retired strip layout before block reuse.
    #[serde(default = "default_layout_validity_ms")]
    pub layout_validity_ms: u64,
}

const fn default_layout_validity_ms() -> u64 {
    30_000
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            cache_capacity: 10_000,
            sweep_chunk_lock_interval_secs: 60,
            lock_hold_warn_threshold_ms: 1000,
            layout_validity_ms: default_layout_validity_ms(),
        }
    }
}

impl LifecycleConfig {
    fn validate(&self) -> Result<(), String> {
        if self.cache_capacity == 0 {
            return Err("lifecycle.cache_capacity must be > 0".into());
        }
        if self.sweep_chunk_lock_interval_secs == 0 {
            return Err("lifecycle.sweep_chunk_lock_interval_secs must be > 0".into());
        }
        if self.layout_validity_ms == 0 {
            return Err("lifecycle.layout_validity_ms must be > 0".into());
        }
        Ok(())
    }
}
