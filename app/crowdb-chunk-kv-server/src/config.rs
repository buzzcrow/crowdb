// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::path::Path;

use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::{CHUNK_KV_HTTP_BASE, CHUNK_KV_RPC_BASE, KV_SERVER_MGMT_BASE};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::BalanceConfig;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("failed to read chunk KV server config: {0}")]
    Read(String),
    #[error("failed to decode chunk KV server config: {0}")]
    Decode(String),
    #[error("chunk KV server config is invalid: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChunkKvServerConfig {
    pub instance_id: u64,
    pub rpc_listen_addr: String,
    pub http_listen_addr: String,
    pub group0_mgmt_seeds: Vec<String>,
    pub max_hosted_partitions: usize,
    pub catalog_refresh_interval_ms: u64,
    pub shutdown_drain_timeout_ms: u64,
    pub storage: StorageConfig,
    pub monitor: DomainMonitorDescriptor,
    pub balance: BalanceConfig,
}

impl Default for ChunkKvServerConfig {
    fn default() -> Self {
        Self {
            instance_id: 0,
            rpc_listen_addr: format!("0.0.0.0:{CHUNK_KV_RPC_BASE}"),
            http_listen_addr: format!("0.0.0.0:{CHUNK_KV_HTTP_BASE}"),
            group0_mgmt_seeds: vec![format!("http://127.0.0.1:{KV_SERVER_MGMT_BASE}")],
            max_hosted_partitions: 256,
            catalog_refresh_interval_ms: 5_000,
            shutdown_drain_timeout_ms: 30_000,
            storage: StorageConfig::default(),
            monitor: default_monitor(),
            balance: BalanceConfig::default(),
        }
    }
}

impl ChunkKvServerConfig {
    /// Loads a TOML configuration file and applies field defaults.
    ///
    /// # Errors
    ///
    /// Returns a typed file, TOML, or semantic validation error.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let encoded = std::fs::read_to_string(path).map_err(|error| ConfigError::Read(error.to_string()))?;
        let config: Self =
            toml::from_str(&encoded).map_err(|error| ConfigError::Decode(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Validates addresses, control-plane discovery, and bounded lifecycle policy.
    ///
    /// # Errors
    ///
    /// Returns a precise invalid configuration description.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.instance_id == 0 {
            return Err(ConfigError::Invalid("instance_id must be nonzero".into()));
        }
        parse_address("rpc_listen_addr", &self.rpc_listen_addr)?;
        parse_address("http_listen_addr", &self.http_listen_addr)?;
        if self.group0_mgmt_seeds.is_empty()
            || self.group0_mgmt_seeds.iter().any(|seed| seed.trim().is_empty())
        {
            return Err(ConfigError::Invalid("group0_mgmt_seeds must be nonempty".into()));
        }
        if self.max_hosted_partitions == 0
            || self.catalog_refresh_interval_ms == 0
            || self.shutdown_drain_timeout_ms == 0
        {
            return Err(ConfigError::Invalid(
                "partition capacity and lifecycle intervals must be nonzero".into(),
            ));
        }
        self.storage.validate()?;
        self.monitor
            .validate()
            .map_err(|error| ConfigError::Invalid(error.to_string()))?;
        if self.balance.target_partitions_per_owner == 0
            || self.balance.target_partition_bytes == 0
            || self.balance.minimum_weighted_improvement_percent > 100
            || self.balance.cooldown_ms == 0
        {
            return Err(ConfigError::Invalid("balance policy is invalid".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub metadata_store_id: u64,
    pub stream_writer_lease_ms: u64,
    pub diskio_connections_per_endpoint: usize,
    pub diskio_rpc_workers: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            metadata_store_id: 1,
            stream_writer_lease_ms: 30_000,
            diskio_connections_per_endpoint: 1,
            diskio_rpc_workers: 2,
        }
    }
}

impl StorageConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.metadata_store_id == 0
            || self.stream_writer_lease_ms == 0
            || self.diskio_connections_per_endpoint == 0
            || self.diskio_rpc_workers == 0
        {
            return Err(ConfigError::Invalid(
                "storage identifiers, lease, connections, and workers must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

fn parse_address(field: &str, value: &str) -> Result<SocketAddr, ConfigError> {
    value
        .parse()
        .map_err(|error| ConfigError::Invalid(format!("{field}: {error}")))
}

fn default_monitor() -> DomainMonitorDescriptor {
    DomainMonitorDescriptor {
        domain: "chunk-kv".into(),
        service_registry_name: "chunk-kv".into(),
        driver_version: 1,
        capability_version: 1,
        heartbeat_interval_ms: 2_000,
        suspect_after_ms: 6_000,
        dead_after_ms: 10_000,
        lease_duration_ms: 12_000,
        max_clock_skew_ms: 1_000,
        self_fence_margin_ms: 1_000,
        failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
        balance_policy: "count-first-v1".into(),
    }
}
