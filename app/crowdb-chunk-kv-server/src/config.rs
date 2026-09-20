// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::path::Path;

use crowdb_protocol::chunk_kv::{
    ChunkKvRangeBalancePolicy, DomainFailurePolicy, DomainMonitorDescriptor, Id128,
};
use crowdb_protocol::chunk_stream::StreamName;
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
    pub rpc_advertise_addr: String,
    pub http_listen_addr: String,
    pub group0_mgmt_seeds: Vec<String>,
    pub max_hosted_partitions: usize,
    pub catalog_refresh_interval_ms: u64,
    pub max_split_catchup_lag_records: u64,
    pub shutdown_drain_timeout_ms: u64,
    pub rpc_workers: u32,
    pub storage: StorageConfig,
    pub bootstrap_partition: Option<BootstrapPartitionConfig>,
    pub monitor: DomainMonitorDescriptor,
    pub balance: BalanceConfig,
}

impl Default for ChunkKvServerConfig {
    fn default() -> Self {
        Self {
            instance_id: 0,
            rpc_listen_addr: format!("0.0.0.0:{CHUNK_KV_RPC_BASE}"),
            rpc_advertise_addr: format!("127.0.0.1:{CHUNK_KV_RPC_BASE}"),
            http_listen_addr: format!("0.0.0.0:{CHUNK_KV_HTTP_BASE}"),
            group0_mgmt_seeds: vec![format!("http://127.0.0.1:{KV_SERVER_MGMT_BASE}")],
            max_hosted_partitions: 256,
            catalog_refresh_interval_ms: 5_000,
            max_split_catchup_lag_records: 1_024,
            shutdown_drain_timeout_ms: 30_000,
            rpc_workers: 2,
            storage: StorageConfig::default(),
            bootstrap_partition: None,
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
        let mut config: Self =
            toml::from_str(&encoded).map_err(|error| ConfigError::Decode(error.to_string()))?;
        config.monitor.chunk_kv_range_balance =
            config.balance.enabled.then(|| balance_policy(&config.balance));
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
        let advertise = parse_address("rpc_advertise_addr", &self.rpc_advertise_addr)?;
        if advertise.ip().is_unspecified() {
            return Err(ConfigError::Invalid(
                "rpc_advertise_addr must be routable, not unspecified".into(),
            ));
        }
        parse_address("http_listen_addr", &self.http_listen_addr)?;
        if self.group0_mgmt_seeds.is_empty()
            || self.group0_mgmt_seeds.iter().any(|seed| seed.trim().is_empty())
        {
            return Err(ConfigError::Invalid("group0_mgmt_seeds must be nonempty".into()));
        }
        if self.max_hosted_partitions == 0
            || self.catalog_refresh_interval_ms == 0
            || self.max_split_catchup_lag_records == 0
            || self.shutdown_drain_timeout_ms == 0
            || self.rpc_workers == 0
        {
            return Err(ConfigError::Invalid(
                "partition capacity and lifecycle intervals must be nonzero".into(),
            ));
        }
        self.storage.validate()?;
        if let Some(bootstrap) = &self.bootstrap_partition {
            bootstrap.validate()?;
        }
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
        let expected_balance_policy = self.balance.enabled.then(|| balance_policy(&self.balance));
        if self.monitor.chunk_kv_range_balance != expected_balance_policy {
            return Err(ConfigError::Invalid(
                "monitor and server balance policy differ".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapPartitionConfig {
    pub partition_id: Id128,
    pub tree_id: u64,
    pub stream_name: StreamName,
    pub owner_epoch: u64,
    pub metadata_group_id: u64,
}

impl BootstrapPartitionConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.partition_id == Id128::default()
            || self.tree_id == 0
            || self.stream_name == StreamName::default()
            || self.owner_epoch == 0
            || self.metadata_group_id == 0
        {
            return Err(ConfigError::Invalid(
                "bootstrap partition identities and epoch must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub metadata_store_id: u64,
    pub stream_writer_lease_ms: u64,
    pub stream_mirror_copies: u32,
    pub diskio_connections_per_endpoint: usize,
    pub diskio_rpc_workers: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            metadata_store_id: 1,
            stream_writer_lease_ms: 30_000,
            stream_mirror_copies: 3,
            diskio_connections_per_endpoint: 1,
            diskio_rpc_workers: 2,
        }
    }
}

impl StorageConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.stream_writer_lease_ms == 0
            || self.stream_mirror_copies == 0
            || self.diskio_connections_per_endpoint == 0
            || self.diskio_rpc_workers == 0
        {
            return Err(ConfigError::Invalid(
                "storage lease, connections, and workers must be nonzero".into(),
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
        chunk_kv_range_balance: Some(ChunkKvRangeBalancePolicy::default()),
    }
}

fn balance_policy(config: &BalanceConfig) -> ChunkKvRangeBalancePolicy {
    ChunkKvRangeBalancePolicy {
        target_partitions_per_owner: u32::try_from(config.target_partitions_per_owner).unwrap_or(u32::MAX),
        target_partition_bytes: config.target_partition_bytes,
        minimum_weighted_improvement_percent: config.minimum_weighted_improvement_percent,
        cooldown_ms: config.cooldown_ms,
        max_owner_request_rate: config.max_owner_request_rate,
    }
}
