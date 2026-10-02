// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Startup configuration shared by the S3 and Iceberg access processes.

use std::path::Path;

use crowdb_chunk_client::{ChunkReadPolicy, SmallWritePolicy};
use crowdb_common::config::{load_from_file, BaseConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct AccessConfig {
    pub deployment: DeploymentConfig,
    pub common: CommonConfig,
    pub read: ReadConfig,
    pub small_write: SmallWriteConfig,
    pub s3: S3Config,
    pub iceberg: IcebergConfig,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentMode {
    #[default]
    Production,
    TestSingleNode,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct DeploymentConfig {
    pub mode: DeploymentMode,
    pub max_node_failures: u32,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            mode: DeploymentMode::Production,
            max_node_failures: 1,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct CommonConfig {
    pub management_seeds: Vec<String>,
    pub diskio_connections_per_endpoint: usize,
    pub diskio_rpc_workers: u32,
}

impl Default for CommonConfig {
    fn default() -> Self {
        Self {
            management_seeds: Vec::new(),
            diskio_connections_per_endpoint: 2,
            diskio_rpc_workers: 2,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReadConfig {
    pub stream_window_bytes: usize,
    pub stream_slots: usize,
    pub global_stream_bytes: usize,
    pub recovery_memory_bytes: usize,
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            stream_window_bytes: 1024 * 1024,
            stream_slots: 3,
            global_stream_bytes: 256 * 1024 * 1024,
            recovery_memory_bytes: 256 * 1024 * 1024,
        }
    }
}

impl ReadConfig {
    #[must_use]
    pub fn policy(&self) -> ChunkReadPolicy {
        ChunkReadPolicy {
            stream_window_bytes: self.stream_window_bytes,
            stream_slots: self.stream_slots,
            global_stream_bytes: self.global_stream_bytes,
            recovery_memory_bytes: self.recovery_memory_bytes,
            ..ChunkReadPolicy::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SmallWriteConfig {
    pub threshold_ratio: f64,
    /// Data bytes per strip block used for the small-object routing boundary.
    pub disk_block_bytes: usize,
    pub conversion_enabled: bool,
    pub mirror_copies: Option<u32>,
    pub ec_data: usize,
    pub ec_code: usize,
    pub memory_budget_bytes: usize,
    pub queue_capacity: usize,
    pub min_pipelines: usize,
    pub max_pipelines: usize,
    pub max_batch_bytes: usize,
    pub chunk_capacity_bytes: u64,
    pub small_strip_prefetch_count: u32,
}

impl Default for SmallWriteConfig {
    fn default() -> Self {
        let policy = SmallWritePolicy::default();
        Self {
            threshold_ratio: 0.9,
            disk_block_bytes: 1024 * 1024,
            conversion_enabled: policy.conversion_enabled,
            mirror_copies: None,
            ec_data: policy.conversion_data_num,
            ec_code: policy.conversion_code_num,
            memory_budget_bytes: policy.memory_budget,
            queue_capacity: policy.queue_capacity,
            min_pipelines: policy.min_pipelines,
            max_pipelines: policy.max_pipelines,
            max_batch_bytes: policy.max_batch_bytes,
            chunk_capacity_bytes: policy.chunk_capacity,
            small_strip_prefetch_count: policy.small_strip_prefetch_count,
        }
    }
}

impl SmallWriteConfig {
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn threshold_exclusive(&self) -> usize {
        let data_shards = if self.conversion_enabled { self.ec_data } else { 1 };
        (self.threshold_ratio * data_shards.saturating_mul(self.disk_block_bytes) as f64).ceil() as usize
    }

    #[must_use]
    pub fn policy(&self) -> SmallWritePolicy {
        SmallWritePolicy {
            conversion_enabled: self.conversion_enabled,
            mirror_copies: self
                .mirror_copies
                .unwrap_or(SmallWritePolicy::default().mirror_copies),
            conversion_data_num: self.ec_data,
            conversion_code_num: self.ec_code,
            memory_budget: self.memory_budget_bytes,
            queue_capacity: self.queue_capacity,
            min_pipelines: self.min_pipelines,
            max_pipelines: self.max_pipelines,
            max_batch_bytes: self.max_batch_bytes,
            chunk_capacity: self.chunk_capacity_bytes,
            small_strip_prefetch_count: self.small_strip_prefetch_count,
            ..SmallWritePolicy::default()
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct S3Config {
    /// Overrides the legacy shared small-write policy for S3 only.
    pub small_write: Option<SmallWriteConfig>,
    pub listen: Option<String>,
    pub tenant: Option<String>,
    pub region: Option<String>,
    pub trusted_network: Option<bool>,
    pub small_object_limit: Option<usize>,
    pub list_scan_items: Option<usize>,
    pub list_scan_bytes: Option<usize>,
    pub continuation_ttl_seconds: Option<u64>,
    pub native_budget_bytes: Option<usize>,
    pub cleanup_backlog_limit: Option<u64>,
    pub ec_data: Option<usize>,
    pub ec_code: Option<usize>,
    pub max_chunk_size: Option<u64>,
    pub large_memory_budget_bytes: Option<usize>,
    pub large_prefetch_strips_per_chunk: Option<usize>,
    pub large_prefetch_max_strips_per_batch: Option<usize>,
    pub large_parallel_strip_writes: Option<usize>,
    pub large_held_buffers: Option<usize>,
    pub large_chunk_preparation_depth: Option<usize>,
    pub large_mirror_copies: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct IcebergConfig {
    /// Overrides the legacy shared small-write policy for Iceberg only.
    pub small_write: Option<SmallWriteConfig>,
    pub listen: Option<String>,
    pub native_budget_bytes: Option<usize>,
    pub ec_data: Option<usize>,
    pub ec_code: Option<usize>,
    pub max_chunk_size: Option<u64>,
    pub large_memory_budget_bytes: Option<usize>,
    pub large_prefetch_strips_per_chunk: Option<usize>,
    pub large_prefetch_max_strips_per_batch: Option<usize>,
    pub large_parallel_strip_writes: Option<usize>,
    pub large_held_buffers: Option<usize>,
    pub large_chunk_preparation_depth: Option<usize>,
    pub large_mirror_copies: Option<u32>,
    pub gc: IcebergGcConfig,
}

impl AccessConfig {
    #[must_use]
    pub fn s3_small_write(&self) -> &SmallWriteConfig {
        self.s3.small_write.as_ref().unwrap_or(&self.small_write)
    }

    #[must_use]
    pub fn iceberg_small_write(&self) -> &SmallWriteConfig {
        self.iceberg.small_write.as_ref().unwrap_or(&self.small_write)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct IcebergGcConfig {
    pub enabled: Option<bool>,
    pub interval_ms: Option<u64>,
    pub catalogs: Option<Vec<String>>,
    pub step_bytes: Option<u32>,
    pub step_ms: Option<u32>,
    pub page_items: Option<u16>,
    pub page_bytes: Option<u32>,
    pub concurrency: Option<u16>,
    pub retry_base_ms: Option<u32>,
    pub retry_max_ms: Option<u32>,
    pub corruption_attempts: Option<u16>,
    pub minimum_retention_ms: Option<u64>,
    pub kv_bytes: Option<u64>,
    pub kv_requests: Option<u32>,
    pub chunk_bytes: Option<u64>,
    pub chunk_requests: Option<u32>,
}

impl BaseConfig for AccessConfig {
    fn validate(&self) -> Result<(), String> {
        if self.common.diskio_connections_per_endpoint == 0 || self.common.diskio_rpc_workers == 0 {
            return Err("common DiskIO connections and RPC workers must be nonzero".into());
        }
        if self.read.stream_slots == 0 || self.read.stream_slots > 64 {
            return Err("read.stream_slots must be between 1 and 64".into());
        }
        if self.read.stream_window_bytes < 64 * 1024 || self.read.stream_window_bytes > 1024 * 1024 {
            return Err("read.stream_window_bytes must be between 64 KiB and 1 MiB".into());
        }
        if self.read.global_stream_bytes < 1024 * 1024 || self.read.global_stream_bytes > u32::MAX as usize {
            return Err("read.global_stream_bytes must be at least 1 MiB and below 4 GiB".into());
        }
        if self.read.recovery_memory_bytes < 1024 * 1024
            || self.read.recovery_memory_bytes > u32::MAX as usize
        {
            return Err("read.recovery_memory_bytes must be between 1 MiB and 4 GiB".into());
        }
        self.validate_small_writes()?;
        if self.s3.ec_data == Some(0) || self.s3.ec_code == Some(0) {
            return Err("S3 EC data and code counts must be nonzero".into());
        }
        if self.s3.max_chunk_size == Some(0) || self.s3.native_budget_bytes == Some(0) {
            return Err("S3 chunk and native budgets must be nonzero".into());
        }
        if self.iceberg.native_budget_bytes == Some(0) {
            return Err("Iceberg native budget must be nonzero".into());
        }
        if self.iceberg.ec_data == Some(0)
            || self.iceberg.ec_code == Some(0)
            || self.iceberg.max_chunk_size == Some(0)
            || self.s3.large_memory_budget_bytes == Some(0)
            || self.s3.large_prefetch_strips_per_chunk == Some(0)
            || self.s3.large_prefetch_max_strips_per_batch == Some(0)
            || self.s3.large_parallel_strip_writes == Some(0)
            || self.s3.large_held_buffers == Some(0)
            || self.s3.large_chunk_preparation_depth == Some(0)
            || self.iceberg.large_memory_budget_bytes == Some(0)
            || self.iceberg.large_prefetch_strips_per_chunk == Some(0)
            || self.iceberg.large_prefetch_max_strips_per_batch == Some(0)
            || self.iceberg.large_parallel_strip_writes == Some(0)
            || self.iceberg.large_held_buffers == Some(0)
            || self.iceberg.large_chunk_preparation_depth == Some(0)
            || self.s3.large_mirror_copies == Some(0)
            || self.iceberg.large_mirror_copies == Some(0)
        {
            return Err("protocol large-write settings must be nonzero".into());
        }
        match self.deployment.mode {
            DeploymentMode::Production => {
                if self.deployment.max_node_failures != 1 {
                    return Err("production requires max_node_failures = 1".into());
                }
                if self.s3_small_write().policy().mirror_copies < 2
                    || self.iceberg_small_write().policy().mirror_copies < 2
                    || self.s3.large_mirror_copies == Some(1)
                    || self.iceberg.large_mirror_copies == Some(1)
                {
                    return Err("production access writes require protected strips".into());
                }
            }
            DeploymentMode::TestSingleNode => {
                if self.deployment.max_node_failures != 0 {
                    return Err("test_single_node requires max_node_failures = 0".into());
                }
                for config in [self.s3_small_write(), self.iceberg_small_write()] {
                    if config.conversion_enabled
                        || config.policy().mirror_copies != 1
                        || config.disk_block_bytes != 1024 * 1024
                    {
                        return Err("test_single_node requires one-copy 1 MiB mirror writes".into());
                    }
                }
                if self.s3.large_mirror_copies != Some(1) || self.iceberg.large_mirror_copies != Some(1) {
                    return Err("test_single_node requires one-copy large mirror strips".into());
                }
            }
        }
        if self.s3.small_object_limit == Some(0)
            || self.s3.list_scan_items == Some(0)
            || self.s3.list_scan_bytes == Some(0)
            || self.s3.continuation_ttl_seconds == Some(0)
        {
            return Err("S3 object and listing limits must be nonzero".into());
        }
        for listen in [self.s3.listen.as_deref(), self.iceberg.listen.as_deref()]
            .into_iter()
            .flatten()
        {
            listen
                .parse::<std::net::SocketAddr>()
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

impl AccessConfig {
    fn validate_small_writes(&self) -> Result<(), String> {
        for config in [
            &self.small_write,
            self.s3_small_write(),
            self.iceberg_small_write(),
        ] {
            config
                .policy()
                .validate()
                .map_err(|error| format!("invalid small_write config: {error}"))?;
            if !config.threshold_ratio.is_finite()
                || config.threshold_ratio <= 0.0
                || config.threshold_ratio > 1.0
                || config.disk_block_bytes < 128 * 1024
                || config.disk_block_bytes > 1024 * 1024
                || !config.disk_block_bytes.is_power_of_two()
                || config.ec_data == 0
                || config.ec_data > 32
                || config.threshold_exclusive() > config.policy().object_limit
            {
                return Err("small_write strip capacity or threshold is invalid".into());
            }
        }
        Ok(())
    }
}

/// Remove a single `--config <path>` pair and load the named TOML file.
///
/// # Errors
/// Rejects duplicate or missing paths and invalid configuration files.
pub fn load_args(mut args: Vec<String>) -> Result<(AccessConfig, Vec<String>), String> {
    let positions: Vec<_> = args
        .iter()
        .enumerate()
        .filter(|(_, value)| *value == "--config")
        .map(|(index, _)| index)
        .collect();
    if positions.len() > 1 {
        return Err("--config may be specified only once".into());
    }
    let Some(index) = positions.first().copied() else {
        return Ok((AccessConfig::default(), args));
    };
    if index + 1 >= args.len() {
        return Err("--config requires a path".into());
    }
    let path = args.remove(index + 1);
    args.remove(index);
    let config =
        load_from_file(Path::new(&path)).map_err(|error| format!("failed to load {path}: {error}"))?;
    Ok((config, args))
}
