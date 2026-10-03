// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production client wiring for stateless S3 handlers.

use std::sync::Arc;

use crate::metadata::ChunkKvMetadataStore;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, LargeWritePolicy,
    SmallWritePolicy,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig as ChunkKvConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_common::ec::EcScheme;
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::ChunkType;

#[derive(Clone)]
pub struct S3StorageClients {
    pub control: Arc<CrowdbKvClient>,
    pub metadata: Arc<ChunkKvMetadataStore>,
    pub chunks: Arc<ChunkIoClient>,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageConnectError {
    #[error("Chunk-KV client configuration failed: {0}")]
    ChunkKv(String),
    #[error("chunk I/O discovery failed: {0}")]
    ChunkIo(String),
}

pub fn own_large_write(policy: &mut LargeWritePolicy) {
    Arc::make_mut(&mut policy.client).chunk_type = ChunkType::S3;
}

pub struct S3WriteSettings {
    pub small: SmallWritePolicy,
    pub threshold_ratio: f64,
    pub disk_block_bytes: usize,
    pub ec_data: usize,
    pub ec_code: usize,
    pub large: S3LargeWriteSettings,
}

#[derive(Default)]
pub struct S3LargeWriteSettings {
    pub mirror_copies: Option<u32>,
    pub max_chunk_size: Option<u64>,
    pub memory_budget_bytes: Option<usize>,
    pub prefetch_strips_per_chunk: Option<usize>,
    pub prefetch_max_strips_per_batch: Option<usize>,
    pub parallel_strip_writes: Option<usize>,
    pub held_buffers: Option<usize>,
    pub chunk_preparation_depth: Option<usize>,
}

pub struct S3WritePolicies {
    pub small: SmallWritePolicy,
    pub large: LargeWritePolicy,
    pub small_threshold: usize,
}

impl S3WriteSettings {
    /// # Errors
    /// Rejects invalid S3 admission or large-write geometry before connecting storage.
    pub fn policies(self) -> Result<S3WritePolicies, String> {
        if self.ec_data == 0 || self.ec_data > 32 || self.ec_code == 0 {
            return Err("S3 EC data and code counts are invalid".into());
        }
        if !self.threshold_ratio.is_finite()
            || !(0.0..=1.0).contains(&self.threshold_ratio)
            || self.threshold_ratio == 0.0
        {
            return Err("S3 small-object threshold ratio is invalid".into());
        }
        let mut small = self.small;
        small.chunk_type = ChunkType::S3;
        small.conversion_data_num = self.ec_data;
        small.conversion_code_num = self.ec_code;
        small.validate().map_err(|error| error.to_string())?;
        let data_shards = if small.conversion_enabled { self.ec_data } else { 1 };
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let small_threshold =
            (self.threshold_ratio * data_shards.saturating_mul(self.disk_block_bytes) as f64).ceil() as usize;
        if small_threshold == 0 || small_threshold > small.object_limit {
            return Err("S3 small-object threshold exceeds the shared writer limit".into());
        }
        let mut client = ChunkClientConfig {
            chunk_type: ChunkType::S3,
            large_mirror_copies: self.large.mirror_copies,
            read_buffer_size: self.disk_block_bytes,
            ..ChunkClientConfig::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
        };
        if let Some(value) = self.large.max_chunk_size {
            client.max_chunk_size = value;
        }
        if let Some(value) = self.large.memory_budget_bytes {
            client.memory_budget = value;
        }
        if let Some(value) = self.large.prefetch_strips_per_chunk {
            client.prefetch_strips_per_chunk = value;
        }
        if let Some(value) = self.large.prefetch_max_strips_per_batch {
            client.large_prefetch_max_strips_per_batch = value;
        }
        if let Some(value) = self.large.parallel_strip_writes {
            client.large_parallel_strip_writes = value;
        }
        if let Some(value) = self.large.held_buffers {
            client.large_held_buffers = value;
        }
        if let Some(value) = self.large.chunk_preparation_depth {
            client.chunk_preparation_depth = value;
        }
        client.validate().map_err(|error| error.to_string())?;
        Ok(S3WritePolicies {
            small,
            large: LargeWritePolicy {
                ec_scheme: EcScheme::new(self.ec_data, self.ec_code),
                client: Arc::new(client),
            },
            small_threshold,
        })
    }
}

impl S3StorageClients {
    /// Connects metadata and chunk clients through one discovery client.
    ///
    /// # Errors
    ///
    /// Returns before readiness on configuration or discovery failure.
    pub async fn connect(
        management_seeds: Vec<String>,
        diskio_connections_per_endpoint: usize,
        diskio_rpc_workers: u32,
        small_write: SmallWritePolicy,
    ) -> Result<Self, StorageConnectError> {
        Self::connect_with_read_policy(
            management_seeds,
            diskio_connections_per_endpoint,
            diskio_rpc_workers,
            small_write,
            ChunkReadPolicy::default(),
        )
        .await
    }

    /// # Errors
    /// Returns an error when the management or `DiskIO` connection cannot be established.
    pub async fn connect_with_read_policy(
        management_seeds: Vec<String>,
        diskio_connections_per_endpoint: usize,
        diskio_rpc_workers: u32,
        mut small_write: SmallWritePolicy,
        read_policy: ChunkReadPolicy,
    ) -> Result<Self, StorageConnectError> {
        small_write.chunk_type = ChunkType::S3;
        let kv = Arc::new(CrowdbKvClient::new(KvConfig::new(management_seeds.clone())));
        let config = ChunkKvConfig::default();
        let catalog = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::clone(&kv)));
        let transport = Arc::new(ChunkKvRpcTransport::new(config.max_owner_connections, 1, 2));
        let metadata = ChunkKvClient::new(config, catalog, transport)
            .map_err(|error| StorageConnectError::ChunkKv(error.to_string()))?;
        metadata
            .refresh_catalog()
            .await
            .map_err(|error| StorageConnectError::ChunkKv(error.to_string()))?;
        let chunks = ChunkIoClient::connect_with_kv_read_policy(
            ChunkIoClientConfig {
                management_seeds,
                diskio_connections_per_endpoint,
                diskio_rpc_workers,
                small_write,
            },
            Arc::clone(&kv),
            read_policy,
        )
        .await
        .map_err(|error| StorageConnectError::ChunkIo(error.to_string()))?;
        Ok(Self {
            control: kv,
            metadata: Arc::new(ChunkKvMetadataStore::new(Arc::new(metadata))),
            chunks: Arc::new(chunks),
        })
    }
}
