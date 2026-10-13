// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Complete an interrupted first checkpoint before catalog publication.

use super::StorageRuntimeError;

#[cfg(feature = "test-util")]
impl super::ChunkKvStorage {
    /// Resume a private bootstrap before its first catalog publication.
    ///
    /// # Errors
    /// Returns durable identity, WAL replay, and checkpoint errors.
    pub async fn recover_bootstrap_partition_for_tests(
        config: &BootstrapPartitionConfig,
        page_store: Arc<PageStore>,
        stream: ChunkStream,
    ) -> Result<Partition, StorageRuntimeError> {
        recover_bootstrap_partition(config, page_store, stream).await
    }
}
use crate::BootstrapPartitionConfig;
use crowdb_chunk_kv::{
    MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange, RequestId,
};
use crowdb_chunk_stream::ChunkStream;
use crowdb_tree_ffi::{CtError, PageStore};
use std::sync::Arc;

pub(super) async fn recover_bootstrap_partition(
    config: &BootstrapPartitionConfig,
    page_store: Arc<PageStore>,
    stream: ChunkStream,
) -> Result<Partition, StorageRuntimeError> {
    let pending = match page_store.chunk_manifest_generation() {
        Ok(generation) => generation == 0,
        Err(CtError::NotFound) => true,
        Err(error) => return Err(StorageRuntimeError::Tree(error.to_string())),
    };
    let partition = Partition::recover_native_latest_prepared_assignment(
        PartitionId {
            high: config.partition_id.high,
            low: config.partition_id.low,
        },
        PartitionRange {
            start: Some(Vec::new()),
            end: None,
        },
        config.owner_epoch,
        config.tree_id,
        PartitionConfig::default(),
        crowdb_tree_ffi::Config::default(),
        page_store,
        stream,
    )
    .await
    .map_err(|error| StorageRuntimeError::Partition(error.to_string()))?;
    if pending {
        partition
            .activate_recovered(config.owner_epoch)
            .map_err(|error| StorageRuntimeError::Partition(error.to_string()))?;
        initialize_bootstrap_root(&partition, config).await?;
    }
    Ok(partition)
}

pub(super) async fn initialize_bootstrap_root(
    partition: &Partition,
    config: &BootstrapPartitionConfig,
) -> Result<(), StorageRuntimeError> {
    // An empty native tree has no root generation to recover. Apply and remove
    // one deterministic bootstrap value before catalog visibility so the
    // first checkpoint has a durable root but exposes no reserved user key.
    for (client_sequence, operation) in [
        (
            1,
            MutationOperation::Put {
                key: Vec::new(),
                value: b"crowdb-bootstrap".to_vec(),
            },
        ),
        (2, MutationOperation::Delete { key: Vec::new() }),
    ] {
        partition
            .mutate(
                config.owner_epoch,
                RequestId {
                    client_high: config.partition_id.high,
                    client_low: config.partition_id.low,
                    client_sequence,
                },
                operation,
            )
            .await
            .map_err(|error| StorageRuntimeError::Partition(format!("bootstrap mutation: {error}")))?;
    }
    partition
        .checkpoint(config.owner_epoch)
        .await
        .map(|_| ())
        .map_err(|error| StorageRuntimeError::Partition(format!("bootstrap checkpoint: {error}")))
}
