// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit local deployment initialization for fixed chunk slot ownership.

use super::{ensure_local_data_group, Error, LocalChunkdbDeployConfig, OpContext, Result};
use std::sync::Arc;

pub(super) async fn initialize(ctx: &OpContext, config: &LocalChunkdbDeployConfig) -> Result<()> {
    for group_id in &config.storage_groups {
        if *group_id == 0 {
            return Err(Error::Validation {
                field: "chunkdb_storage_groups".into(),
                message: "group 0 cannot store chunk state".into(),
            });
        }
        ensure_local_data_group(ctx, *group_id).await?;
    }
    let bootstrap = crowdb_protocol::chunk_slot::ChunkSlotBootstrap {
        service_instances: (0..config.instance_count)
            .map(|index| 20_000 + u64::try_from(index).unwrap_or(u64::MAX))
            .collect(),
        storage_groups: config
            .storage_groups
            .iter()
            .map(|group_id| crowdb_protocol::chunk_slot::ChunkStorageGroup {
                store_id: 0,
                group_id: *group_id,
            })
            .collect(),
    };
    let maps = crowdb_kv_client::ChunkSlotMapClient::new(Arc::clone(ctx.kv_arc()));
    maps.initialize_layout(&bootstrap).await?;
    if config.dynamic_ownership {
        maps.initialize_service_epochs().await?;
    }
    Ok(())
}
