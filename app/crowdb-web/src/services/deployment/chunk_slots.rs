// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Initialize a fixed layout once for the configured local deployment plan.

use crate::{
    error::{err_409, err_502},
    services::Failure,
    state::AppState,
};
use crowdb_kv_client::{ChunkSlotMapClient, CrowdbSysmdClient, GetOutcome, ReadMode};
use crowdb_protocol::{
    chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup},
    key::{ChunkSlotMapHeadKey, TextKey},
};

pub(super) async fn prepare(state: &AppState, instance_id: u64, dynamic: bool) -> Result<(), Failure> {
    state
        .op_context()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let kv = state.kv_client().await;
    let maps = ChunkSlotMapClient::new(kv.clone());
    let service_key = ChunkSlotMapHeadKey::Service.to_path();
    if matches!(
        kv.get(0, 0, service_key.as_bytes(), ReadMode::Linearizable, None)
            .await
            .map_err(|error| err_502(error.to_string()))?,
        GetOutcome::Found { .. }
    ) {
        maps.read_service()
            .await
            .map_err(|error| err_502(error.to_string()))?;
        maps.read_storage()
            .await
            .map_err(|error| err_502(error.to_string()))?;
        if dynamic
            != maps
                .read_dynamic_service_snapshot()
                .await
                .map_err(|error| err_502(error.to_string()))?
                .is_some()
        {
            return Err(err_409(
                "ChunkDB ownership policy differs from the initialized cluster",
            ));
        }
        return Ok(());
    }
    let sysmd = CrowdbSysmdClient::from_shared(kv);
    let mut stores = sysmd
        .list_stores()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    stores.sort_by_key(|store| store.store_id);
    let mut storage = None;
    for store in stores {
        let groups = sysmd
            .list_groups_in_store(store.store_id)
            .await
            .map_err(|error| err_502(error.to_string()))?;
        if let Some(group) = groups
            .into_iter()
            .filter(|group| group.group_id != 0)
            .min_by_key(|group| group.group_id)
        {
            storage = Some(ChunkStorageGroup {
                store_id: store.store_id,
                group_id: group.group_id,
            });
            break;
        }
    }
    let storage = storage.ok_or_else(|| err_409("Create an ordinary data group before deploying CDB"))?;
    let (count, mut instances) = {
        let config = state.config.read().unwrap();
        (
            config.nodes.len().max(1),
            config
                .servers
                .iter()
                .filter(|server| server.service_type == crowdb_console_shared::config::ServiceType::Chunkdb)
                .filter_map(|server| server.id.strip_prefix("chunkdb-")?.parse::<u64>().ok())
                .collect::<Vec<_>>(),
        )
    };
    instances.push(instance_id);
    instances.sort_unstable();
    instances.dedup();
    // Reserve the next free service IDs for Nodes whose default deployment is
    // still waiting. Ownership is by instance ID, never by Node ID.
    let mut next = 1;
    while instances.len() < count {
        if !instances.contains(&next) {
            instances.push(next);
        }
        next += 1;
    }
    instances.sort_unstable();
    maps.initialize_layout(&ChunkSlotBootstrap {
        service_instances: instances,
        storage_groups: vec![storage],
    })
    .await
    .map_err(|error| err_502(format!("Chunk slot initialization: {error}")))?;
    if dynamic {
        maps.initialize_service_epochs()
            .await
            .map_err(|error| err_502(error.to_string()))?;
    }
    Ok(())
}
