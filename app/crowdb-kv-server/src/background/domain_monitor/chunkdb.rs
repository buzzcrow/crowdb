// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Audits fixed chunk slot maps without assigning ownership from heartbeats.

use bytes::Bytes;
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::chunk_slot::{ChunkSlotBinding, ChunkSlotMap, ChunkSlotMapHead, ChunkStorageGroup};
use crowdb_protocol::key::{ChunkServiceSlotsKey, ChunkSlotMapHeadKey, ChunkStorageSlotsKey, TextKey};

use crate::group0_control_plane::Group0ControlPlane;

use super::{DomainMonitorDriver, DomainMonitorFuture};

#[derive(Default)]
pub struct ChunkdbRangeMonitorDriver;

impl ChunkdbRangeMonitorDriver {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl DomainMonitorDriver for ChunkdbRangeMonitorDriver {
    fn domain(&self) -> &'static str {
        "chunkdb"
    }

    fn driver_version(&self) -> u32 {
        2
    }

    fn tick<'a>(
        &'a self,
        control: &'a Group0ControlPlane,
        descriptor: &'a DomainMonitorDescriptor,
    ) -> DomainMonitorFuture<'a> {
        Box::pin(async move {
            if descriptor.driver_version != 2
                || descriptor.failure_policy != DomainFailurePolicy::OperatorOnly
                || descriptor.balance_policy != "fixed-slots-v1"
            {
                return Err("chunkdb requires the fixed slot monitor policy".into());
            }
            audit_maps(control).await
        })
    }
}

async fn audit_maps(control: &Group0ControlPlane) -> Result<(), String> {
    // One fixed-cutoff scan includes both heads and both owner tables. Registry
    // liveness is intentionally unrelated to persistent ownership assignment.
    let records = control
        .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 256)
        .await
        .map_err(|error| error.to_string())?;
    let mut service_head: Option<ChunkSlotMapHead> = None;
    let mut storage_head: Option<ChunkSlotMapHead> = None;
    let mut service = Vec::new();
    let mut storage = Vec::new();
    for record in records {
        let path = std::str::from_utf8(&record.key).map_err(|error| error.to_string())?;
        if path.starts_with("/chunkdb/range_") {
            return Err("legacy chunkdb range layout requires explicit conversion".into());
        }
        if path == ChunkSlotMapHeadKey::Service.to_path() {
            service_head = Some(decode(&record.value)?);
        } else if path == ChunkSlotMapHeadKey::Storage.to_path() {
            storage_head = Some(decode(&record.value)?);
        } else if path.starts_with(&ChunkServiceSlotsKey::prefix_all()) {
            let key = ChunkServiceSlotsKey::from_path(path).map_err(|error| error.to_string())?;
            let binding: ChunkSlotBinding<u64> = decode(&record.value)?;
            if key.instance_id != binding.owner || key.to_path() != path {
                return Err(format!("slot binding key/owner mismatch at {path}"));
            }
            service.push(binding);
        } else if path.starts_with(&ChunkStorageSlotsKey::prefix_all()) {
            let key = ChunkStorageSlotsKey::from_path(path).map_err(|error| error.to_string())?;
            let binding: ChunkSlotBinding<ChunkStorageGroup> = decode(&record.value)?;
            if key.store_id != binding.owner.store_id
                || key.group_id != binding.owner.group_id
                || key.to_path() != path
            {
                return Err(format!("slot binding key/owner mismatch at {path}"));
            }
            storage.push(binding);
        }
    }
    ChunkSlotMap::new(
        service_head.ok_or("service slot map is not initialized")?,
        service,
    )
    .map_err(|error| error.to_string())?;
    ChunkSlotMap::new(
        storage_head.ok_or("storage slot map is not initialized")?,
        storage,
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
}
