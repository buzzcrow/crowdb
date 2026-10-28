// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::decode;
use crate::group0_control_plane::Group0ControlPlane;
use bytes::Bytes;
use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotAuthority, ChunkSlotBinding, ChunkSlotMap, ChunkSlotMapHead, ChunkStorageGroup,
};
use crowdb_protocol::key::{
    ChunkServiceAuthorityKey, ChunkServiceSlotsKey, ChunkSlotMapHeadKey, ChunkStorageSlotsKey, TextKey,
};
use std::collections::HashMap;

pub(super) async fn load_maps(
    control: &Group0ControlPlane,
) -> Result<(u64, ChunkSlotMap<u64>, ChunkSlotMap<ChunkSlotAuthority>), String> {
    let path = ChunkSlotMapHeadKey::Service.to_path();
    let before = control
        .get(path.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    let records = control
        .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 256)
        .await
        .map_err(|error| error.to_string())?;
    let mut service = Vec::new();
    let mut authority = Vec::new();
    let mut storage = Vec::new();
    let mut heads = HashMap::new();
    for record in records {
        let key = std::str::from_utf8(&record.key).map_err(|error| error.to_string())?;
        if key.starts_with("/chunkdb/range_") {
            return Err("legacy range layout is unsupported".into());
        }
        if key.starts_with("/chunkdb/slot_head/") {
            heads.insert(key.to_owned(), decode::<ChunkSlotMapHead>(&record.value)?);
        } else if key.starts_with(&ChunkServiceSlotsKey::prefix_all()) {
            let binding: ChunkSlotBinding<u64> = decode(&record.value)?;
            if (ChunkServiceSlotsKey {
                instance_id: binding.owner,
            })
            .to_path()
                != key
            {
                return Err("service key mismatch".into());
            }
            service.push(binding);
        } else if key.starts_with(&ChunkServiceAuthorityKey::prefix_all()) {
            let binding: ChunkSlotBinding<ChunkSlotAuthority> = decode(&record.value)?;
            if (ChunkServiceAuthorityKey {
                authority: binding.owner,
            })
            .to_path()
                != key
            {
                return Err("authority key mismatch".into());
            }
            authority.push(binding);
        } else if key.starts_with(&ChunkStorageSlotsKey::prefix_all()) {
            let binding: ChunkSlotBinding<ChunkStorageGroup> = decode(&record.value)?;
            if (ChunkStorageSlotsKey {
                store_id: binding.owner.store_id,
                group_id: binding.owner.group_id,
            })
            .to_path()
                != key
            {
                return Err("storage key mismatch".into());
            }
            storage.push(binding);
        }
    }
    let head = |kind: ChunkSlotMapHeadKey| {
        heads
            .get(&kind.to_path())
            .cloned()
            .ok_or_else(|| "slot head missing".to_owned())
    };
    let service =
        ChunkSlotMap::new(head(ChunkSlotMapHeadKey::Service)?, service).map_err(|error| error.to_string())?;
    let authority = ChunkSlotMap::new(head(ChunkSlotMapHeadKey::Authority)?, authority)
        .map_err(|error| error.to_string())?;
    ChunkSlotMap::new(head(ChunkSlotMapHeadKey::Storage)?, storage).map_err(|error| error.to_string())?;
    if service.head().generation != authority.head().generation
        || ChunkSlot::all().any(|slot| service.owner(slot) != authority.owner(slot).instance_id())
    {
        return Err("service and authority maps disagree".into());
    }
    let after = control
        .get(path.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    if before.revision != after.revision || before.value != after.value {
        return Err("service generation changed during audit".into());
    }
    Ok((before.revision, service, authority))
}
