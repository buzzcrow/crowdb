// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use super::{
    ChunkSlot, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotError, ChunkSlotMap, ChunkSlotMapHead,
    ChunkSlotOwner, ChunkStorageGroup, CHUNK_SLOT_COUNT, CHUNK_SLOT_LAYOUT_VERSION,
};

/// Explicit initial owners. Existing maps must match; this never authorizes a
/// resize. Service slots are interleaved and storage slots form balanced bands,
/// ensuring the two layers are independently assigned even at equal counts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkSlotBootstrap {
    pub service_instances: Vec<u64>,
    pub storage_groups: Vec<ChunkStorageGroup>,
}

impl ChunkSlotBootstrap {
    /// Build the complete initial service map, including zero-slot instances.
    ///
    /// # Errors
    /// Rejects empty lists, duplicate/invalid owners and unsupported sizes.
    pub fn service_map(&self) -> Result<ChunkSlotMap<u64>, ChunkSlotError> {
        assign(&self.service_instances, true)
    }

    /// Build the complete initial storage map from explicitly selected groups.
    ///
    /// # Errors
    /// Rejects empty lists, duplicate owners and group-zero destinations.
    pub fn storage_map(&self) -> Result<ChunkSlotMap<ChunkStorageGroup>, ChunkSlotError> {
        assign(&self.storage_groups, false)
    }
}

fn assign<O: ChunkSlotOwner>(owners: &[O], interleaved: bool) -> Result<ChunkSlotMap<O>, ChunkSlotError> {
    let count = u32::try_from(owners.len()).map_err(|_| ChunkSlotError::InvalidHead)?;
    if count == 0 {
        return Err(ChunkSlotError::InvalidHead);
    }
    let mut bindings: Vec<_> = owners
        .iter()
        .map(|owner| ChunkSlotBinding {
            generation: 1,
            owner: *owner,
            slots: ChunkSlotBitmap::default(),
        })
        .collect();
    for slot in ChunkSlot::all() {
        let index = if interleaved {
            u64::from(slot.value()) % u64::from(count)
        } else {
            u64::from(slot.value()) * u64::from(count) / u64::from(CHUNK_SLOT_COUNT)
        };
        let index = usize::try_from(index).map_err(|_| ChunkSlotError::InvalidHead)?;
        bindings[index].slots.insert(slot);
    }
    ChunkSlotMap::new(
        ChunkSlotMapHead {
            layout_version: CHUNK_SLOT_LAYOUT_VERSION,
            generation: 1,
            owner_count: count,
        },
        bindings,
    )
}
