// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed service slot authority, independent of storage destinations.

use arc_swap::ArcSwapOption;
use crowdb_kv_client::{ChunkSlotMapClient, CrowdbKvClient};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBitmap, ChunkSlotMap, CHUNK_SLOT_COUNT};
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
struct OwnedSlots {
    generation: u64,
    instance_id: u64,
    slots: ChunkSlotBitmap,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("chunk slot {bucket} is not owned by this instance")]
pub struct NotMyRange {
    pub bucket: u16,
}

#[derive(Default)]
pub struct RangeGuard {
    owned: ArcSwapOption<OwnedSlots>,
}

impl RangeGuard {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Empty and uninitialized owners never admit chunk work.
    ///
    /// # Errors
    /// Returns the rejected slot when this instance has no authority for it.
    pub fn check(&self, chunk_id: &ChunkId) -> Result<(), NotMyRange> {
        let slot = ChunkSlot::for_chunk(chunk_id);
        if self
            .owned
            .load()
            .as_ref()
            .is_some_and(|owned| owned.slots.contains(slot))
        {
            Ok(())
        } else {
            Err(NotMyRange { bucket: slot.value() })
        }
    }

    /// Install the server's bitmap from a validated complete service map.
    ///
    /// # Errors
    /// Rejects missing owners and unsupported changes to initialized authority.
    pub fn install(&self, map: &ChunkSlotMap<u64>, instance_id: u64) -> crowdb_kv_client::Result<()> {
        let binding = map
            .bindings()
            .iter()
            .find(|binding| binding.owner == instance_id)
            .ok_or_else(|| invalid("instance is absent from the service slot map"))?;
        let replacement = Arc::new(OwnedSlots {
            generation: map.head().generation,
            instance_id,
            slots: binding.slots.clone(),
        });
        loop {
            let current = self.owned.load_full();
            if let Some(current) = &current {
                return if **current == *replacement {
                    Ok(())
                } else {
                    Err(invalid("service slot assignment is fixed; handoff is required"))
                };
            }
            let previous = self
                .owned
                .compare_and_swap(&current, Some(Arc::clone(&replacement)));
            if previous.is_none() {
                return Ok(());
            }
        }
    }

    /// Reload a complete fixed-layout service map; endpoint changes are separate.
    ///
    /// # Errors
    /// Returns read, validation or unsupported ownership-change errors.
    pub async fn load_from_group0(
        &self,
        kv: &Arc<CrowdbKvClient>,
        instance_id: u64,
    ) -> crowdb_kv_client::Result<()> {
        let map = ChunkSlotMapClient::new(Arc::clone(kv)).read_service().await?;
        self.install(&map, instance_id)
    }

    #[must_use]
    pub fn owned_bucket_count(&self) -> u64 {
        self.owned
            .load()
            .as_ref()
            .map_or(0, |owned| u64::try_from(owned.slots.slots().count()).unwrap_or(0))
    }

    #[must_use]
    pub fn quota_share(&self, total: u64) -> u64 {
        let scaled = u128::from(total) * u128::from(self.owned_bucket_count()) / u128::from(CHUNK_SLOT_COUNT);
        u64::try_from(scaled).unwrap_or(u64::MAX)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.owned
            .load()
            .as_ref()
            .map_or(true, |owned| owned.slots.is_empty())
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        !self.is_empty()
    }

    #[cfg(feature = "test-util")]
    pub fn replace_for_tests(&self, ranges: &[OwnedRange]) {
        let slots = ChunkSlot::all()
            .filter(|slot| {
                ranges
                    .iter()
                    .any(|range| slot.value() >= range.start && slot.value() <= range.end)
            })
            .collect();
        self.owned.store(Some(Arc::new(OwnedSlots {
            generation: 1,
            instance_id: 1,
            slots,
        })));
    }
}

#[cfg(feature = "test-util")]
#[derive(Debug, Clone, Copy)]
pub struct OwnedRange {
    pub start: u16,
    pub end: u16,
    pub sub_range_index: u32,
}

#[must_use]
pub fn chunk_bucket(chunk_id: &ChunkId) -> u16 {
    ChunkSlot::for_chunk(chunk_id).value()
}

fn invalid(reason: &str) -> crowdb_kv_client::Error {
    crowdb_kv_client::Error::SysdataDecode {
        key: "/chunkdb/slot_service/".into(),
        reason: reason.into(),
    }
}
