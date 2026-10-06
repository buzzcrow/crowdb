// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable service-slot admission snapshots, independent of storage destinations.

use arc_swap::ArcSwapOption;
use crowdb_kv_client::{ChunkSlotMapClient, CrowdbKvClient};
use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotAuthority, ChunkSlotBitmap, ChunkSlotMap, CHUNK_SLOT_COUNT,
};
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
struct OwnedSlots {
    generation: u64,
    instance_id: u64,
    slots: ChunkSlotBitmap,
    epochs: Option<Box<[u64; CHUNK_SLOT_COUNT as usize]>>,
}

tokio::task_local! {
    static EXECUTION: ExecutionAuthority;
}

/// Immutable authority captured before an execution's first asynchronous step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionAuthority(Option<Arc<OwnedSlots>>);

impl ExecutionAuthority {
    pub(crate) fn cache_epoch(id: &ChunkId) -> Option<(u64, u64)> {
        EXECUTION
            .try_with(|execution| {
                execution.0.as_ref().and_then(|owned| {
                    owned.epochs.as_ref().map(|epochs| {
                        (
                            owned.instance_id,
                            epochs[usize::from(ChunkSlot::for_chunk(id).value())],
                        )
                    })
                })
            })
            .ok()
            .flatten()
    }
    pub async fn scope<F: std::future::Future>(self, future: F) -> F::Output {
        EXECUTION.scope(self, future).await
    }
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
    pub fn capture(&self) -> ExecutionAuthority {
        EXECUTION
            .try_with(Clone::clone)
            .unwrap_or_else(|_| ExecutionAuthority(self.owned.load_full()))
    }

    /// Local, allocation-free submission check. Already submitted work is not drained.
    /// Only this slot's owner/epoch matters, never the map publication generation.
    pub fn check_submission(&self, id: &ChunkId, captured: &ExecutionAuthority) -> Result<(), NotMyRange> {
        let slot = ChunkSlot::for_chunk(id);
        let current = self.owned.load();
        let valid = match (&captured.0, current.as_ref()) {
            (Some(before), Some(after)) => {
                before.instance_id == after.instance_id
                    && before.slots.contains(slot)
                    && after.slots.contains(slot)
                    && match (&before.epochs, &after.epochs) {
                        (None, None) => true,
                        (Some(a), Some(b)) => a[usize::from(slot.value())] == b[usize::from(slot.value())],
                        _ => false,
                    }
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(NotMyRange { bucket: slot.value() })
        }
    }

    /// Install complete dynamic authority; incarnation is diagnostic, not admission.
    /// Restart/regrant must durably advance each affected slot epoch before serving.
    pub fn install_dynamic(
        &self,
        map: &ChunkSlotMap<ChunkSlotAuthority>,
        instance_id: u64,
    ) -> crowdb_kv_client::Result<()> {
        let slots = ChunkSlot::all()
            .filter(|slot| map.owner(*slot).instance_id() == instance_id)
            .collect();
        let mut epochs = Box::new([0; CHUNK_SLOT_COUNT as usize]);
        for slot in ChunkSlot::all() {
            epochs[usize::from(slot.value())] = map.owner(slot).generation();
        }
        let replacement = Arc::new(OwnedSlots {
            generation: map.head().generation,
            instance_id,
            slots,
            epochs: Some(epochs),
        });
        loop {
            let current = self.owned.load_full();
            if let Some(before) = &current {
                if before.instance_id != instance_id
                    || before.epochs.is_none()
                    || replacement.generation < before.generation
                {
                    return Err(invalid(
                        "dynamic authority cannot replace fixed or newer authority",
                    ));
                }
                let old_epochs = before.epochs.as_ref().expect("dynamic snapshot has epochs");
                let new_epochs = replacement.epochs.as_ref().expect("dynamic snapshot has epochs");
                for slot in ChunkSlot::all() {
                    let index = usize::from(slot.value());
                    if new_epochs[index] < old_epochs[index]
                        || (before.slots.contains(slot) != replacement.slots.contains(slot)
                            && new_epochs[index] <= old_epochs[index])
                    {
                        return Err(invalid("slot epoch rollback or owner change without advancement"));
                    }
                }
                if replacement.generation == before.generation && **before != *replacement {
                    return Err(invalid("conflicting authority at the same generation"));
                }
            }
            let previous = self
                .owned
                .compare_and_swap(&current, Some(Arc::clone(&replacement)));
            if match (&*previous, &current) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            } {
                return Ok(());
            }
        }
    }
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
    /// Rejects unsupported changes to initialized authority. New instances
    /// start with no slots until an explicit ownership migration assigns work.
    pub fn install(&self, map: &ChunkSlotMap<u64>, instance_id: u64) -> crowdb_kv_client::Result<()> {
        let binding = map
            .bindings()
            .iter()
            .find(|binding| binding.owner == instance_id)
            .map_or_else(ChunkSlotBitmap::default, |binding| binding.slots.clone());
        let replacement = Arc::new(OwnedSlots {
            generation: map.head().generation,
            instance_id,
            slots: binding,
            epochs: None,
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
        let maps = ChunkSlotMapClient::new(Arc::clone(kv));
        if let Some(snapshot) = maps.read_dynamic_service_snapshot().await? {
            self.install_dynamic(snapshot.authority(), instance_id)
        } else {
            let map = maps.read_service().await?;
            self.install(&map, instance_id)
        }
    }

    /// Immutable fixed-layout authority used to bound maintenance scans.
    #[must_use]
    pub fn owned_slots(&self) -> ChunkSlotBitmap {
        self.owned
            .load()
            .as_ref()
            .map_or_else(ChunkSlotBitmap::default, |owned| owned.slots.clone())
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
        self.owned.load().is_some()
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
            epochs: None,
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
