// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Complete epoch snapshots; every regrant advances only the affected slots.

use super::{ChunkSlot, ChunkSlotAuthority, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotError, ChunkSlotMap};
use std::collections::{HashMap, HashSet};

impl ChunkSlotMap<ChunkSlotAuthority> {
    /// Reassign or regrant slots, including same-instance restart. Unmoved epochs stay unchanged.
    /// Incarnation is retained as diagnostic data and never gates the reassignment.
    ///
    /// # Errors
    /// Rejects duplicate slots, invalid owners and exhausted publication or slot epochs.
    pub fn reassign(&self, assignments: &[(ChunkSlot, u64)]) -> Result<Self, ChunkSlotError> {
        let generation = self
            .head()
            .generation
            .checked_add(1)
            .ok_or(ChunkSlotError::InvalidHead)?;
        let mut targets = HashMap::with_capacity(assignments.len());
        let mut seen = HashSet::with_capacity(assignments.len());
        for &(slot, instance_id) in assignments {
            let previous = self.owner(slot);
            let epoch = previous
                .generation()
                .checked_add(1)
                .ok_or(ChunkSlotError::InvalidHead)?;
            if !seen.insert(slot) {
                return Err(ChunkSlotError::InvalidOwner);
            }
            let target = ChunkSlotAuthority::new(instance_id, previous.incarnation(), epoch)
                .map_err(|_| ChunkSlotError::InvalidOwner)?;
            targets.insert(slot, target);
        }
        let mut owners: HashMap<_, ChunkSlotBitmap> = HashMap::new();
        for slot in ChunkSlot::all() {
            owners
                .entry(targets.get(&slot).copied().unwrap_or_else(|| self.owner(slot)))
                .or_default()
                .insert(slot);
        }
        let mut bindings: Vec<_> = owners
            .into_iter()
            .map(|(owner, slots)| ChunkSlotBinding {
                generation,
                owner,
                slots,
            })
            .collect();
        bindings.sort_by_key(|binding| binding.owner.to_fence_value());
        let mut head = self.head().clone();
        head.generation = generation;
        head.owner_count = u32::try_from(bindings.len()).map_err(|_| ChunkSlotError::InvalidHead)?;
        Self::new(head, bindings)
    }
}
