// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashSet;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use super::{ChunkSlot, ChunkSlotBitmap, ChunkSlotError, CHUNK_SLOT_COUNT, CHUNK_SLOT_LAYOUT_VERSION};

/// Publish a head and all its owner records in one group-0 conditional batch.
/// Service and storage heads have independent generations and namespaces.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkSlotMapHead {
    pub layout_version: u32,
    pub generation: u64,
    pub owner_count: u32,
}

/// Exactly one record per owner, including service owners with no slots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkSlotBinding<O> {
    pub generation: u64,
    pub owner: O,
    pub slots: ChunkSlotBitmap,
}

/// Service maps use instance IDs; endpoint discovery is independent of ownership.
pub trait ChunkSlotOwner: Copy + Eq + Hash {
    fn is_valid(self) -> bool;
}

impl ChunkSlotOwner for u64 {
    fn is_valid(self) -> bool {
        self != 0
    }
}

impl ChunkSlotOwner for super::ChunkSlotAuthority {
    fn is_valid(self) -> bool {
        true
    }
}

/// An explicitly eligible direct-KV destination. Group zero is never eligible.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkStorageGroup {
    pub store_id: u64,
    pub group_id: u64,
}

impl ChunkSlotOwner for ChunkStorageGroup {
    fn is_valid(self) -> bool {
        self.group_id != 0
    }
}

/// Validated immutable snapshot; slot lookup allocates nothing and takes no lock.
#[derive(Clone, Debug)]
pub struct ChunkSlotMap<O> {
    head: ChunkSlotMapHead,
    bindings: Vec<ChunkSlotBinding<O>>,
    owner_indices: Box<[usize; CHUNK_SLOT_COUNT as usize]>,
}

impl<O: ChunkSlotOwner> ChunkSlotMap<O> {
    /// Build a complete generation, rejecting holes, overlaps and duplicate owners.
    ///
    /// # Errors
    /// Returns a layout, generation, identity or coverage error without a partial map.
    pub fn new(head: ChunkSlotMapHead, bindings: Vec<ChunkSlotBinding<O>>) -> Result<Self, ChunkSlotError> {
        if head.layout_version != CHUNK_SLOT_LAYOUT_VERSION
            || head.generation == 0
            || head.owner_count == 0
            || usize::try_from(head.owner_count).ok() != Some(bindings.len())
        {
            return Err(ChunkSlotError::InvalidHead);
        }
        let mut owners = HashSet::with_capacity(bindings.len());
        let mut owner_indices = Box::new([usize::MAX; CHUNK_SLOT_COUNT as usize]);
        for (index, binding) in bindings.iter().enumerate() {
            if binding.generation != head.generation {
                return Err(ChunkSlotError::MixedGeneration);
            }
            if !binding.owner.is_valid() || !owners.insert(binding.owner) {
                return Err(ChunkSlotError::InvalidOwner);
            }
            for slot in binding.slots.slots() {
                let entry = &mut owner_indices[usize::from(slot.value())];
                if *entry != usize::MAX {
                    return Err(ChunkSlotError::Overlap(slot.value()));
                }
                *entry = index;
            }
        }
        for slot in ChunkSlot::all() {
            if owner_indices[usize::from(slot.value())] == usize::MAX {
                return Err(ChunkSlotError::Missing(slot.value()));
            }
        }
        Ok(Self {
            head,
            bindings,
            owner_indices,
        })
    }

    #[must_use]
    pub fn head(&self) -> &ChunkSlotMapHead {
        &self.head
    }

    #[must_use]
    pub fn bindings(&self) -> &[ChunkSlotBinding<O>] {
        &self.bindings
    }

    #[must_use]
    pub fn owner(&self, slot: ChunkSlot) -> O {
        self.bindings[self.owner_indices[usize::from(slot.value())]].owner
    }
}
