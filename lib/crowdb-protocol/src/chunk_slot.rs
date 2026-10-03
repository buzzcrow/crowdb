// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed chunk-ID hash space. Never derive placement from the number of owners.

mod bitmap;
mod bootstrap;
mod map;

pub use bitmap::ChunkSlotBitmap;
pub use bootstrap::ChunkSlotBootstrap;
pub use map::{ChunkSlotBinding, ChunkSlotMap, ChunkSlotMapHead, ChunkSlotOwner, ChunkStorageGroup};

use serde::{Deserialize, Serialize};

use crate::chunk_id::ChunkIdParts;
use crate::common::ChunkId;

pub const CHUNK_SLOT_COUNT: u16 = 1024;
pub const CHUNK_SLOT_BITMAP_BYTES: usize = 128;
/// Version 1 hashes high then low, both big endian, using xxh64 with seed zero.
pub const CHUNK_SLOT_LAYOUT_VERSION: u32 = 1;

/// A validated logical slot. Legacy 16-bit buckets are a different namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
pub struct ChunkSlot(u16);

impl ChunkSlot {
    /// Hash the canonical 16-byte chunk ID into the fixed slot space.
    #[must_use]
    pub fn for_chunk(id: &ChunkId) -> Self {
        let bytes = ChunkIdParts::from_proto(id).to_bytes();
        let hash = xxhash_rust::xxh64::xxh64(&bytes, 0).to_le_bytes();
        Self(u16::from_le_bytes([hash[0], hash[1]]) % CHUNK_SLOT_COUNT)
    }

    #[must_use]
    pub const fn value(self) -> u16 {
        self.0
    }

    pub fn all() -> impl ExactSizeIterator<Item = Self> {
        (0..CHUNK_SLOT_COUNT).map(Self)
    }
}

impl TryFrom<u16> for ChunkSlot {
    type Error = ChunkSlotError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if value >= CHUNK_SLOT_COUNT {
            return Err(ChunkSlotError::InvalidSlot(value));
        }
        Ok(Self(value))
    }
}

impl From<ChunkSlot> for u16 {
    fn from(slot: ChunkSlot) -> Self {
        slot.value()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ChunkSlotError {
    #[error("chunk slot {0} is outside the fixed 1024-slot space")]
    InvalidSlot(u16),
    #[error("chunk slot bitmap must contain exactly 128 bytes, got {0}")]
    BitmapLength(usize),
    #[error("unsupported chunk slot layout or invalid map generation/count")]
    InvalidHead,
    #[error("chunk slot map contains an invalid or duplicate owner")]
    InvalidOwner,
    #[error("chunk slot binding generation differs from its map head")]
    MixedGeneration,
    #[error("chunk slot {0} has more than one owner")]
    Overlap(u16),
    #[error("chunk slot {0} has no owner")]
    Missing(u16),
}
