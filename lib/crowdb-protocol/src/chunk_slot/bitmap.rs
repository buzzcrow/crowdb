// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use super::{ChunkSlot, ChunkSlotError, CHUNK_SLOT_BITMAP_BYTES};

/// Wire bit i is the low-to-high bit (i % 8) in byte (i / 8).
/// Deserialization rejects every length other than 128 bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub struct ChunkSlotBitmap([u8; CHUNK_SLOT_BITMAP_BYTES]);

impl Default for ChunkSlotBitmap {
    fn default() -> Self {
        Self([0; CHUNK_SLOT_BITMAP_BYTES])
    }
}

impl ChunkSlotBitmap {
    #[must_use]
    pub fn contains(&self, slot: ChunkSlot) -> bool {
        let index = usize::from(slot.value());
        self.0[index / 8] & (1 << (index % 8)) != 0
    }

    pub fn insert(&mut self, slot: ChunkSlot) {
        let index = usize::from(slot.value());
        self.0[index / 8] |= 1 << (index % 8);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|byte| *byte == 0)
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; CHUNK_SLOT_BITMAP_BYTES] {
        &self.0
    }

    pub fn slots(&self) -> impl Iterator<Item = ChunkSlot> + '_ {
        ChunkSlot::all().filter(|slot| self.contains(*slot))
    }
}

impl FromIterator<ChunkSlot> for ChunkSlotBitmap {
    fn from_iter<T: IntoIterator<Item = ChunkSlot>>(iter: T) -> Self {
        let mut bitmap = Self::default();
        for slot in iter {
            bitmap.insert(slot);
        }
        bitmap
    }
}

impl TryFrom<Vec<u8>> for ChunkSlotBitmap {
    type Error = ChunkSlotError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let length = bytes.len();
        Ok(Self(
            bytes
                .try_into()
                .map_err(|_| ChunkSlotError::BitmapLength(length))?,
        ))
    }
}

impl From<ChunkSlotBitmap> for Vec<u8> {
    fn from(bitmap: ChunkSlotBitmap) -> Self {
        bitmap.0.to_vec()
    }
}
