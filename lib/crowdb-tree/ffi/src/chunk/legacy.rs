// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only validation of the historical logical-address manifest.
use crate::{crc32c, crc32c_update, CtError};

const HEADER: usize = 80;
const SEGMENT: usize = 33;
const PACK: usize = 53;
const REFERENCES_PER_SEGMENT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LegacyPack {
    pub owner_tree_id: u64,
    pub chunk_high: u64,
    pub chunk_low: u64,
    pub offset: u64,
    pub length: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct LegacyManifest<'a> {
    bytes: &'a [u8],
    packs_offset: usize,
}

impl<'a> LegacyManifest<'a> {
    /// Validate the complete immutable manifest, including its CRC and geometry.
    /// # Errors
    /// Returns corruption for any truncated, unsupported or inconsistent descriptor.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, CtError> {
        if bytes.len() < HEADER + 8 || bytes.len() > 16 * 1024 * 1024 || u32_at(bytes, 0) != 7 {
            return Err(CtError::Corruption);
        }
        let segments = usize::try_from(u64_at(bytes, 72)).map_err(|_| CtError::Corruption)?;
        if segments > (bytes.len() - HEADER - 8) / SEGMENT {
            return Err(CtError::Corruption);
        }
        let packs_offset = HEADER + segments * SEGMENT + 8;
        let packs = usize::try_from(u64_at(bytes, packs_offset - 8)).map_err(|_| CtError::Corruption)?;
        if packs != (bytes.len() - packs_offset) / PACK
            || !(bytes.len() - packs_offset).is_multiple_of(PACK)
            || segments != packs.div_ceil(REFERENCES_PER_SEGMENT)
        {
            return Err(CtError::Corruption);
        }
        let view = Self { bytes, packs_offset };
        if view.tree_id() == 0 || view.owner_epoch() == 0 || matches!(view.generation(), 0 | u64::MAX) {
            return Err(CtError::Corruption);
        }
        let crc = crc32c_update(crc32c(&bytes[..68]), &bytes[packs_offset..]);
        if crc32c_update(crc, &bytes[HEADER..packs_offset - 8]) != u32_at(bytes, 68) {
            return Err(CtError::Corruption);
        }
        let mut end = 0_u64;
        let mut reused = 0_u64;
        let mut reused_bytes = 0_u64;
        for (index, pack) in view.packs().enumerate() {
            let offset = packs_offset + index * PACK;
            let logical = u64_at(bytes, offset + 16);
            let length = u64::from(pack.length);
            if pack.owner_tree_id == 0
                || u64_at(bytes, offset + 8) != index as u64
                || !matches!(pack.chunk_high >> 56, 2 | 3)
                || length == 0
                || length > 256 * 1024 * 1024
                || logical < end
                || logical > u64_at(bytes, 28)
                || length > u64_at(bytes, 28) - logical
                || pack.offset.checked_add(length).is_none()
                || bytes[offset + 52] > 1
            {
                return Err(CtError::Corruption);
            }
            end = logical + length;
            if bytes[offset + 52] != 0 {
                reused += 1;
                reused_bytes += length;
            }
        }
        if reused != u64_at(bytes, 44) || reused_bytes != u64_at(bytes, 52) {
            return Err(CtError::Corruption);
        }
        for index in 0..segments {
            let offset = HEADER + index * SEGMENT;
            let first = index * REFERENCES_PER_SEGMENT;
            let count = (packs - first).min(REFERENCES_PER_SEGMENT);
            if u64_at(bytes, offset) == 0
                || u64_at(bytes, offset + 8) == 0
                || u64_at(bytes, offset + 16) != first as u64
                || u64::from(u32_at(bytes, offset + 24)) != count as u64
                || bytes[offset + 32] > 1
            {
                return Err(CtError::Corruption);
            }
        }
        Ok(view)
    }
    #[must_use]
    pub fn tree_id(self) -> u64 {
        u64_at(self.bytes, 4)
    }
    #[must_use]
    pub fn generation(self) -> u64 {
        u64_at(self.bytes, 12)
    }
    #[must_use]
    pub fn owner_epoch(self) -> u64 {
        u64_at(self.bytes, 20)
    }
    #[must_use]
    pub fn packs(self) -> impl ExactSizeIterator<Item = LegacyPack> + 'a {
        self.bytes[self.packs_offset..]
            .as_chunks::<PACK>()
            .0
            .iter()
            .map(|bytes| LegacyPack {
                owner_tree_id: u64_at(bytes, 0),
                chunk_high: u64_at(bytes, 24),
                chunk_low: u64_at(bytes, 32),
                offset: u64_at(bytes, 40),
                length: u32_at(bytes, 48),
            })
    }
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("validated manifest field"),
    )
}
fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("validated manifest field"),
    )
}
