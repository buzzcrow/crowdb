// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Compact append lengths with bounded, in-memory random-seek samples.

use super::{StreamChunkRecordError, MAX_STREAM_CHUNK_BYTES, MAX_STREAM_CHUNK_RECORD_BYTES};
use crate::frame::FrameLayout;
use serde::{Deserialize, Serialize};
use StreamChunkRecordError::Invalid;

mod block;
use block::{decode_block, encode_block};
const BLOCK_ENTRIES: usize = 256;
const MAX_ENTRIES: u64 = MAX_STREAM_CHUNK_BYTES / 48;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMappings {
    count: u32,
    #[serde(with = "bounded_bytes")]
    encoded: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamMapping {
    pub logical_start: u64,
    pub logical_end: u64,
    pub physical_start: u64,
}

#[derive(Clone, Copy, Debug)]
struct SeekSample {
    logical: u64,
    physical: u64,
    encoded: usize,
}

#[derive(Clone, Debug, Default)]
pub struct StreamMappingIndex {
    samples: Vec<SeekSample>,
    logical_end: u64,
    physical_end: u64,
}

impl StreamMappings {
    /// # Errors
    /// Rejects empty groups, overflow or geometry beyond one physical chunk.
    pub fn new(lengths: &[u32]) -> Result<Self, StreamChunkRecordError> {
        if lengths.len() as u64 > MAX_ENTRIES {
            return Err(Invalid("mapping count"));
        }
        let mut encoded = Vec::new();
        for block in lengths.chunks(BLOCK_ENTRIES) {
            encode_block(block, &mut encoded)?;
        }
        let result = Self {
            count: u32::try_from(lengths.len()).map_err(|_| Invalid("mapping count"))?,
            encoded,
        };
        let end = lengths
            .iter()
            .try_fold(0_u64, |total, length| total.checked_add(u64::from(*length)))
            .ok_or(Invalid("logical overflow"))?;
        result.index(0, end)?;
        Ok(result)
    }

    #[must_use]
    pub const fn count(&self) -> u32 {
        self.count
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        self.encoded.len()
    }

    /// Expand only one chunk's append lengths while creating a checkpoint.
    /// # Errors
    /// Rejects malformed counts/blocks before reserving unbounded memory.
    pub fn lengths(&self) -> Result<Vec<u32>, StreamChunkRecordError> {
        if u64::from(self.count) > MAX_ENTRIES {
            return Err(Invalid("mapping count"));
        }
        let mut output = Vec::with_capacity(self.count as usize);
        let mut cursor = 0;
        while output.len() < self.count as usize {
            let block = decode_block(&self.encoded, &mut cursor)?;
            if block.len() != (self.count as usize - output.len()).min(BLOCK_ENTRIES) {
                return Err(Invalid("noncanonical mapping block count"));
            }
            output.extend(block);
        }
        if cursor != self.encoded.len() {
            return Err(Invalid("mapping trailing bytes"));
        }
        Ok(output)
    }

    /// Validate the entire record once, creating samples without expanding all offsets.
    /// # Errors
    /// Rejects corrupt encoding, excessive work, discontinuous bounds or physical overflow.
    pub fn index(&self, start: u64, end: u64) -> Result<StreamMappingIndex, StreamChunkRecordError> {
        if u64::from(self.count) > MAX_ENTRIES || self.encoded.len() > MAX_STREAM_CHUNK_RECORD_BYTES {
            return Err(Invalid("mapping budget"));
        }
        let mut index = StreamMappingIndex {
            logical_end: start,
            ..Default::default()
        };
        let mut encoded = 0;
        let mut count = 0;
        while count < self.count {
            index.samples.push(SeekSample {
                logical: index.logical_end,
                physical: index.physical_end,
                encoded,
            });
            let lengths = decode_block(&self.encoded, &mut encoded)?;
            if lengths.len() != ((self.count - count) as usize).min(BLOCK_ENTRIES) {
                return Err(Invalid("noncanonical mapping block count"));
            }
            count = count
                .checked_add(u32::try_from(lengths.len()).map_err(|_| Invalid("mapping block count"))?)
                .ok_or(Invalid("mapping count overflow"))?;
            if count > self.count {
                return Err(Invalid("mapping count differs"));
            }
            for length in lengths {
                index.logical_end = index
                    .logical_end
                    .checked_add(u64::from(length))
                    .ok_or(Invalid("logical overflow"))?;
                index.physical_end = advance_physical(index.physical_end, length)?;
            }
        }
        if encoded != self.encoded.len() || index.logical_end != end {
            return Err(Invalid("mapping coverage or trailing bytes"));
        }
        Ok(index)
    }
}

impl StreamMappingIndex {
    #[must_use]
    pub const fn physical_end(&self) -> u64 {
        self.physical_end
    }

    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        self.samples.capacity() * std::mem::size_of::<SeekSample>()
    }

    /// Decode at most 256 complete-append lengths for an arbitrary logical seek.
    /// # Errors
    /// Rejects a different/corrupt mapping buffer or an out-of-range offset.
    pub fn locate(
        &self,
        mappings: &StreamMappings,
        offset: u64,
    ) -> Result<StreamMapping, StreamChunkRecordError> {
        if offset >= self.logical_end {
            return Err(Invalid("offset beyond mappings"));
        }
        let index = self
            .samples
            .partition_point(|sample| sample.logical <= offset)
            .checked_sub(1)
            .ok_or(Invalid("offset before mappings"))?;
        let mut sample = self.samples[index];
        for length in decode_block(&mappings.encoded, &mut sample.encoded)? {
            let end = sample
                .logical
                .checked_add(u64::from(length))
                .ok_or(Invalid("logical overflow"))?;
            if offset < end {
                return Ok(StreamMapping {
                    logical_start: sample.logical,
                    logical_end: end,
                    physical_start: sample.physical,
                });
            }
            sample.logical = end;
            sample.physical = advance_physical(sample.physical, length)?;
        }
        Err(Invalid("seek sample coverage"))
    }
}

fn advance_physical(physical: u64, length: u32) -> Result<u64, StreamChunkRecordError> {
    if length == 0 {
        return Err(Invalid("empty append"));
    }
    let length = FrameLayout::StreamGroupV2
        .physical_length(u64::from(length))
        .map_err(|_| Invalid("frame geometry"))?;
    physical
        .checked_add(length)
        .filter(|end| *end <= MAX_STREAM_CHUNK_BYTES)
        .ok_or(Invalid("physical chunk overflow"))
}

mod bounded_bytes {
    use super::MAX_STREAM_CHUNK_RECORD_BYTES;
    use serde::{
        de::{Error, Visitor},
        Deserializer, Serializer,
    };
    pub(super) fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(value)
    }
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(decoder: D) -> Result<Vec<u8>, D::Error> {
        struct Bounded;
        impl Visitor<'_> for Bounded {
            type Value = Vec<u8>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("bounded Stream mappings")
            }
            fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                if value.len() > MAX_STREAM_CHUNK_RECORD_BYTES {
                    return Err(E::custom("mapping bytes exceed chunk record budget"));
                }
                Ok(value.to_vec())
            }
        }
        decoder.deserialize_bytes(Bounded)
    }
}
