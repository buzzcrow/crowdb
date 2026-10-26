// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{invalid, Result, StoreError};
use bincode::Options;
use crowdb_protocol::{chunkdb::rpc::Chunk, common::ChunkId};
use serde::{Deserialize, Serialize};

const MAX_RANGES: usize = 4096;
const MAX_RECORD_BYTES: u64 = 128 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetiredByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChunkRangeRetirement {
    version: u16,
    chunk_id: Option<ChunkId>,
    writer_epoch: u64,
    create_ts_ms: u64,
    sealed_ts_ms: u64,
    sealed_bytes: u64,
    pub revision: u64,
    ranges: Vec<RetiredByteRange>,
}

impl ChunkRangeRetirement {
    #[must_use]
    pub fn ranges(&self) -> &[RetiredByteRange] {
        &self.ranges
    }

    pub(super) fn new(chunk: &Chunk) -> Self {
        Self {
            version: 2,
            chunk_id: chunk.id,
            writer_epoch: chunk.writer_epoch,
            create_ts_ms: chunk.create_ts_ms,
            sealed_ts_ms: chunk.sealed_ts_ms,
            sealed_bytes: sealed_bytes(chunk),
            revision: chunk.modify_ts,
            ranges: Vec::new(),
        }
    }
    pub(super) fn validate(&self, chunk: &Chunk) -> Result<()> {
        self.validate_shape()?;
        if self.chunk_id != chunk.id
            || self.writer_epoch != chunk.writer_epoch
            || self.create_ts_ms != chunk.create_ts_ms
            || self.sealed_ts_ms != chunk.sealed_ts_ms
            || self.sealed_bytes != sealed_bytes(chunk)
        {
            return Err(invalid("retirement resource identity corruption"));
        }
        if self.revision > chunk.modify_ts {
            return Err(StoreError::Conflict);
        }
        Ok(())
    }
    fn validate_shape(&self) -> Result<()> {
        if !matches!(self.version, 1 | 2)
            || self.chunk_id.is_none()
            || (self.version == 1 && self.writer_epoch == 0)
            || self.sealed_bytes == 0
            || self.revision == 0
            || self.ranges.len() > MAX_RANGES
            || self
                .ranges
                .iter()
                .any(|r| r.start >= r.end || r.end > self.sealed_bytes)
            || self.ranges.windows(2).any(|pair| pair[0].end >= pair[1].start)
        {
            return Err(invalid("retirement resource format or interval corruption"));
        }
        Ok(())
    }
    pub(super) fn contains(&self, range: RetiredByteRange) -> bool {
        self.ranges
            .iter()
            .any(|r| r.start <= range.start && r.end >= range.end)
    }
    pub(super) fn insert(&mut self, range: RetiredByteRange) -> Result<()> {
        let mut ranges = self.ranges.clone();
        ranges.push(range);
        ranges.sort_unstable_by_key(|r| r.start);
        let mut merged: Vec<RetiredByteRange> = Vec::with_capacity(ranges.len());
        for range in ranges {
            if let Some(previous) = merged.last_mut().filter(|previous| previous.end >= range.start) {
                previous.end = previous.end.max(range.end);
            } else {
                merged.push(range);
            }
        }
        if merged.len() > MAX_RANGES {
            return Err(invalid("retirement range budget exceeded"));
        }
        self.ranges = merged;
        Ok(())
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|error| StoreError::Serde(error.to_string()))
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let record: Self = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(MAX_RECORD_BYTES)
            .reject_trailing_bytes()
            .deserialize(bytes)
            .map_err(|error| StoreError::Serde(error.to_string()))?;
        record.validate_shape()?;
        Ok(record)
    }
}

fn sealed_bytes(chunk: &Chunk) -> u64 {
    if chunk.writer_epoch == 0 {
        u64::from(chunk.sealed_length) * 1024
    } else {
        chunk.acknowledged_cursor
    }
}
