// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persistent chunk task keys and runnable/lease indexes.

use crate::chunk_domain::ChunkDomain;
use crate::chunk_slot::ChunkSlot;
use crate::common::ChunkId;

use super::encoding::{
    check_exact, decode_chunk_id, decode_header, decode_u16, decode_u64, decode_u8, encode_chunk_id,
    encode_header, encode_u16, encode_u64, encode_u8, BinaryKey, KeyError,
};

/// Canonical task record: magic, tag, partition ID, kind, task ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkTaskKey {
    pub partition_id: ChunkId,
    pub kind: u16,
    pub task_id: ChunkId,
}

impl BinaryKey for ChunkTaskKey {
    const TYPE_TAG: u16 = 0x000D;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_chunk_id(out, &self.partition_id);
        encode_u16(out, self.kind);
        encode_chunk_id(out, &self.task_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (partition_id, offset) = decode_chunk_id(fields, 0)?;
        let (kind, offset) = decode_u16(fields, offset)?;
        let (task_id, offset) = decode_chunk_id(fields, offset)?;
        check_exact(fields, offset)?;
        Ok(Self {
            partition_id,
            kind,
            task_id,
        })
    }
}

impl ChunkTaskKey {
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        prefix(Self::TYPE_TAG)
    }

    #[must_use]
    pub fn prefix_for_partition(partition_id: &ChunkId) -> Vec<u8> {
        let mut bytes = prefix(Self::TYPE_TAG);
        encode_chunk_id(&mut bytes, partition_id);
        bytes
    }
}

/// Runnable task index: domain, slot, inverse priority, eligibility, identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadyChunkTaskKey {
    pub priority_inverse: u8,
    pub eligible_at_ms: u64,
    pub partition_id: ChunkId,
    pub kind: u16,
    pub task_id: ChunkId,
}

impl BinaryKey for ReadyChunkTaskKey {
    const TYPE_TAG: u16 = 0x0013;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_scope(out, &self.partition_id);
        encode_u8(out, self.priority_inverse);
        encode_u64(out, self.eligible_at_ms);
        encode_chunk_id(out, &self.partition_id);
        encode_u16(out, self.kind);
        encode_chunk_id(out, &self.task_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (priority_inverse, offset) = decode_u8(fields, 3)?;
        let (eligible_at_ms, offset) = decode_u64(fields, offset)?;
        let (partition_id, offset) = decode_chunk_id(fields, offset)?;
        let (kind, offset) = decode_u16(fields, offset)?;
        let (task_id, offset) = decode_chunk_id(fields, offset)?;
        check_exact(fields, offset)?;
        check_scope(fields, &partition_id)?;
        Ok(Self {
            priority_inverse,
            eligible_at_ms,
            partition_id,
            kind,
            task_id,
        })
    }
}

impl ReadyChunkTaskKey {
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        prefix(Self::TYPE_TAG)
    }
}

/// Liveness index: domain, slot, expiry, identity. Independent of generic
/// work priority so finalization never scans unrelated task kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FinalizeChunkTaskKey {
    pub expires_at_ms: u64,
    pub partition_id: ChunkId,
    pub task_id: ChunkId,
}

impl BinaryKey for FinalizeChunkTaskKey {
    const TYPE_TAG: u16 = 0x0015;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_scope(out, &self.partition_id);
        encode_u64(out, self.expires_at_ms);
        encode_chunk_id(out, &self.partition_id);
        encode_chunk_id(out, &self.task_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (expires_at_ms, offset) = decode_u64(fields, 3)?;
        let (partition_id, offset) = decode_chunk_id(fields, offset)?;
        let (task_id, offset) = decode_chunk_id(fields, offset)?;
        check_exact(fields, offset)?;
        check_scope(fields, &partition_id)?;
        Ok(Self {
            expires_at_ms,
            partition_id,
            task_id,
        })
    }
}

impl FinalizeChunkTaskKey {
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        prefix(Self::TYPE_TAG)
    }
}

/// Claimed task index: domain, slot, lease deadline, identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeasedChunkTaskKey {
    pub lease_deadline_ms: u64,
    pub partition_id: ChunkId,
    pub kind: u16,
    pub task_id: ChunkId,
}

impl BinaryKey for LeasedChunkTaskKey {
    const TYPE_TAG: u16 = 0x0014;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_scope(out, &self.partition_id);
        encode_u64(out, self.lease_deadline_ms);
        encode_chunk_id(out, &self.partition_id);
        encode_u16(out, self.kind);
        encode_chunk_id(out, &self.task_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (lease_deadline_ms, offset) = decode_u64(fields, 3)?;
        let (partition_id, offset) = decode_chunk_id(fields, offset)?;
        let (kind, offset) = decode_u16(fields, offset)?;
        let (task_id, offset) = decode_chunk_id(fields, offset)?;
        check_exact(fields, offset)?;
        check_scope(fields, &partition_id)?;
        Ok(Self {
            lease_deadline_ms,
            partition_id,
            kind,
            task_id,
        })
    }
}

impl LeasedChunkTaskKey {
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        prefix(Self::TYPE_TAG)
    }
}

fn prefix(type_tag: u16) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(3);
    encode_header(&mut bytes, type_tag);
    bytes
}

fn encode_scope(out: &mut Vec<u8>, id: &ChunkId) {
    encode_u8(out, ChunkDomain::for_chunk(id).map_or(0, |domain| domain as u8));
    encode_u16(out, ChunkSlot::for_chunk(id).value());
}

fn check_scope(fields: &[u8], id: &ChunkId) -> Result<(), KeyError> {
    let mut expected = Vec::with_capacity(3);
    encode_scope(&mut expected, id);
    if fields.get(..3) != Some(expected.as_slice()) {
        return Err(KeyError::BadTag);
    }
    Ok(())
}
