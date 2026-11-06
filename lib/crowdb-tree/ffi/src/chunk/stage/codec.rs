// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{StageId, StagePack, StageRecord, StageState, MAX_STAGE_BYTES, MAX_STAGE_PACKS};
use crate::{crc32c, CtError};

const MAGIC: u32 = 0x4753_5443;

impl StageRecord {
    pub fn encode(&self) -> Result<Vec<u8>, CtError> {
        self.validate()?;
        let mut out = Vec::with_capacity(100 + 16 * self.packs.len());
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&1_u32.to_le_bytes());
        for value in [
            self.tree_id,
            self.id.owner_epoch,
            self.id.operation_high,
            self.id.operation_low,
            self.revision,
            self.chunk_high,
            self.chunk_low,
            self.capacity,
            self.manifest_generation,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        for value in [self.purpose, self.state as u32, self.packs.len() as u32, 0] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        for pack in &self.packs {
            out.extend_from_slice(&pack.offset.to_le_bytes());
            out.extend_from_slice(&pack.length.to_le_bytes());
            out.extend_from_slice(&pack.checksum.to_le_bytes());
        }
        let crc = crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CtError> {
        if !(100..=MAX_STAGE_BYTES).contains(&bytes.len()) || u32_at(bytes, 0) != MAGIC {
            return Err(CtError::Corruption);
        }
        if u32_at(bytes, 4) != 1 {
            return Err(CtError::NotSupported);
        }
        let count = u32_at(bytes, 88) as usize;
        if count > MAX_STAGE_PACKS
            || bytes.len() != 100 + 16 * count
            || u32_at(bytes, 92) != 0
            || crc32c(&bytes[..bytes.len() - 4]) != u32_at(bytes, bytes.len() - 4)
        {
            return Err(CtError::Corruption);
        }
        let state = match u32_at(bytes, 84) {
            1 => StageState::Reserved,
            2 => StageState::Writing,
            3 => StageState::Reloadable,
            4 => StageState::Referenced,
            5 => StageState::Retired,
            6 => StageState::Deleting,
            7 => StageState::Reclaimed,
            _ => return Err(CtError::Corruption),
        };
        let out = Self {
            tree_id: u64_at(bytes, 8),
            id: StageId {
                owner_epoch: u64_at(bytes, 16),
                operation_high: u64_at(bytes, 24),
                operation_low: u64_at(bytes, 32),
            },
            revision: u64_at(bytes, 40),
            chunk_high: u64_at(bytes, 48),
            chunk_low: u64_at(bytes, 56),
            capacity: u64_at(bytes, 64),
            manifest_generation: u64_at(bytes, 72),
            purpose: u32_at(bytes, 80),
            state,
            packs: (0..count)
                .map(|i| StagePack {
                    offset: u64_at(bytes, 96 + 16 * i),
                    length: u32_at(bytes, 104 + 16 * i),
                    checksum: u32_at(bytes, 108 + 16 * i),
                })
                .collect(),
        };
        out.validate().map_err(|_| CtError::Corruption)?;
        Ok(out)
    }
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("validated stage wire bounds"))
}
fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("validated stage wire bounds"))
}

pub(crate) fn encode_stage_list(records: &[StageRecord]) -> Result<Vec<u8>, CtError> {
    if records.len() > super::MAX_STAGE_SCAN as usize {
        return Err(CtError::InvalidArgument);
    }
    let mut out = (records.len() as u32).to_le_bytes().to_vec();
    for record in records {
        let bytes = record.encode()?;
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

pub(crate) fn decode_stage_list(bytes: &[u8]) -> Result<Vec<StageRecord>, CtError> {
    if bytes.len() < 4 || bytes.len() > 4 + super::MAX_STAGE_SCAN as usize * (4 + MAX_STAGE_BYTES) {
        return Err(CtError::Corruption);
    }
    let count = u32_at(bytes, 0) as usize;
    if count > super::MAX_STAGE_SCAN as usize {
        return Err(CtError::Corruption);
    }
    let mut records = Vec::with_capacity(count);
    let mut at = 4;
    for _ in 0..count {
        if bytes.len() - at < 4 {
            return Err(CtError::Corruption);
        }
        let length = u32_at(bytes, at) as usize;
        at += 4;
        if length > bytes.len() - at {
            return Err(CtError::Corruption);
        }
        records.push(StageRecord::decode(&bytes[at..at + length])?);
        at += length;
    }
    if at != bytes.len() {
        return Err(CtError::Corruption);
    }
    Ok(records)
}
