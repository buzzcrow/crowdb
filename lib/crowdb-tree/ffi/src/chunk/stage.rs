// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned protection of one independently allocated immutable pack window.

use crate::CtError;

mod codec;
pub(crate) use codec::{decode_stage_list, encode_stage_list};
pub const MAX_STAGE_PACKS: usize = 1024;
pub const MAX_STAGE_SCAN: u32 = 64;
pub const MAX_STAGE_BYTES: usize = 100 + 16 * MAX_STAGE_PACKS;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct StageId {
    pub owner_epoch: u64,
    pub operation_high: u64,
    pub operation_low: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum StageState {
    Reserved = 1,
    Writing = 2,
    Reloadable = 3,
    Referenced = 4,
    Retired = 5,
    Deleting = 6,
    Reclaimed = 7,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagePack {
    pub offset: u64,
    pub length: u32,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageRecord {
    pub tree_id: u64,
    pub id: StageId,
    pub revision: u64,
    /// Fixed before allocation is submitted, including retries after lost replies.
    pub chunk_high: u64,
    pub chunk_low: u64,
    pub capacity: u64,
    /// Concrete Tree page/index purpose, matching the ChunkId prefix.
    pub purpose: u32,
    pub state: StageState,
    pub manifest_generation: u64,
    pub packs: Vec<StagePack>,
}

impl StageRecord {
    pub fn validate(&self) -> Result<(), CtError> {
        if self.tree_id == 0
            || self.id.owner_epoch == 0
            || (self.id.operation_high == 0 && self.id.operation_low == 0)
            || self.revision == 0
            || self.capacity == 0
            || self.capacity > u64::from(u32::MAX)
            || !matches!(self.purpose, 2 | 3)
            || self.chunk_high >> 56 != u64::from(self.purpose)
            || self.packs.len() > MAX_STAGE_PACKS
            || (matches!(self.state, StageState::Reloadable | StageState::Referenced)
                && self.packs.is_empty())
            || (self.state == StageState::Referenced && self.manifest_generation == 0)
            || (matches!(
                self.state,
                StageState::Reserved | StageState::Writing | StageState::Reloadable
            ) && self.manifest_generation != 0)
        {
            return Err(CtError::InvalidArgument);
        }
        let mut end = 0;
        for pack in &self.packs {
            if pack.length == 0 || pack.length > 256 * 1024 * 1024 || pack.offset < end {
                return Err(CtError::InvalidArgument);
            }
            end = pack
                .offset
                .checked_add(u64::from(pack.length))
                .ok_or(CtError::InvalidArgument)?;
            if end > self.capacity {
                return Err(CtError::InvalidArgument);
            }
        }
        Ok(())
    }
}

pub fn validate_stage_transition(
    expected: Option<&StageRecord>,
    next: Option<&StageRecord>,
) -> Result<(), CtError> {
    if let Some(record) = expected {
        record.validate()?;
    }
    if let Some(record) = next {
        record.validate()?;
    }
    let (Some(old), Some(new)) = (expected, next) else {
        return match (expected, next) {
            (None, Some(record)) if record.revision == 1 && record.state == StageState::Reserved => Ok(()),
            (Some(record), None) if record.state == StageState::Reclaimed => Ok(()),
            _ => Err(CtError::InvalidArgument),
        };
    };
    if old.tree_id != new.tree_id
        || old.id != new.id
        || old.chunk_high != new.chunk_high
        || old.chunk_low != new.chunk_low
        || old.capacity != new.capacity
        || old.purpose != new.purpose
        || old.revision.checked_add(1) != Some(new.revision)
        || !new.packs.starts_with(&old.packs)
        || (new.packs != old.packs && !matches!(old.state, StageState::Reserved | StageState::Writing))
        || new.manifest_generation < old.manifest_generation
    {
        return Err(CtError::InvalidArgument);
    }
    use StageState::*;
    if matches!(
        (old.state, new.state),
        (Reserved, Writing | Retired)
            | (Writing, Writing | Reloadable | Retired)
            | (Reloadable, Referenced | Retired)
            | (Referenced, Referenced | Retired)
            | (Retired, Deleting)
            | (Deleting, Reclaimed)
    ) {
        Ok(())
    } else {
        Err(CtError::InvalidArgument)
    }
}
