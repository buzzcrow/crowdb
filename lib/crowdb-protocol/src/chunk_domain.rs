// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Independent maintenance authority for system and user-data chunks.

use crate::common::ChunkId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ChunkDomain {
    System = 1,
    UserData = 2,
}

impl ChunkDomain {
    /// Unsupported and retired types have no maintenance authority.
    #[must_use]
    pub fn for_chunk(id: &ChunkId) -> Option<Self> {
        match id.high.to_be_bytes()[0] {
            1..=3 => Some(Self::System),
            4..=6 => Some(Self::UserData),
            _ => None,
        }
    }
}
