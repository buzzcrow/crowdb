// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Captured execution identity, independent of routing-map publication epochs.

use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// A process lifetime, including restarts under the same service instance ID.
///
/// Service admission uses owner plus a durable per-slot epoch. Restart and
/// reassignment must advance that epoch even when the owner ID is unchanged,
/// so incarnation is not an additional admission condition. This type remains
/// in the existing KV fence wire format, which still compares the full encoded
/// identity until its `ChunkDB` callers are migrated to submission-time checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct ChunkServiceIncarnation([u8; 16]);

impl ChunkServiceIncarnation {
    /// Generate a fresh process identity from the operating system RNG.
    ///
    /// # Errors
    /// Returns an error when entropy is unavailable or the identity is zero.
    pub fn generate() -> Result<Self, ChunkSlotAuthorityError> {
        let mut bytes = [0; 16];
        getrandom::getrandom(&mut bytes).map_err(|_| ChunkSlotAuthorityError)?;
        Self::try_from(bytes)
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

impl TryFrom<[u8; 16]> for ChunkServiceIncarnation {
    type Error = ChunkSlotAuthorityError;

    fn try_from(bytes: [u8; 16]) -> Result<Self, Self::Error> {
        if bytes == [0; 16] {
            return Err(ChunkSlotAuthorityError);
        }
        Ok(Self(bytes))
    }
}

impl From<ChunkServiceIncarnation> for [u8; 16] {
    fn from(value: ChunkServiceIncarnation) -> Self {
        value.bytes()
    }
}

/// Authority for one slot in its selected data group. Ordinary writes retain
/// this value; refreshing a map must never replace an in-flight operation's value.
///
/// Capture owner/epoch at request entry and recheck that slot before each client
/// KV submission. Another slot's publication must not invalidate this request.
/// Rejection stops the execution; rerouting starts a new execution with the same
/// request ID, even if this process remains owner under a newer epoch. Never
/// replace the captured epoch and continue using the old execution's state.
/// Submitted work may finish without handoff draining it; an unknown submitted
/// outcome still requires reconciliation before replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkSlotAuthority {
    instance_id: NonZeroU64,
    incarnation: ChunkServiceIncarnation,
    generation: NonZeroU64,
}

impl ChunkSlotAuthority {
    /// Construct a nonzero instance and per-slot generation.
    ///
    /// # Errors
    /// Returns an error for instance ID or generation zero.
    pub fn new(
        instance_id: u64,
        incarnation: ChunkServiceIncarnation,
        generation: u64,
    ) -> Result<Self, ChunkSlotAuthorityError> {
        Ok(Self {
            instance_id: NonZeroU64::new(instance_id).ok_or(ChunkSlotAuthorityError)?,
            incarnation,
            generation: NonZeroU64::new(generation).ok_or(ChunkSlotAuthorityError)?,
        })
    }

    #[must_use]
    pub const fn instance_id(self) -> u64 {
        self.instance_id.get()
    }

    #[must_use]
    pub const fn incarnation(self) -> ChunkServiceIncarnation {
        self.incarnation
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation.get()
    }

    /// Canonical KV comparison value: version, instance BE, incarnation, epoch BE.
    #[must_use]
    pub fn to_fence_value(self) -> [u8; 33] {
        let mut bytes = [0; 33];
        bytes[0] = 1;
        bytes[1..9].copy_from_slice(&self.instance_id().to_be_bytes());
        bytes[9..25].copy_from_slice(&self.incarnation.bytes());
        bytes[25..33].copy_from_slice(&self.generation().to_be_bytes());
        bytes
    }

    /// Decode a canonical KV comparison value without accepting future versions.
    ///
    /// # Errors
    /// Returns an error for an invalid length, version or zero identity field.
    pub fn from_fence_value(bytes: &[u8]) -> Result<Self, ChunkSlotAuthorityError> {
        let bytes: &[u8; 33] = bytes.try_into().map_err(|_| ChunkSlotAuthorityError)?;
        if bytes[0] != 1 {
            return Err(ChunkSlotAuthorityError);
        }
        let instance_id = u64::from_be_bytes(bytes[1..9].try_into().map_err(|_| ChunkSlotAuthorityError)?);
        let incarnation = ChunkServiceIncarnation::try_from(
            <[u8; 16]>::try_from(&bytes[9..25]).map_err(|_| ChunkSlotAuthorityError)?,
        )?;
        let generation = u64::from_be_bytes(bytes[25..33].try_into().map_err(|_| ChunkSlotAuthorityError)?);
        Self::new(instance_id, incarnation, generation)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid ChunkDB slot authority or unavailable process incarnation")]
pub struct ChunkSlotAuthorityError;
