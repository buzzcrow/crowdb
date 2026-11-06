// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded durable reference preparation before a root becomes visible.

use super::stage::StageId;
use crate::{crc32c_update, CtError};

// Final release also writes the candidate, window, manifest and authority.
pub const MAX_REFERENCE_WINDOW: usize = 60;
pub const MAX_CANDIDATE_REFERENCES: u64 = 16 * 1024 * 1024;
pub const MAX_CANDIDATE_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestCandidate {
    pub id: StageId,
    pub expected_generation: u64,
    pub reference_count: u64,
    pub reference_checksum: u32,
    /// Immutable root descriptor; reference windows are persisted separately.
    pub manifest: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum CandidateState {
    Preparing = 1,
    Ready = 2,
    Committed = 3,
    Aborted = 4,
    Reclaimed = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CandidateStatus {
    pub id: StageId,
    pub state: CandidateState,
    pub generation: u64,
    pub reference_count: u64,
    pub prepared_references: u64,
    pub prepared_windows: u64,
    pub released_windows: u64,
}

pub fn validate_operation(id: StageId) -> Result<(), CtError> {
    if id.owner_epoch == 0 || (id.operation_high == 0 && id.operation_low == 0) {
        return Err(CtError::InvalidArgument);
    }
    Ok(())
}

/// Update in the same sorted order used by prepare_manifest_references.
#[must_use]
pub fn reference_checksum(mut checksum: u32, references: &[StageId]) -> u32 {
    for id in references {
        for value in [id.owner_epoch, id.operation_high, id.operation_low] {
            checksum = crc32c_update(checksum, &value.to_le_bytes());
        }
    }
    checksum
}

impl ManifestCandidate {
    pub fn validate(&self) -> Result<(), CtError> {
        validate_operation(self.id)?;
        if self.expected_generation == u64::MAX
            || self.reference_count > MAX_CANDIDATE_REFERENCES
            || self.manifest.is_empty()
            || self.manifest.len() > MAX_CANDIDATE_MANIFEST_BYTES
            || (self.reference_count == 0 && self.reference_checksum != 0)
        {
            return Err(CtError::InvalidArgument);
        }
        Ok(())
    }
}

/// Implementations serialize these operations with root publication and GC.
/// No method may infer an unknown outcome from a timeout or create a new ID.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PinPruneProgress {
    pub examined: u32,
    pub released: u32,
    pub after: Option<StageId>,
    pub more: bool,
}

pub trait StageProtectionCatalog: Send + Sync {
    /// Local fixed-process lease check before submission and after each read window.
    /// # Errors
    /// Returns unavailable for an expired owner or unsupported for legacy catalogs.
    fn check_read_owner(&self) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    /// Called after closing native write admission and draining every accepted backend operation.
    /// A lost reply retries this same writer identity; it never releases live reader pins.
    /// # Errors
    /// Returns an authority, metadata availability, or unsupported error.
    fn writer_quiesced(&self, _tree_id: u64, _owner_epoch: u64) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    /// Resolves an earlier owner's uncommitted candidate after actual root and WAL recovery.
    /// # Errors
    /// Returns an authority, incomplete recovery, or unsupported error.
    fn recover_manifest_candidate(&self, _tree_id: u64, _id: StageId) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    /// Remove only pins whose process exclusion has been authoritatively confirmed.
    /// # Errors
    /// Rejects incomplete recovery, unavailable metadata, or unsupported catalogs.
    fn prune_stage_pins(
        &self,
        _tree_id: u64,
        _stage: StageId,
        _after: Option<StageId>,
        _limit: u32,
    ) -> Result<PinPruneProgress, CtError> {
        Err(CtError::NotSupported)
    }

    fn load_manifest_candidate(&self, tree_id: u64, id: StageId) -> Result<Option<CandidateStatus>, CtError>;
    fn list_manifest_candidates(
        &self,
        tree_id: u64,
        after: Option<StageId>,
        limit: u32,
    ) -> Result<Vec<CandidateStatus>, CtError>;
    fn begin_manifest_candidate(&self, tree_id: u64, candidate: &ManifestCandidate) -> Result<(), CtError>;
    fn prepare_manifest_references(
        &self,
        tree_id: u64,
        id: StageId,
        window: u64,
        references: &[StageId],
    ) -> Result<(), CtError>;
    fn commit_manifest_candidate(&self, tree_id: u64, id: StageId) -> Result<(), CtError>;
    fn abort_manifest_candidate(&self, tree_id: u64, id: StageId) -> Result<(), CtError>;
    /// Release one previously prepared window after abort or root reclamation.
    fn release_manifest_references(&self, tree_id: u64, id: StageId, window: u64) -> Result<(), CtError>;
    fn pin_stage(&self, tree_id: u64, stage: StageId, pin: StageId) -> Result<(), CtError>;
    /// A fenced owner may release its own pin; it may never acquire a new one.
    fn unpin_stage(&self, tree_id: u64, stage: StageId, pin: StageId) -> Result<(), CtError>;
}
