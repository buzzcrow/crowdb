// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{state::CompositionState, CompositionError},
    CompositionUploadStatus,
};
use crate::{AuthorityError, OperationId, SnapshotId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::composition) struct UploadOwnership {
    pub staged: Vec<OperationId>,
    pub staged_retired: usize,
    pub candidates: Vec<CandidateOwnership>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::composition) struct CandidateOwnership {
    pub input_revision: u64,
    pub snapshot: SnapshotId,
    pub owner: OperationId,
    pub uploads: Vec<OperationId>,
    pub after: Option<Vec<u8>>,
    pub complete: bool,
    pub retired: usize,
    pub keep_committed: bool,
    pub fenced: bool,
}
impl UploadOwnership {
    pub(in crate::composition) fn validate(&self) -> Result<(), CompositionError> {
        if self.staged_retired > self.staged.len() || self.candidates.len() > 4097 {
            return Err(CompositionError::Corrupt);
        }
        ids(&self.staged)?;
        let mut revisions = std::collections::BTreeSet::new();
        for candidate in &self.candidates {
            if candidate.input_revision == 0
                || !revisions.insert(candidate.input_revision)
                || candidate.retired > candidate.uploads.len()
                || (candidate.keep_committed && (candidate.retired != 0 || candidate.fenced))
                || (candidate.complete && candidate.after.is_some())
            {
                return Err(CompositionError::Corrupt);
            }
            SnapshotId::from_bytes(candidate.snapshot.as_bytes()).map_err(AuthorityError::from)?;
            OperationId::from_bytes(candidate.owner.as_bytes()).map_err(AuthorityError::from)?;
            ids(&candidate.uploads)?;
        }
        Ok(())
    }
}
fn ids(ids: &[OperationId]) -> Result<(), CompositionError> {
    if ids.len() > 4096 || ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CompositionError::Limit);
    }
    for id in ids {
        OperationId::from_bytes(id.as_bytes()).map_err(AuthorityError::from)?;
    }
    Ok(())
}
pub(in crate::composition) fn staging_owner(operation: OperationId) -> Result<OperationId, CompositionError> {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-composition-staging-v1\0");
    hash.update(operation.as_bytes());
    OperationId::from_bytes(&hash.finalize()[..16])
        .map_err(AuthorityError::from)
        .map_err(Into::into)
}
pub(in crate::composition) fn status(
    state: &CompositionState,
) -> Result<CompositionUploadStatus, CompositionError> {
    let uploads = state.uploads.as_ref().ok_or(CompositionError::Conflict)?;
    let candidate = state.draft.preparation.as_ref().map(|p| p.candidate);
    let ownership = uploads
        .candidates
        .iter()
        .find(|p| Some(p.snapshot) == candidate && p.input_revision == state.draft.record.revision);
    Ok(CompositionUploadStatus {
        candidate,
        publication_ready: matches!(
            state.draft.record.progress,
            super::super::DraftProgress::Prepared { .. }
                | super::super::DraftProgress::Committing { .. }
                | super::super::DraftProgress::Published { .. }
        ) && ownership.is_some_and(|p| p.complete && p.retired == 0),
        staged_uploads: uploads.staged.len() as u64,
        candidate_uploads: ownership.map_or(0, |p| p.uploads.len() as u64),
    })
}
