// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod edit;
mod prepare;
mod release;
pub use release::CompositionUploadCleanup;
mod state;
pub(super) use state::{staging_owner, UploadOwnership};

pub(super) fn publication_ready(
    state: &super::state::CompositionState,
) -> Result<bool, super::CompositionError> {
    state::status(state).map(|status| status.publication_ready)
}

use super::{CompositionError, DraftProgress};
use crate::{DatasetAuthority, DatasetIdentity, OperationId, SnapshotId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompositionUploadStatus {
    pub candidate: Option<SnapshotId>,
    pub publication_ready: bool,
    pub staged_uploads: u64,
    pub candidate_uploads: u64,
}
impl DatasetAuthority {
    /// Enrolls a draft in upload ownership before accepting uploaded field edits.
    /// Version 2 prevents older composition writers from ignoring ownership fences.
    /// # Errors
    /// Rejects frozen drafts, conflicting updates and corrupt or unavailable state.
    pub async fn manage_composition_uploads(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<CompositionUploadStatus, CompositionError> {
        let (old, state) = self.load_composition(identity, operation).await?;
        if state.uploads.is_none() {
            if !matches!(state.draft.record.progress, DraftProgress::Editing) {
                return Err(CompositionError::Frozen);
            }
            let mut next = state.clone();
            next.edit = None;
            next.draft.record.version = 2;
            next.uploads = Some(UploadOwnership::default());
            self.replace_composition(identity, &old, &state, &next).await?;
        }
        self.composition_upload_status(identity, operation).await
    }
    /// Reads durable payload readiness without advancing publication or cleanup.
    /// # Errors
    /// Rejects unknown or unmanaged compositions and malformed metadata.
    pub async fn composition_upload_status(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<CompositionUploadStatus, CompositionError> {
        let (_, state) = self.load_composition(identity, operation).await?;
        state::status(&state)
    }
}
