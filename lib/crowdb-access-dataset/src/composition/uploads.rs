// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod release;
pub use release::CompositionUploadCleanup;
pub(super) use release::UploadCursor;
mod state;
pub(super) use state::{staging_owner, UploadOwnership};

pub(super) fn publication_ready(
    state: &super::state::CompositionState,
) -> Result<bool, super::CompositionError> {
    state::status(state).map(|status| status.publication_ready)
}

use super::CompositionError;
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
