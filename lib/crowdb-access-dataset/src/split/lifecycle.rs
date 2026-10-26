// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{SplitDraftRecord, SplitError};
use crate::{
    composition::DraftProgress, AuthorityError, DatasetAuthority, DatasetIdentity, OperationId, SnapshotId,
    StoreError,
};

#[derive(Debug, thiserror::Error)]
pub enum SplitLifecycleError {
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Split(#[from] SplitError),
    #[error(transparent)]
    Inspection(#[from] crate::inspection::InspectionError),
    #[error("Split operation was not found")]
    NotFound,
    #[error("Split draft or publication precondition changed")]
    Conflict,
    #[error("Split head precondition changed")]
    HeadConflict {
        snapshot: SnapshotId,
        observed: Box<super::SplitReference>,
    },
    #[error("Split operation metadata is corrupt")]
    Corrupt,
    #[error("Split operation exceeded its work limit")]
    Limit,
    #[error("Split draft has no complete current evaluation")]
    NotEvaluated,
    #[error("Split overlap or coverage policy is violated")]
    PolicyViolation,
    #[error("Split draft cannot be edited in its current state")]
    Frozen,
}
impl DatasetAuthority {
    /// Reads the stored draft without recovering a historical publication request.
    /// # Errors
    /// Returns unknown operations, corrupt records or storage failures.
    pub async fn split_draft_status(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<SplitDraftRecord, SplitLifecycleError> {
        let (_, state) = self.load_split_draft(identity, operation).await?;
        let mut record = state.record;
        if let DraftProgress::Committing { target, .. } = record.progress {
            record.progress = DraftProgress::Prepared { target };
        }
        Ok(record)
    }

    /// Freezes a legacy unpublished draft as abandoned for bounded scratch cleanup.
    /// # Errors
    /// Rejects stale revisions or an already published draft.
    pub async fn abandon_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        expected_revision: u64,
    ) -> Result<SplitDraftRecord, SplitLifecycleError> {
        let (old, state) = self.load_split_draft(identity, operation).await?;
        if state.initial {
            return Err(SplitLifecycleError::Frozen);
        }
        if state.record.revision != expected_revision {
            return Err(SplitLifecycleError::Conflict);
        }
        if matches!(state.record.progress, DraftProgress::Abandoned) {
            return Ok(state.record);
        }
        if matches!(state.record.progress, DraftProgress::Published { .. }) {
            return Err(SplitLifecycleError::Frozen);
        }
        let mut next = state.clone();
        next.record.progress = DraftProgress::Abandoned;
        next.edit = None;
        self.replace_split_draft(identity, &old, &state, &next).await?;

        Ok(next.record)
    }
}
