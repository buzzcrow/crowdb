// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{CompositionError, DraftProgress};
use crate::{CommitAttempt, DatasetAuthority, DatasetIdentity, OperationId, SuccessfulCommit};

impl DatasetAuthority {
    /// Compatibility adapter for publishing a prepared immutable result without process writes.
    /// # Errors
    /// Rejects stale heads, divergent attempts, abandoned drafts and incomplete preparation.
    pub async fn publish_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        attempt: &CommitAttempt,
    ) -> Result<SuccessfulCommit, CompositionError> {
        attempt.validate()?;
        let (_, state) = self.load_composition(identity, operation).await?;
        let (DraftProgress::Prepared { target }
        | DraftProgress::Committing { target, .. }
        | DraftProgress::Published { target, .. }) = state.draft.record.progress
        else {
            return Err(CompositionError::Frozen);
        };
        if target != attempt.target {
            return Err(CompositionError::Conflict);
        }
        if state.uploads.is_some() && !super::uploads::publication_ready(&state)? {
            return Err(CompositionError::Conflict);
        }
        Ok(self.publish_snapshot_candidate(identity, attempt).await?)
    }
}
