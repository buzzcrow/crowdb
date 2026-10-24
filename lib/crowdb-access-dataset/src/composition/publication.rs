// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{state::CompositionState, CompositionDraft, CompositionError, DraftProgress};
use crate::{CommitAttempt, DatasetAuthority, DatasetIdentity, OperationId, StoredValue, SuccessfulCommit};

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
    pub(super) async fn reconcile_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<CompositionDraft, CompositionError> {
        let (old, state) = self.load_composition(identity, operation).await?;
        self.help_composition_edit(identity, &state).await?;
        if let DraftProgress::Committing { target, attempt } = state.draft.record.progress {
            let pending = self
                .stored_commit_attempt(identity, attempt)
                .await?
                .ok_or(CompositionError::Corrupt)?;
            if pending.target != target {
                return Err(CompositionError::Corrupt);
            }
            if let Some(receipt) = self.commit_status(identity, attempt).await? {
                self.finish_legacy_composition_publication(identity, &old, &state, &receipt)
                    .await?;
            } else {
                let mut next = state.clone();
                next.draft.record.progress = DraftProgress::Prepared { target };
                self.replace_composition(identity, &old, &state, &next).await?;
            }
            return Ok(self.load_composition(identity, operation).await?.1.draft);
        }
        Ok(state.draft)
    }
    pub(super) async fn finish_legacy_composition_publication(
        &self,
        identity: &DatasetIdentity,
        old: &StoredValue,
        state: &CompositionState,
        receipt: &SuccessfulCommit,
    ) -> Result<(), CompositionError> {
        let mut next = state.clone();
        next.edit = None;
        next.draft.record.progress = DraftProgress::Published {
            target: receipt.attempt.target,
            attempt: receipt.attempt.id,
        };
        self.replace_composition(identity, old, state, &next).await?;
        Ok(())
    }
}
