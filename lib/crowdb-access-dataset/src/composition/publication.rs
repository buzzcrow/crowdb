// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{state::CompositionState, CompositionDraft, CompositionError, DraftProgress};
use crate::{
    AuthorityError, CommitAttempt, DatasetAuthority, DatasetIdentity, OperationId, StoredValue,
    SuccessfulCommit,
};

impl DatasetAuthority {
    /// Publishes a complete immutable composition with one data-head CAS and durable receipt.
    /// # Errors
    /// Rejects stale heads, divergent attempts, abandoned drafts and incomplete preparation.
    pub async fn publish_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        attempt: &CommitAttempt,
    ) -> Result<SuccessfulCommit, CompositionError> {
        attempt.validate()?;
        let (mut old, mut state) = self.load_composition(identity, operation).await?;
        let (DraftProgress::Prepared { target }
        | DraftProgress::Committing { target, .. }
        | DraftProgress::Published { target, .. }) = state.draft.record.progress
        else {
            return Err(CompositionError::Frozen);
        };
        if target != attempt.target {
            return Err(CompositionError::Conflict);
        }
        if let Some(receipt) = self.commit_status(identity, attempt.id).await? {
            if receipt.attempt != *attempt {
                return Err(CompositionError::Conflict);
            }
            self.finish_composition_publication(identity, &old, &state, &receipt)
                .await?;
            return Ok(receipt);
        }
        if state.uploads.is_some() && !super::uploads::publication_ready(&state)? {
            return Err(CompositionError::Conflict);
        }
        self.persist_attempt(identity, attempt).await?;
        match state.draft.record.progress {
            DraftProgress::Prepared { .. } => {
                self.check_composition_source(identity, &state.draft).await?;
                let mut next = state.clone();
                next.edit = None;
                next.draft.record.progress = DraftProgress::Committing {
                    target,
                    attempt: attempt.id,
                };
                self.replace_composition(identity, &old, &state, &next).await?;
                (old, state) = self.load_composition(identity, operation).await?;
            }
            DraftProgress::Committing { attempt: pending, .. } if pending == attempt.id => {}
            DraftProgress::Published { .. } => {}
            _ => return Err(CompositionError::Conflict),
        }
        match self.publish_snapshot_candidate(identity, attempt).await {
            Ok(receipt) => {
                self.finish_composition_publication(identity, &old, &state, &receipt)
                    .await?;
                Ok(receipt)
            }
            Err(AuthorityError::HeadConflict) => {
                let mut next = state.clone();
                next.draft.record.progress = DraftProgress::Prepared { target };
                self.replace_composition(identity, &old, &state, &next).await?;
                Err(CompositionError::Conflict)
            }
            Err(error) => Err(error.into()),
        }
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
                self.finish_composition_publication(identity, &old, &state, &receipt)
                    .await?;
            } else if self.inspect_head(identity).await?.generation != pending.expected_head {
                if let Some(receipt) = self.commit_status(identity, attempt).await? {
                    self.finish_composition_publication(identity, &old, &state, &receipt)
                        .await?;
                } else {
                    let mut next = state.clone();
                    next.draft.record.progress = DraftProgress::Prepared { target };
                    self.replace_composition(identity, &old, &state, &next).await?;
                }
            }
            return Ok(self.load_composition(identity, operation).await?.1.draft);
        }
        Ok(state.draft)
    }
    pub(super) async fn finish_composition_publication(
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
