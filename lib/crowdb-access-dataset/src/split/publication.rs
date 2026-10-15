// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    head::CommittedSplitHead, key, state::DraftState, SplitHead, SplitLifecycleError, SplitRevisionRecord,
};
use crate::{
    composition::DraftProgress, CasOutcome, CommitAttempt, CommitGeneration, CommitTarget, DatasetAuthority,
    DatasetIdentity, OperationId, StoredValue, SuccessfulCommit,
};

impl DatasetAuthority {
    /// Publishes one prepared complete Split set without changing the Dataset data head.
    /// # Errors
    /// Rejects divergent attempts, stale heads, abandoned drafts and unavailable owners.
    pub async fn publish_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        attempt: &CommitAttempt,
    ) -> Result<SuccessfulCommit, SplitLifecycleError> {
        attempt.validate()?;
        let (mut old, mut state) = self.load_split_draft(identity, operation).await?;
        if state.initial {
            return Err(SplitLifecycleError::Frozen);
        }
        let (DraftProgress::Prepared { target }
        | DraftProgress::Committing { target, .. }
        | DraftProgress::Published { target, .. }) = state.record.progress
        else {
            return Err(SplitLifecycleError::Frozen);
        };
        if target != attempt.target {
            return Err(SplitLifecycleError::Conflict);
        }
        let CommitTarget::SplitRevision { snapshot, revision } = target else {
            return Err(SplitLifecycleError::Corrupt);
        };
        let stored = self
            .split_state_get(
                &key::revision(identity, snapshot, revision).map_err(crate::AuthorityError::from)?,
            )
            .await?
            .ok_or(SplitLifecycleError::NotFound)?;
        let candidate = SplitRevisionRecord::decode(&stored.bytes)?;
        if candidate.snapshot != snapshot
            || candidate.revision != revision
            || candidate.operation != operation
        {
            return Err(SplitLifecycleError::Corrupt);
        }
        if let Some(receipt) = self.commit_status(identity, attempt.id).await? {
            if receipt.attempt != *attempt {
                return Err(SplitLifecycleError::Conflict);
            }
            self.finish_split_publication(identity, &receipt, &old, &state)
                .await?;
            return Ok(receipt);
        }
        self.persist_attempt(identity, attempt).await?;
        if let Some(receipt) = self.split_revision_receipt(identity, snapshot, revision).await? {
            self.finish_split_publication(identity, &receipt, &old, &state)
                .await?;
            return Ok(receipt);
        }
        self.renew_split_draft_lease(identity, &state).await?;
        match state.record.progress {
            DraftProgress::Prepared { .. } => {
                let mut next = state.clone();
                next.record.progress = DraftProgress::Committing {
                    target,
                    attempt: attempt.id,
                };
                next.edit = None;
                self.replace_split_draft(identity, &old, &state, &next).await?;
                (old, state) = self.load_split_draft(identity, operation).await?;
            }
            DraftProgress::Committing { attempt: pending, .. } if pending == attempt.id => {}
            _ => return Err(SplitLifecycleError::Conflict),
        }
        self.commit_split_head(identity, attempt, &old, &state).await
    }

    async fn commit_split_head(
        &self,
        identity: &DatasetIdentity,
        attempt: &CommitAttempt,
        old: &StoredValue,
        state: &DraftState,
    ) -> Result<SuccessfulCommit, SplitLifecycleError> {
        let CommitTarget::SplitRevision { snapshot, revision } = attempt.target else {
            return Err(SplitLifecycleError::Corrupt);
        };
        let key = key::head(identity, snapshot).map_err(crate::AuthorityError::from)?;
        let current = self.split_state_get(&key).await?;
        if let Some(value) = &current {
            self.help_split_head(identity, value).await?;
        }
        if let Some(receipt) = self.split_revision_receipt(identity, snapshot, revision).await? {
            self.finish_split_publication(identity, &receipt, old, state)
                .await?;
            return Ok(receipt);
        }
        let observed = self
            .resolve_split_reference(identity, current.as_ref(), snapshot)
            .await?;
        if observed.generation != attempt.expected_head || observed.revision != state.record.expected_revision
        {
            self.reset_split_publication(identity, old, state).await?;
            return Err(SplitLifecycleError::Conflict);
        }
        // Reclaim must invalidate this Split head after closing owner admission.
        self.require_committed_snapshot(identity, snapshot).await?;
        let bytes = CommittedSplitHead::encode(
            SplitHead {
                version: 1,
                snapshot,
                revision: Some(revision),
                fence: OperationId::random(),
            },
            attempt.clone(),
        )?;
        let result = self
            .store
            .compare_exchange(&key, current.as_ref().map(|v| v.bytes.as_slice()), &bytes)
            .await?;
        let receipt = match result {
            CasOutcome::Applied(_) => SuccessfulCommit {
                version: 1,
                attempt: attempt.clone(),
                committed_head: CommitGeneration::from_head(&bytes),
            },
            CasOutcome::Conflict(_) => {
                let Some(receipt) = self.split_revision_receipt(identity, snapshot, revision).await? else {
                    self.reset_split_publication(identity, old, state).await?;
                    return Err(SplitLifecycleError::Conflict);
                };
                receipt
            }
        };
        self.persist_commit_receipt(identity, &receipt).await?;
        self.finish_split_publication(identity, &receipt, old, state)
            .await?;
        Ok(receipt)
    }
    pub(super) async fn reset_split_publication(
        &self,
        identity: &DatasetIdentity,
        old: &StoredValue,
        state: &DraftState,
    ) -> Result<(), SplitLifecycleError> {
        let DraftProgress::Committing { target, .. } = state.record.progress else {
            return Err(SplitLifecycleError::Corrupt);
        };
        let mut next = state.clone();
        next.record.progress = DraftProgress::Prepared { target };
        self.replace_split_draft(identity, old, state, &next).await
    }
    pub(super) async fn finish_split_publication(
        &self,
        identity: &DatasetIdentity,
        receipt: &SuccessfulCommit,
        old: &StoredValue,
        state: &DraftState,
    ) -> Result<(), SplitLifecycleError> {
        let mut next = state.clone();
        next.edit = None;
        next.record.progress = DraftProgress::Published {
            target: receipt.attempt.target,
            attempt: receipt.attempt.id,
        };
        self.replace_split_draft(identity, old, state, &next).await?;
        self.release_split_draft_leases(identity, state).await?;
        Ok(())
    }
}
