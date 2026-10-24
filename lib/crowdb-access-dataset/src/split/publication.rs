// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, state::DraftState, SplitHead, SplitLifecycleError, SplitRevisionRecord};
use crate::{
    composition::DraftProgress, CasOutcome, CommitAttempt, CommitGeneration, CommitTarget, DatasetAuthority,
    DatasetIdentity, OperationId, SplitRevisionId, StoredValue, SuccessfulCommit,
};

impl DatasetAuthority {
    /// Compatibility adapter for callers that prepared a complete result through a draft.
    /// # Errors
    /// Rejects mismatched targets, initial Split drafts and unavailable results.
    pub async fn publish_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        attempt: &CommitAttempt,
    ) -> Result<SuccessfulCommit, SplitLifecycleError> {
        attempt.validate()?;
        let (_, state) = self.load_split_draft(identity, operation).await?;
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
        self.commit_split_revision(identity, attempt, state.record.expected_revision, Some(operation))
            .await
    }

    /// Updates currentSplit from a complete immutable result and caller-owned preconditions.
    /// Success describes the current effect, without persisting request or response records.
    /// # Errors
    /// Rejects missing results, unavailable owners, stale preconditions and unknown storage outcomes.
    pub async fn publish_split_revision(
        &self,
        identity: &DatasetIdentity,
        attempt: &CommitAttempt,
        expected_revision: Option<SplitRevisionId>,
    ) -> Result<SuccessfulCommit, SplitLifecycleError> {
        self.commit_split_revision(identity, attempt, expected_revision, None)
            .await
    }

    async fn commit_split_revision(
        &self,
        identity: &DatasetIdentity,
        attempt: &CommitAttempt,
        expected_revision: Option<SplitRevisionId>,
        operation: Option<OperationId>,
    ) -> Result<SuccessfulCommit, SplitLifecycleError> {
        attempt.validate()?;
        let CommitTarget::SplitRevision { snapshot, revision } = attempt.target else {
            return Err(SplitLifecycleError::Conflict);
        };
        let stored = self
            .split_state_get(
                &key::revision(identity, snapshot, revision).map_err(crate::AuthorityError::from)?,
            )
            .await?
            .ok_or(SplitLifecycleError::NotFound)?;
        if stored.bytes == super::cleanup::SEALED {
            return Err(SplitLifecycleError::NotFound);
        }
        let candidate = SplitRevisionRecord::decode(&stored.bytes)?;
        if candidate.snapshot != snapshot
            || candidate.revision != revision
            || operation.is_some_and(|operation| candidate.operation != operation)
        {
            return Err(SplitLifecycleError::Corrupt);
        }
        let key = key::head(identity, snapshot).map_err(crate::AuthorityError::from)?;
        let current = self.split_state_get(&key).await?;
        // Closing Snapshot admission invalidates this exact head generation before reclaim.
        self.require_committed_snapshot(identity, snapshot).await?;
        let observed = self
            .resolve_split_reference(identity, current.as_ref(), snapshot)
            .await?;
        if observed.revision == Some(revision) && current.is_some() {
            return current_success(attempt, current.as_ref());
        }
        if observed.generation != attempt.expected_head || observed.revision != expected_revision {
            return Err(SplitLifecycleError::Conflict);
        }
        let bytes = SplitHead {
            version: 1,
            snapshot,
            revision: Some(revision),
            fence: OperationId::random(),
        }
        .encode()?;
        match self
            .store
            .compare_exchange(&key, current.as_ref().map(|v| v.bytes.as_slice()), &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(SuccessfulCommit {
                version: 1,
                attempt: attempt.clone(),
                committed_head: CommitGeneration::from_head(&bytes),
            }),
            CasOutcome::Conflict(observed) => {
                if super::head::reference(observed.as_ref(), snapshot)?.revision != Some(revision) {
                    return Err(SplitLifecycleError::Conflict);
                }
                current_success(attempt, observed.as_ref())
            }
        }
    }

    pub(super) async fn reset_legacy_split_publication(
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
    pub(super) async fn finish_legacy_split_publication(
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
        self.replace_split_draft(identity, old, state, &next).await
    }
}

fn current_success(
    attempt: &CommitAttempt,
    current: Option<&StoredValue>,
) -> Result<SuccessfulCommit, SplitLifecycleError> {
    let current = current.ok_or(SplitLifecycleError::Conflict)?;
    Ok(SuccessfulCommit {
        version: 1,
        attempt: attempt.clone(),
        committed_head: CommitGeneration::from_head(&current.bytes),
    })
}
