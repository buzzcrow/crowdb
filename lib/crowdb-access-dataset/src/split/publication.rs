// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, SplitHead, SplitHeadUpdate, SplitLifecycleError, SplitReference, SplitRevisionRecord};
use crate::{
    CasOutcome, CommitAttempt, CommitGeneration, CommitTarget, DatasetAuthority, DatasetIdentity,
    OperationId, SnapshotId, SplitRevisionId, StoredValue, SuccessfulCommit,
};

impl DatasetAuthority {
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
        let (snapshot, update) = attempt_update(attempt, expected_revision)?;
        confirmed_commit(
            attempt,
            &self.update_split_head(identity, snapshot, &update).await?,
        )
    }

    /// Updates the resource pointer without a request identity or editing state.
    /// # Errors
    /// Rejects invalid or missing results, deletion admission, stale conditions and unknown outcomes.
    pub async fn update_split_head(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        update: &SplitHeadUpdate,
    ) -> Result<SplitReference, SplitLifecycleError> {
        self.commit_split_revision(identity, snapshot, update).await
    }

    async fn commit_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        update: &SplitHeadUpdate,
    ) -> Result<SplitReference, SplitLifecycleError> {
        let revision = update.revision;
        CommitTarget::SplitRevision { snapshot, revision }.validate()?;
        if let Some(expected) = update.expected_revision {
            SplitRevisionId::from_bytes(expected.as_bytes()).map_err(crate::AuthorityError::from)?;
        }
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
        if candidate.snapshot != snapshot || candidate.revision != revision {
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
            return current_reference(revision, current.as_ref());
        }
        if observed.generation != update.expected_head || observed.revision != update.expected_revision {
            return Err(SplitLifecycleError::HeadConflict {
                snapshot,
                observed: Box::new(observed),
            });
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
            CasOutcome::Applied(_) => Ok(SplitReference {
                revision: Some(revision),
                generation: Some(CommitGeneration::from_head(&bytes)),
            }),
            CasOutcome::Conflict(observed) => {
                let head = super::head::reference(observed.as_ref(), snapshot)?;
                if head.revision != Some(revision) {
                    return Err(SplitLifecycleError::HeadConflict {
                        snapshot,
                        observed: Box::new(head),
                    });
                }
                current_reference(revision, observed.as_ref())
            }
        }
    }
}

fn current_reference(
    revision: SplitRevisionId,
    current: Option<&StoredValue>,
) -> Result<SplitReference, SplitLifecycleError> {
    let current = current.ok_or(SplitLifecycleError::Conflict)?;
    Ok(SplitReference {
        revision: Some(revision),
        generation: Some(CommitGeneration::from_head(&current.bytes)),
    })
}

fn attempt_update(
    attempt: &CommitAttempt,
    expected_revision: Option<SplitRevisionId>,
) -> Result<(SnapshotId, SplitHeadUpdate), SplitLifecycleError> {
    attempt.validate()?;
    let CommitTarget::SplitRevision { snapshot, revision } = attempt.target else {
        return Err(SplitLifecycleError::Conflict);
    };
    Ok((
        snapshot,
        SplitHeadUpdate {
            revision,
            expected_revision,
            expected_head: attempt.expected_head,
        },
    ))
}

fn confirmed_commit(
    attempt: &CommitAttempt,
    head: &SplitReference,
) -> Result<SuccessfulCommit, SplitLifecycleError> {
    Ok(SuccessfulCommit {
        version: 1,
        attempt: attempt.clone(),
        committed_head: head.generation.ok_or(SplitLifecycleError::Corrupt)?,
    })
}
