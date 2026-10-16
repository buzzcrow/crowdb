// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, SplitHead, SplitLifecycleError};
use crate::{
    AuthorityError, CasOutcome, CommitAttempt, CommitTarget, DatasetAuthority, DatasetIdentity, OperationId,
    SnapshotId, SuccessfulCommit,
};

impl DatasetAuthority {
    pub(super) async fn reconcile_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<super::SplitDraftRecord, SplitLifecycleError> {
        let (old, state) = self.load_split_draft(identity, operation).await?;
        if let crate::composition::DraftProgress::Committing { target, attempt: id } = state.record.progress {
            let attempt = self
                .stored_commit_attempt(identity, id)
                .await?
                .ok_or(SplitLifecycleError::Corrupt)?;
            if attempt.target != target {
                return Err(SplitLifecycleError::Corrupt);
            }
            if let Some(receipt) = self.commit_status(identity, id).await? {
                self.finish_split_publication(identity, &receipt, &old, &state)
                    .await?;
                return Ok(self.load_split_draft(identity, operation).await?.1.record);
            }
            let CommitTarget::SplitRevision { snapshot, .. } = target else {
                return Err(SplitLifecycleError::Corrupt);
            };
            let observed = self.inspect_split_head(identity, snapshot).await?;
            if observed.generation != attempt.expected_head {
                if let Some(receipt) = self.commit_status(identity, id).await? {
                    self.finish_split_publication(identity, &receipt, &old, &state)
                        .await?;
                } else {
                    self.reset_split_publication(identity, &old, &state).await?;
                }
                return Ok(self.load_split_draft(identity, operation).await?.1.record);
            }
        }
        Ok(state.record)
    }

    pub(crate) async fn recover_split_commit(
        &self,
        identity: &DatasetIdentity,
        attempt: &CommitAttempt,
    ) -> Result<Option<SuccessfulCommit>, AuthorityError> {
        let CommitTarget::SplitRevision { snapshot, revision } = attempt.target else {
            return Err(AuthorityError::Corrupt);
        };
        self.split_revision_receipt(identity, snapshot, revision)
            .await
            .map(|receipt| receipt.filter(|r| r.attempt == *attempt))
            .map_err(authority_error)
    }
    pub(crate) async fn fence_split_publication(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        self.invalidate_split_head(identity, snapshot)
            .await
            .map_err(authority_error)
    }
    async fn invalidate_split_head(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), SplitLifecycleError> {
        let key = key::head(identity, snapshot).map_err(AuthorityError::from)?;
        for _ in 0..64 {
            let old = self.store.get(&key).await?;
            if let Some(value) = &old {
                self.help_split_head(identity, value).await?;
            }
            let observed = self
                .resolve_split_reference(identity, old.as_ref(), snapshot)
                .await?;
            let bytes = SplitHead {
                version: 1,
                snapshot,
                revision: observed.revision,
                fence: OperationId::random(),
            }
            .encode()?;
            if matches!(
                self.store
                    .compare_exchange(&key, old.as_ref().map(|v| v.bytes.as_slice()), &bytes)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(SplitLifecycleError::Conflict)
    }
}
fn authority_error(error: SplitLifecycleError) -> AuthorityError {
    match error {
        SplitLifecycleError::Authority(error) => error,
        SplitLifecycleError::Store(error) => error.into(),
        SplitLifecycleError::Conflict => AuthorityError::HeadConflict,
        SplitLifecycleError::NotFound => AuthorityError::SnapshotNotFound,
        SplitLifecycleError::Limit => crate::DatasetError::ReadLimitExceeded.into(),
        _ => AuthorityError::Corrupt,
    }
}
