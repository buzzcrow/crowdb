// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, SplitHead, SplitLifecycleError};
use crate::{
    AuthorityError, CasOutcome, CommitAttempt, CommitTarget, DatasetAuthority, DatasetIdentity, OperationId,
    SnapshotId, SuccessfulCommit,
};

impl DatasetAuthority {
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
        let mut old = self.store.get(&key).await?;
        let mut initial = None;
        for _ in 0..64 {
            let observed = super::head::reference(old.as_ref(), snapshot)?;
            let revision = if observed.revision.is_some() {
                observed.revision
            } else {
                if initial.is_none() {
                    initial = Some(self.initial_split_revision(identity, snapshot).await?);
                }
                initial.flatten()
            };
            let bytes = SplitHead {
                version: 1,
                snapshot,
                revision,
                fence: OperationId::random(),
            }
            .encode()?;
            match self
                .store
                .compare_exchange(&key, old.as_ref().map(|v| v.bytes.as_slice()), &bytes)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(observed) => old = observed,
            }
        }
        Err(SplitLifecycleError::Conflict)
    }
}
fn authority_error(error: SplitLifecycleError) -> AuthorityError {
    match error {
        SplitLifecycleError::Authority(error) => error,
        SplitLifecycleError::Store(error) => error.into(),
        SplitLifecycleError::Conflict | SplitLifecycleError::HeadConflict { .. } => {
            AuthorityError::HeadConflict
        }
        SplitLifecycleError::NotFound => AuthorityError::SnapshotNotFound,
        SplitLifecycleError::Limit => crate::DatasetError::ReadLimitExceeded.into(),
        _ => AuthorityError::Corrupt,
    }
}
