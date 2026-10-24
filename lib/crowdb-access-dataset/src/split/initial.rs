// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{state::state_key, SplitLifecycleError, SplitRevisionRecord};
use crate::{AuthorityError, DatasetAuthority, DatasetIdentity, OperationId, SnapshotId, SplitRevisionId};
use std::time::Duration;

impl DatasetAuthority {
    pub(crate) async fn fence_initial_split_reservation(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<bool, SplitLifecycleError> {
        let key = state_key(b"DS\0SD\0", identity, operation, None)?;
        let stored = self.split_state_get(&key).await?;
        if let Some(stored) = &stored {
            if stored.bytes != super::cleanup::SEALED {
                return Ok(true);
            }
        } else {
            match self
                .store
                .compare_exchange(&key, None, super::cleanup::SEALED)
                .await?
            {
                crate::CasOutcome::Applied(_) => {}
                crate::CasOutcome::Conflict(Some(v)) if v.bytes == super::cleanup::SEALED => {}
                crate::CasOutcome::Conflict(_) => return Err(SplitLifecycleError::Conflict),
            }
        }
        let begin = state_key(b"DS\0SB\0", identity, operation, None)?;
        let stored = self.split_state_get(&begin).await?;
        match self
            .store
            .compare_exchange(
                &begin,
                stored.as_ref().map(|v| v.bytes.as_slice()),
                super::cleanup::SEALED,
            )
            .await?
        {
            crate::CasOutcome::Applied(_) => Ok(false),
            crate::CasOutcome::Conflict(Some(v)) if v.bytes == super::cleanup::SEALED => Ok(false),
            crate::CasOutcome::Conflict(_) => Err(SplitLifecycleError::Conflict),
        }
    }

    pub(crate) async fn require_initial_split_owner(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
    ) -> Result<Option<SnapshotId>, SplitLifecycleError> {
        let candidate = self
            .snapshot_candidate(identity, snapshot)
            .await?
            .ok_or(SplitLifecycleError::NotFound)?;
        if candidate.initial_split != Some(revision) {
            return Err(SplitLifecycleError::Conflict);
        }
        if let Some(parent) = candidate.parent {
            self.require_committed_snapshot(identity, parent).await?;
        }
        self.check_snapshot_readable(identity, snapshot).await?;
        Ok(candidate.parent)
    }
    pub(crate) async fn validate_initial_split(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        initial: Option<SplitRevisionId>,
    ) -> Result<(), AuthorityError> {
        let Some(revision) = initial else {
            return Ok(());
        };
        let value = self
            .store
            .get_bounded(
                &super::key::revision(identity, snapshot, revision)?,
                crate::MetadataScanBudget {
                    items: 1,
                    bytes: 1024 * 1024,
                    timeout: Duration::from_secs(2),
                },
                &crate::ReadCancellation::new(),
            )
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let record = SplitRevisionRecord::decode(&value.bytes).map_err(|_| AuthorityError::Corrupt)?;
        if record.snapshot != snapshot || record.revision != revision {
            return Err(AuthorityError::Corrupt);
        }
        Ok(())
    }
    pub(crate) async fn initial_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<Option<SplitRevisionId>, AuthorityError> {
        match self.snapshot_candidate(identity, snapshot).await {
            Ok(candidate) => Ok(candidate.and_then(|candidate| candidate.initial_split)),
            Err(AuthorityError::SnapshotProtected) => Ok(None),
            Err(error) => Err(error),
        }
    }
}
