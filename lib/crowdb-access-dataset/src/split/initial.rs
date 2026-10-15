// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    state::{state_key, DraftState},
    DraftSplit, MembershipOrigin, SplitDraftRecord, SplitLifecycleError, SplitPolicy, SplitRevisionRecord,
};
use crate::{
    composition::DraftProgress, AuthorityError, DatasetAuthority, DatasetIdentity, OperationId, SnapshotId,
    SplitRevisionId,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

impl DatasetAuthority {
    pub(crate) async fn release_initial_split_leases(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<(), SplitLifecycleError> {
        let (_, state) = self.load_split_draft(identity, operation).await?;
        if !state.initial {
            return Err(SplitLifecycleError::Conflict);
        }
        self.release_split_draft_leases(identity, &state).await
    }

    pub(crate) async fn begin_initial_split(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        snapshot: SnapshotId,
        policy: SplitPolicy,
        splits: Vec<DraftSplit>,
    ) -> Result<SplitDraftRecord, SplitLifecycleError> {
        self.require_initial_split_owner(identity, snapshot, operation)
            .await?;
        let record = SplitDraftRecord {
            version: 1,
            operation,
            snapshot,
            expected_revision: None,
            revision: 1,
            policy,
            splits,
            progress: DraftProgress::Editing,
        };
        let initial = record.encode()?;
        let candidate = self
            .snapshot_candidate(identity, snapshot)
            .await?
            .ok_or(SplitLifecycleError::NotFound)?;
        for split in &record.splits {
            if let MembershipOrigin::Inherited(source) = &split.origin {
                if candidate.parent != Some(source.snapshot) {
                    return Err(SplitLifecycleError::Conflict);
                }
                let source_record = self
                    .get_split_revision(
                        identity,
                        source.snapshot,
                        source.revision,
                        crate::MetadataScanBudget {
                            items: 1,
                            bytes: 1024 * 1024,
                            timeout: Duration::from_secs(2),
                        },
                        &crate::ReadCancellation::new(),
                    )
                    .await?;
                if !source_record.splits.iter().any(|s| s.name == source.name) {
                    return Err(SplitLifecycleError::NotFound);
                }
            }
        }
        self.put_split_state(&state_key(b"DS\0SB\0", identity, operation, None)?, &initial)
            .await?;
        let state = DraftState {
            source_leases: super::inheritance::source_owners(&record).into_iter().collect(),
            record,
            initial: true,
            edit: None,
            evaluation: None,
        };
        let key = state_key(b"DS\0SD\0", identity, operation, None)?;
        if self.split_state_get(&key).await?.is_none() {
            match self.put_split_state(&key, &state.encode()?).await {
                Ok(()) | Err(SplitLifecycleError::Conflict) => {}
                Err(error) => return Err(error),
            }
        }
        let (_, current) = self.load_split_draft(identity, operation).await?;
        if !current.initial {
            return Err(SplitLifecycleError::Conflict);
        }
        self.renew_split_draft_lease(identity, &current).await?;
        Ok(current.record)
    }
    pub(crate) async fn require_initial_split_owner(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        operation: OperationId,
    ) -> Result<(), AuthorityError> {
        let candidate = self
            .snapshot_candidate(identity, snapshot)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        if candidate.initial_split != Some(super::candidate_revision(operation, 1)?) {
            return Err(AuthorityError::AlreadyExists);
        }
        if let Some(parent) = candidate.parent {
            self.require_committed_snapshot(identity, parent).await?;
        }
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.touch_read_handle(identity, snapshot, operation, time, 3600)
            .await
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
        let (_, state) =
            self.load_split_draft(identity, record.operation)
                .await
                .map_err(|error| match error {
                    SplitLifecycleError::Authority(error) => error,
                    SplitLifecycleError::Store(error) => error.into(),
                    _ => AuthorityError::Corrupt,
                })?;
        if !state.initial
            || state.record.progress
                != (DraftProgress::Prepared {
                    target: crate::CommitTarget::SplitRevision { snapshot, revision },
                })
        {
            return Err(AuthorityError::SnapshotNotFound);
        }
        Ok(())
    }
    pub(crate) async fn initial_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<Option<SplitRevisionId>, AuthorityError> {
        Ok(self
            .snapshot_candidate(identity, snapshot)
            .await?
            .and_then(|candidate| candidate.initial_split))
    }
}
