// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    resource::{DeletionPhase, RetiringRevision, RevisionResource},
    SplitRevisionDeletion,
};
use crate::{
    split::{head, key, SplitHead, SplitLifecycleError},
    AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, OperationId, ReadCancellation, SnapshotId,
    SplitRevisionId, StoredValue,
};

impl DatasetAuthority {
    pub(super) async fn mark_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        cancel: &ReadCancellation,
    ) -> Result<SplitRevisionDeletion, SplitLifecycleError> {
        super::cancelled(cancel)?;
        let key = key::revision(identity, snapshot, revision).map_err(AuthorityError::from)?;
        let mut stored = self
            .split_state_get(&key)
            .await?
            .ok_or(SplitLifecycleError::NotFound)?;
        match RevisionResource::decode(&stored.bytes, snapshot, revision)? {
            RevisionResource::Sealed => return Ok(terminal(snapshot, revision)),
            RevisionResource::Deleting(value) if value.phase == DeletionPhase::Retiring => {
                return Ok(value.result())
            }
            _ => {}
        }
        let (initial, head) = self.split_deletion_admission(identity, snapshot).await?;
        for _ in 0..64 {
            super::cancelled(cancel)?;
            match RevisionResource::decode(&stored.bytes, snapshot, revision)? {
                RevisionResource::Sealed => return Ok(terminal(snapshot, revision)),
                RevisionResource::Deleting(value) => {
                    if value.phase == DeletionPhase::Retiring {
                        return Ok(value.result());
                    }
                    if initial == Some(revision) {
                        return self.restore_protected_split(&key, &stored, &value).await;
                    }
                    return self
                        .finish_split_mark(identity, stored, value, head, cancel)
                        .await;
                }
                RevisionResource::Complete(record) => {
                    if initial == Some(revision)
                        || head::reference(head.as_ref(), snapshot)?.revision == Some(revision)
                    {
                        return Err(SplitLifecycleError::Frozen);
                    }
                    let value = RetiringRevision {
                        phase: DeletionPhase::Closing,
                        reclaim_after: self
                            .reclaim_time()
                            .checked_add(20 * 60)
                            .ok_or(SplitLifecycleError::Limit)?,
                        record,
                    };
                    let bytes = value.encode()?;
                    match self
                        .store
                        .compare_exchange(&key, Some(&stored.bytes), &bytes)
                        .await?
                    {
                        CasOutcome::Applied(revision) => stored = StoredValue { bytes, revision },
                        CasOutcome::Conflict(Some(observed)) => stored = observed,
                        CasOutcome::Conflict(None) => return Err(SplitLifecycleError::Conflict),
                    }
                }
            }
        }
        Err(SplitLifecycleError::Conflict)
    }

    async fn split_deletion_admission(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(Option<SplitRevisionId>, Option<StoredValue>), SplitLifecycleError> {
        self.check_snapshot_readable(identity, snapshot).await?;
        let candidate = self.snapshot_candidate(identity, snapshot).await?;
        if !candidate.as_ref().is_some_and(|value| value.version == 2) {
            self.require_committed_snapshot(identity, snapshot).await?;
        }
        let initial = candidate.and_then(|value| value.initial_split);
        let head_key = key::head(identity, snapshot).map_err(AuthorityError::from)?;
        Ok((initial, self.split_state_get(&head_key).await?))
    }

    async fn finish_split_mark(
        &self,
        identity: &DatasetIdentity,
        stored: StoredValue,
        value: RetiringRevision,
        mut head: Option<StoredValue>,
        cancel: &ReadCancellation,
    ) -> Result<SplitRevisionDeletion, SplitLifecycleError> {
        let snapshot = value.record.snapshot;
        let revision = value.record.revision;
        let resource_key = key::revision(identity, snapshot, revision).map_err(AuthorityError::from)?;
        let head_key = key::head(identity, snapshot).map_err(AuthorityError::from)?;
        for _ in 0..64 {
            super::cancelled(cancel)?;
            let observed = head::reference(head.as_ref(), snapshot)?;
            if observed.revision == Some(revision) {
                return self.restore_protected_split(&resource_key, &stored, &value).await;
            }
            let bytes = SplitHead {
                version: 3,
                snapshot,
                revision: observed.revision,
                fence: OperationId::random(),
            }
            .encode()?;
            if head.as_ref().is_some_and(|old| old.bytes == bytes) {
                continue;
            }
            match self
                .store
                .compare_exchange(&head_key, head.as_ref().map(|old| old.bytes.as_slice()), &bytes)
                .await?
            {
                CasOutcome::Applied(_) => {
                    super::cancelled(cancel)?;
                    return self.activate_split_mark(&resource_key, &stored, value).await;
                }
                CasOutcome::Conflict(observed) => head = observed,
            }
        }
        Err(SplitLifecycleError::Conflict)
    }

    async fn activate_split_mark(
        &self,
        key: &[u8],
        stored: &StoredValue,
        mut value: RetiringRevision,
    ) -> Result<SplitRevisionDeletion, SplitLifecycleError> {
        value.phase = DeletionPhase::Retiring;
        match self
            .store
            .compare_exchange(key, Some(&stored.bytes), &value.encode()?)
            .await?
        {
            CasOutcome::Applied(_) => Ok(value.result()),
            CasOutcome::Conflict(Some(observed)) => {
                closed_result(&observed.bytes, value.record.snapshot, value.record.revision)
            }
            CasOutcome::Conflict(None) => Err(SplitLifecycleError::Conflict),
        }
    }

    async fn restore_protected_split(
        &self,
        key: &[u8],
        stored: &StoredValue,
        value: &RetiringRevision,
    ) -> Result<SplitRevisionDeletion, SplitLifecycleError> {
        match self
            .store
            .compare_exchange(key, Some(&stored.bytes), &value.record.encode()?)
            .await?
        {
            CasOutcome::Applied(_) => Err(SplitLifecycleError::Frozen),
            CasOutcome::Conflict(Some(observed)) => {
                closed_result(&observed.bytes, value.record.snapshot, value.record.revision)
            }
            CasOutcome::Conflict(None) => Err(SplitLifecycleError::Conflict),
        }
    }
}

fn closed_result(
    bytes: &[u8],
    snapshot: SnapshotId,
    revision: SplitRevisionId,
) -> Result<SplitRevisionDeletion, SplitLifecycleError> {
    match RevisionResource::decode(bytes, snapshot, revision)? {
        RevisionResource::Deleting(value) if value.phase == DeletionPhase::Retiring => Ok(value.result()),
        RevisionResource::Sealed => Ok(terminal(snapshot, revision)),
        _ => Err(SplitLifecycleError::Conflict),
    }
}

pub(super) fn terminal(snapshot: SnapshotId, revision: SplitRevisionId) -> SplitRevisionDeletion {
    SplitRevisionDeletion {
        snapshot,
        revision,
        reclaim_after: None,
        complete: true,
    }
}
