// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    admission::terminal,
    resource::{DeletionPhase, RetiringRevision, RevisionResource},
    SplitRevisionDeletion,
};
use crate::{
    split::{key, SplitLifecycleError, RETIRED_RECORD},
    AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, MetadataScan, MetadataScanBudget,
    ReadCancellation, SnapshotId, SplitRevisionId, StoredValue,
};
use std::time::Duration;

/// Discardable work position; the final SR remains the only discovery authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitRevisionReclaimCursor {
    pub(super) identity: DatasetIdentity,
    pub(super) snapshot: SnapshotId,
    pub(super) revision: SplitRevisionId,
    after: Option<Vec<u8>>,
    pub(super) waiting: bool,
}
impl SplitRevisionReclaimCursor {
    #[must_use]
    pub fn new(identity: DatasetIdentity, snapshot: SnapshotId, revision: SplitRevisionId) -> Self {
        Self {
            identity,
            snapshot,
            revision,
            after: None,
            waiting: false,
        }
    }
}

impl DatasetAuthority {
    /// Seals one bounded member-page window, preserving the full SR until its final step.
    /// Terminal roots remain discoverable so arbitrarily late unknown pages are reswept.
    /// # Errors
    /// Rejects live unmarked owners, malformed resource facts, cancellation and storage failures.
    pub async fn advance_split_revision_reclaim(
        &self,
        cursor: &mut SplitRevisionReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<SplitRevisionDeletion, SplitLifecycleError> {
        super::cancelled(cancel)?;
        cursor.waiting = false;
        let root = key::revision(&cursor.identity, cursor.snapshot, cursor.revision)
            .map_err(AuthorityError::from)?;
        let mut stored = self
            .split_state_get(&root)
            .await?
            .ok_or(SplitLifecycleError::NotFound)?;
        for _ in 0..64 {
            let decoded = RevisionResource::decode(&stored.bytes, cursor.snapshot, cursor.revision)?;
            let (resource, result) = match decoded {
                RevisionResource::Sealed => (None, terminal(cursor.snapshot, cursor.revision)),
                RevisionResource::Complete(record) => {
                    let deadline = self
                        .split_owner_reclaim_deadline(&cursor.identity, cursor.snapshot)
                        .await?
                        .ok_or(SplitLifecycleError::Frozen)?;
                    let value = RetiringRevision {
                        phase: DeletionPhase::Retiring,
                        reclaim_after: deadline,
                        record,
                    };
                    let bytes = value.encode()?;
                    super::cancelled(cancel)?;
                    match self
                        .store
                        .compare_exchange(&root, Some(&stored.bytes), &bytes)
                        .await?
                    {
                        CasOutcome::Applied(revision) => stored = StoredValue { bytes, revision },
                        CasOutcome::Conflict(Some(observed)) => stored = observed,
                        CasOutcome::Conflict(None) => return Err(SplitLifecycleError::Conflict),
                    }
                    continue;
                }
                RevisionResource::Deleting(mut value) => {
                    if value.phase == DeletionPhase::Closing {
                        if self
                            .split_owner_reclaim_deadline(&cursor.identity, cursor.snapshot)
                            .await?
                            .is_some()
                        {
                            value.phase = DeletionPhase::Retiring;
                            let bytes = value.encode()?;
                            super::cancelled(cancel)?;
                            match self
                                .store
                                .compare_exchange(&root, Some(&stored.bytes), &bytes)
                                .await?
                            {
                                CasOutcome::Applied(revision) => stored = StoredValue { bytes, revision },
                                CasOutcome::Conflict(Some(observed)) => stored = observed,
                                CasOutcome::Conflict(None) => return Err(SplitLifecycleError::Conflict),
                            }
                            continue;
                        }
                        let result = self
                            .mark_split_revision(&cursor.identity, cursor.snapshot, cursor.revision, cancel)
                            .await?;
                        cursor.waiting = result
                            .reclaim_after
                            .is_some_and(|deadline| deadline > self.reclaim_time());
                        return Ok(result);
                    }
                    if self.reclaim_time() < value.reclaim_after
                        && self
                            .split_owner_reclaim_deadline(&cursor.identity, cursor.snapshot)
                            .await?
                            .is_none()
                    {
                        cursor.waiting = true;
                        return Ok(value.result());
                    }
                    let result = value.result();
                    (Some(value), result)
                }
            };
            return self
                .retire_split_member_window(cursor, stored, resource.is_some(), result, cancel)
                .await;
        }
        Err(SplitLifecycleError::Conflict)
    }

    async fn retire_split_member_window(
        &self,
        cursor: &mut SplitRevisionReclaimCursor,
        stored: StoredValue,
        finish_root: bool,
        result: SplitRevisionDeletion,
        cancel: &ReadCancellation,
    ) -> Result<SplitRevisionDeletion, SplitLifecycleError> {
        let root = key::revision(&cursor.identity, cursor.snapshot, cursor.revision)
            .map_err(AuthorityError::from)?;
        let prefix = key::membership_prefix(&cursor.identity, cursor.snapshot, cursor.revision)
            .map_err(AuthorityError::from)?;
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix,
                    after: cursor.after.clone(),
                    budget: MetadataScanBudget {
                        items: 20,
                        bytes: 2 * 1024 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                },
                cancel,
            )
            .await?;
        for (key, value) in page.entries {
            super::cancelled(cancel)?;
            self.seal_retired_split_value(&key, Some(value), cancel).await?;
            cursor.after = Some(key);
        }
        if page.after.is_some() {
            return Ok(SplitRevisionDeletion {
                complete: false,
                ..result
            });
        }
        let summary = key::revision_summary(&cursor.identity, cursor.snapshot, cursor.revision)
            .map_err(AuthorityError::from)?;
        super::cancelled(cancel)?;
        let old = self.split_state_get(&summary).await?;
        self.seal_retired_split_value(&summary, old, cancel).await?;
        if finish_root {
            super::cancelled(cancel)?;
            match self
                .store
                .compare_exchange(&root, Some(&stored.bytes), RETIRED_RECORD)
                .await?
            {
                CasOutcome::Applied(_) => {}
                CasOutcome::Conflict(Some(value)) if value.bytes == RETIRED_RECORD => {}
                CasOutcome::Conflict(_) => return Err(SplitLifecycleError::Conflict),
            }
        }
        cursor.after = None;
        Ok(terminal(cursor.snapshot, cursor.revision))
    }

    async fn split_owner_reclaim_deadline(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<Option<u64>, SplitLifecycleError> {
        match self.snapshot_reclaim_after(identity, snapshot).await {
            Ok(deadline) => Ok((deadline <= self.reclaim_time()).then_some(deadline)),
            Err(AuthorityError::SnapshotProtected) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn seal_retired_split_value(
        &self,
        key: &[u8],
        mut old: Option<StoredValue>,
        cancel: &ReadCancellation,
    ) -> Result<(), SplitLifecycleError> {
        for _ in 0..64 {
            super::cancelled(cancel)?;
            if old.as_ref().is_some_and(|value| value.bytes == RETIRED_RECORD) {
                return Ok(());
            }
            match self
                .store
                .compare_exchange(
                    key,
                    old.as_ref().map(|value| value.bytes.as_slice()),
                    RETIRED_RECORD,
                )
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(observed) => old = observed,
            }
        }
        Err(SplitLifecycleError::Conflict)
    }
}
