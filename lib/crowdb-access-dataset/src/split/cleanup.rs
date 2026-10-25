// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    state::state_key,
    work::{work_key, EvaluationIntent},
    SplitLifecycleError,
};
use crate::{
    composition::DraftProgress, inspection::work::InspectionWork, AuthorityError, CasOutcome,
    DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId, ReadCancellation,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

mod revision;

pub(crate) const SEALED: &[u8] = b"split-retired-v1";
const END: &[u8] = b"split-retired-end-v1";
const RECORD_CAP: usize = 1024 * 1024 + 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SplitScratchCleanup {
    pub input_revision: u64,
    pub next_page: u64,
    pub complete: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Cursor {
    revision: u64,
    page: u64,
    item: usize,
}
/// Opaque local work position bound to one historical Split draft.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitReclaimCursor {
    pub(super) identity: DatasetIdentity,
    pub(super) operation: OperationId,
    progress: Cursor,
}
impl SplitReclaimCursor {
    #[must_use]
    pub fn new(identity: DatasetIdentity, operation: OperationId) -> Self {
        Self {
            identity,
            operation,
            progress: Cursor {
                revision: 1,
                page: 0,
                item: 0,
            },
        }
    }
}
impl DatasetAuthority {
    /// Advances one bounded historical work window using a local cursor.
    /// # Errors
    /// Returns protected resources, corrupt facts, storage failures and work limits.
    pub async fn advance_split_reclaim(
        &self,
        cursor: &mut SplitReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<SplitScratchCleanup, SplitLifecycleError> {
        if cancel.is_cancelled() {
            return Err(crate::inspection::InspectionError::Cancelled.into());
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            let result = self
                .cleanup_split_scratch_step(&cursor.identity, cursor.operation, &mut cursor.progress, cancel)
                .await?;
            if result.complete {
                self.store
                    .delete(&state_key(
                        b"DS\0SCG\0",
                        &cursor.identity,
                        cursor.operation,
                        None,
                    )?)
                    .await?;
            }
            Ok(result)
        })
        .await
        .map_err(|_| SplitLifecycleError::Limit)?
    }
    #[cfg(feature = "test-util")]
    /// Advances an isolated historical scratch fixture.
    /// # Errors
    /// Returns protected resources, corrupt facts, storage failures and work limits.
    pub async fn cleanup_split_scratch_step_for_tests(
        &self,
        cursor: &mut SplitReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<SplitScratchCleanup, SplitLifecycleError> {
        self.advance_split_reclaim(cursor, cancel).await
    }
    /// Retires obsolete incomplete membership windows, preserving complete revision data.
    /// Each call processes at most twenty page references and retains small write fences.
    /// # Errors
    /// Rejects legacy drafts without work intents, live initial drafts, corruption and bounded-work failures.
    async fn cleanup_split_scratch_step(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cursor: &mut Cursor,
        cancel: &ReadCancellation,
    ) -> Result<SplitScratchCleanup, SplitLifecycleError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.cleanup_split_window(identity, operation, cursor, cancel),
        )
        .await
        .map_err(|_| SplitLifecycleError::Limit)?
    }
    async fn cleanup_split_window(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cursor: &mut Cursor,
        cancel: &ReadCancellation,
    ) -> Result<SplitScratchCleanup, SplitLifecycleError> {
        let (_, state) = self.load_split_draft(identity, operation).await?;
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 20,
                bytes: 14 * 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )?;
        let retired = self
            .inspection_snapshot_retired(identity, state.record.snapshot, &mut work)
            .await?;
        if state.work_version != Some(1) && !retired {
            return Err(SplitLifecycleError::Frozen);
        }
        if retired {
            self.require_legacy_scratch_reclaim_ready(identity, state.record.snapshot, cancel)
                .await?;
        }
        let complete_initial = state.initial
            && !retired
            && self
                .snapshot_has_commit_proof(identity, state.record.snapshot, &mut work)
                .await?;
        if state.initial && !retired && !complete_initial {
            return Err(SplitLifecycleError::Frozen);
        }
        let end = state
            .record
            .revision
            .checked_add(u64::from(
                retired
                    || complete_initial
                    || matches!(
                        state.record.progress,
                        DraftProgress::Prepared { .. }
                            | DraftProgress::Committing { .. }
                            | DraftProgress::Published { .. }
                            | DraftProgress::Abandoned
                    ),
            ))
            .ok_or(SplitLifecycleError::Limit)?;
        if cursor.revision == 0 || cursor.revision > end {
            return Err(SplitLifecycleError::Corrupt);
        }
        if cursor.revision < end {
            let revision = super::candidate_revision(operation, cursor.revision)?;
            let summary = super::SplitRevisionSummary {
                version: 1,
                snapshot: state.record.snapshot,
                revision,
                operation,
                split_count: 0,
            };
            let preserve = self
                .fence_split_cleanup_revision(identity, &summary, retired, &mut work)
                .await?;
            let intent_key = work_key(identity, operation, cursor.revision, cursor.page)?;
            self.retire_split_intent(
                &intent_key,
                &super::key::membership_prefix(identity, state.record.snapshot, revision)
                    .map_err(AuthorityError::from)?,
                !preserve,
                cursor,
                &mut work,
            )
            .await?;
        }
        Ok(SplitScratchCleanup {
            input_revision: cursor.revision,
            next_page: cursor.page,
            complete: cursor.revision == end,
        })
    }
    async fn retire_split_intent(
        &self,
        key: &[u8],
        prefix: &[u8],
        retire_members: bool,
        cursor: &mut Cursor,
        work: &mut InspectionWork<'_>,
    ) -> Result<(), SplitLifecycleError> {
        let stored = work.get(key, RECORD_CAP).await?;
        if stored.as_ref().map_or(true, |v| v.bytes == END) {
            if stored.is_none()
                && !matches!(
                    self.store.compare_exchange(key, None, END).await?,
                    CasOutcome::Applied(_)
                )
            {
                return Err(SplitLifecycleError::Conflict);
            }
            cursor.revision += 1;
            cursor.page = 0;
            cursor.item = 0;
            return Ok(());
        }
        let stored = stored.ok_or(SplitLifecycleError::Corrupt)?;
        if stored.bytes != SEALED {
            let intent: EvaluationIntent = super::codec::decode(&stored.bytes)?;
            if intent.version != 1
                || intent.pages.len() > 64
                || cursor.item > intent.pages.len()
                || intent.pages.iter().any(|p| !p.key.starts_with(prefix))
            {
                return Err(SplitLifecycleError::Corrupt);
            }
            let stop = (cursor.item + 20).min(intent.pages.len());
            if retire_members {
                for page in &intent.pages[cursor.item..stop] {
                    self.seal_split_key(&page.key, work).await?;
                }
            }
            cursor.item = stop;
            if stop < intent.pages.len() {
                return Ok(());
            }
            match self
                .store
                .compare_exchange(key, Some(&stored.bytes), SEALED)
                .await?
            {
                CasOutcome::Applied(_) => {}
                CasOutcome::Conflict(Some(v)) if v.bytes == SEALED => {}
                CasOutcome::Conflict(_) => return Err(SplitLifecycleError::Conflict),
            }
        }
        cursor.page = cursor.page.checked_add(1).ok_or(SplitLifecycleError::Limit)?;
        cursor.item = 0;
        Ok(())
    }
    async fn seal_split_key(
        &self,
        key: &[u8],
        work: &mut InspectionWork<'_>,
    ) -> Result<(), SplitLifecycleError> {
        self.seal_split_value(key, SEALED, work).await
    }
    async fn seal_split_value(
        &self,
        key: &[u8],
        bytes: &[u8],
        work: &mut InspectionWork<'_>,
    ) -> Result<(), SplitLifecycleError> {
        let stored = work.get(key, RECORD_CAP).await?;
        if stored
            .as_ref()
            .is_some_and(|v| v.bytes == bytes || v.bytes == SEALED)
        {
            return Ok(());
        }
        match self
            .store
            .compare_exchange(key, stored.as_ref().map(|v| v.bytes.as_slice()), bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(v)) if v.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(SplitLifecycleError::Conflict),
        }
    }
}
