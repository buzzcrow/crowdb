// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{state::DraftState, SplitLifecycleError};
use crate::{
    inspection::work::InspectionWork, AuthorityError, DatasetAuthority, DatasetIdentity, MetadataScanBudget,
    OperationId, ReadCancellation, SnapshotId,
};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSplitCleanup {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub after: Option<Vec<u8>>,
    pub scanned_drafts: u64,
    pub complete: bool,
    identity: Option<DatasetIdentity>,
    child: Option<super::SplitReclaimCursor>,
}
impl SnapshotSplitCleanup {
    #[must_use]
    pub fn new(snapshot: SnapshotId) -> Self {
        Self {
            version: 1,
            snapshot,
            after: None,
            scanned_drafts: 0,
            complete: false,
            identity: None,
            child: None,
        }
    }
}
impl DatasetAuthority {
    #[cfg(feature = "test-util")]
    /// Advances an isolated legacy-draft cleanup fixture.
    /// # Errors
    /// Rejects live owners, malformed progress, cancellation and storage failures.
    pub async fn cleanup_snapshot_split_drafts_step_for_tests(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        progress: &mut SnapshotSplitCleanup,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotSplitCleanup, SplitLifecycleError> {
        self.cleanup_snapshot_split_drafts_step(identity, snapshot, progress, cancel)
            .await
    }

    /// Finds and retires work from all drafts of a terminal Snapshot in resumable windows.
    /// Draft roots remain as durable receipts; newly started drafts cannot evaluate a retired owner.
    /// # Errors
    /// Rejects live owners, invalid cursors, cancellation and bounded-work failures.
    pub(crate) async fn cleanup_snapshot_split_drafts_step(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        progress: &mut SnapshotSplitCleanup,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotSplitCleanup, SplitLifecycleError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.cleanup_snapshot_draft_window(identity, snapshot, progress, cancel),
        )
        .await
        .map_err(|_| SplitLifecycleError::Limit)?
    }
    async fn cleanup_snapshot_draft_window(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        progress: &mut SnapshotSplitCleanup,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotSplitCleanup, SplitLifecycleError> {
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 1,
                bytes: 2 * 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )?;
        if !self
            .inspection_snapshot_retired(identity, snapshot, &mut work)
            .await?
        {
            return Err(SplitLifecycleError::Frozen);
        }
        self.require_reclaim_ready(identity, snapshot).await?;
        let mut prefix = b"DS\0SD\0".to_vec();
        prefix.extend(identity.key().map_err(AuthorityError::from)?);
        if progress.identity.as_ref().is_some_and(|bound| bound != identity) {
            return Err(SplitLifecycleError::Corrupt);
        }
        progress.identity = Some(identity.clone());
        if progress.version != 1
            || progress.snapshot != snapshot
            || progress
                .after
                .as_ref()
                .is_some_and(|v| !v.starts_with(&prefix) || v.len() != prefix.len() + 16)
        {
            return Err(SplitLifecycleError::Corrupt);
        }
        if progress.complete {
            return Ok(progress.clone());
        }
        let page = work.scan(prefix.clone(), progress.after.clone(), 1).await?;
        if let Some((key, value)) = page.entries.first() {
            let operation = OperationId::from_bytes(
                key.strip_prefix(prefix.as_slice())
                    .ok_or(SplitLifecycleError::Corrupt)?,
            )
            .map_err(AuthorityError::from)?;
            if value.bytes != super::cleanup::SEALED {
                let state = DraftState::decode(&value.bytes, operation)?;
                if state.record.snapshot == snapshot {
                    let child = progress
                        .child
                        .get_or_insert_with(|| super::SplitReclaimCursor::new(identity.clone(), operation));
                    if !self.advance_split_reclaim(child, cancel).await?.complete {
                        return Ok(progress.clone());
                    }
                    progress.child = None;
                }
            }
            progress.after = Some(key.clone());
            progress.scanned_drafts = progress
                .scanned_drafts
                .checked_add(1)
                .ok_or(SplitLifecycleError::Limit)?;
        }
        progress.complete = page.after.is_none();
        Ok(progress.clone())
    }
}
