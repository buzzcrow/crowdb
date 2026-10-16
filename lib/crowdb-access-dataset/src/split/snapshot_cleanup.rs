// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{state::DraftState, SplitLifecycleError};
use crate::{
    inspection::work::InspectionWork, AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity,
    MetadataScanBudget, OperationId, ReadCancellation, SnapshotId,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSplitCleanup {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub after: Option<Vec<u8>>,
    pub scanned_drafts: u64,
    pub complete: bool,
}
impl DatasetAuthority {
    /// Finds and retires work from all drafts of a terminal Snapshot in resumable windows.
    /// Draft roots remain as durable receipts; newly started drafts cannot evaluate a retired owner.
    /// # Errors
    /// Rejects live owners, invalid cursors, cancellation and bounded-work failures.
    pub async fn cleanup_snapshot_split_drafts_step(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotSplitCleanup, SplitLifecycleError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.cleanup_snapshot_draft_window(identity, snapshot, cancel),
        )
        .await
        .map_err(|_| SplitLifecycleError::Limit)?
    }
    async fn cleanup_snapshot_draft_window(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
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
        let mut prefix = b"DS\0SD\0".to_vec();
        prefix.extend(identity.key().map_err(AuthorityError::from)?);
        let mut cursor_key = b"DS\0GSD\0".to_vec();
        cursor_key.extend(identity.key().map_err(AuthorityError::from)?);
        cursor_key.extend(snapshot.as_bytes());
        let stored = work.get(&cursor_key, 4096).await?;
        let mut progress: SnapshotSplitCleanup = stored.as_ref().map_or_else(
            || {
                Ok(SnapshotSplitCleanup {
                    version: 1,
                    snapshot,
                    after: None,
                    scanned_drafts: 0,
                    complete: false,
                })
            },
            |v| super::codec::decode(&v.bytes),
        )?;
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
            return Ok(progress);
        }
        let page = work.scan(prefix.clone(), progress.after.clone(), 1).await?;
        if let Some((key, value)) = page.entries.first() {
            let operation = OperationId::from_bytes(
                key.strip_prefix(prefix.as_slice())
                    .ok_or(SplitLifecycleError::Corrupt)?,
            )
            .map_err(AuthorityError::from)?;
            if value.bytes != super::cleanup::SEALED {
                let mut state = DraftState::decode(&value.bytes, operation)?;
                if state.record.snapshot == snapshot {
                    if state.work_version.is_none() {
                        let old = state.clone();
                        state.work_version = Some(1);
                        self.replace_split_draft(identity, value, &old, &state).await?;
                    }
                    if !self
                        .cleanup_split_scratch_step(identity, operation, cancel)
                        .await?
                        .complete
                    {
                        return Ok(progress);
                    }
                    self.release_split_draft_leases(identity, &state).await?;
                }
            }
            progress.after = Some(key.clone());
            progress.scanned_drafts = progress
                .scanned_drafts
                .checked_add(1)
                .ok_or(SplitLifecycleError::Limit)?;
        }
        progress.complete = page.after.is_none();
        let bytes = super::codec::encode(&progress)?;
        match self
            .store
            .compare_exchange(&cursor_key, stored.as_ref().map(|v| v.bytes.as_slice()), &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(progress),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(progress),
            CasOutcome::Conflict(_) => Err(SplitLifecycleError::Conflict),
        }
    }
}
