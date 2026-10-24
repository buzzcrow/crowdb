// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, DatasetUploads, MetadataScanBudget, ReadCancellation,
    SnapshotId, SnapshotRecord, SnapshotUploadCleanup, StoreError, UploadReclaimError,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotReclaimError {
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error(transparent)]
    Upload(#[from] UploadReclaimError),
    #[error(transparent)]
    Split(#[from] crate::split::SplitLifecycleError),
    #[error(transparent)]
    Inspection(#[from] crate::inspection::InspectionError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("Snapshot reclaim exceeded its work deadline")]
    Limit,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotReclaimPhase {
    GracePeriod,
    UploadOwners,
    SplitDrafts,
    IndexFences,
    Metadata,
    Complete,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotReclaimStatus {
    pub snapshot: SnapshotId,
    pub phase: SnapshotReclaimPhase,
    pub complete: bool,
    pub physical_cleanup_deferred: bool,
}

/// Opaque local cleanup position. Losing it repeats idempotent retirement work.
#[derive(Clone, Debug)]
pub struct SnapshotReclaimCursor {
    pub(super) identity: DatasetIdentity,
    pub(super) snapshot: SnapshotId,
    uploads: SnapshotUploadCleanup,
    metadata: super::MetadataCleanupProgress,
    splits: crate::split::SnapshotSplitCleanup,
    indexes: crate::inspection::CandidateRetirementCursor,
}
impl SnapshotReclaimCursor {
    #[must_use]
    pub fn new(identity: DatasetIdentity, snapshot: SnapshotId) -> Self {
        Self {
            identity,
            snapshot,
            uploads: SnapshotUploadCleanup::default(),
            metadata: super::MetadataCleanupProgress::new(snapshot),
            splits: crate::split::SnapshotSplitCleanup::new(snapshot),
            indexes: crate::inspection::CandidateRetirementCursor::default(),
        }
    }
}

impl DatasetAuthority {
    /// Marks deletion and observes its durable status. Background cleanup owns progress.
    /// # Errors
    /// Rejects protected or missing resources and unavailable storage.
    pub async fn mark_snapshot_reclaim(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<SnapshotReclaimStatus, SnapshotReclaimError> {
        self.begin_reclaim(identity, snapshot).await?;
        let stored = self
            .store
            .get(&crate::key::read_lease(identity, snapshot).map_err(AuthorityError::from)?)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let access = super::decode_access(&stored.bytes)?;
        Ok(SnapshotReclaimStatus {
            snapshot,
            phase: if access.cleanup_complete {
                SnapshotReclaimPhase::Complete
            } else if self.reclaim_time() < access.reclaim_after {
                SnapshotReclaimPhase::GracePeriod
            } else {
                SnapshotReclaimPhase::UploadOwners
            },
            complete: access.cleanup_complete,
            physical_cleanup_deferred: true,
        })
    }

    /// Advances durable owner and metadata retirement without waiting for physical upload cleanup.
    /// Each component resumes its own bounded window after interruption or restart.
    /// # Errors
    /// Rejects protected owners, unsupported content, storage failures and bounded-work exhaustion.
    pub async fn advance_snapshot_reclaim(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut SnapshotReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotReclaimStatus, SnapshotReclaimError> {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.reclaim_snapshot_window(uploads, cursor, cancel),
        )
        .await
        .map_err(|_| SnapshotReclaimError::Limit)?
    }
    async fn reclaim_snapshot_window(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut SnapshotReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotReclaimStatus, SnapshotReclaimError> {
        if cancel.is_cancelled() {
            return Err(crate::inspection::InspectionError::Cancelled.into());
        }
        let identity = &cursor.identity;
        let snapshot = cursor.snapshot;
        let mut status = self.mark_snapshot_reclaim(identity, snapshot).await?;
        if status.complete || status.phase == SnapshotReclaimPhase::GracePeriod {
            return Ok(status);
        }
        if !self
            .reclaim_snapshot_uploads_step(identity, snapshot, uploads, &mut cursor.uploads)
            .await?
            .complete
        {
            return Ok(status);
        }
        status.phase = SnapshotReclaimPhase::SplitDrafts;
        if !self
            .cleanup_snapshot_split_drafts_step(identity, snapshot, &mut cursor.splits, cancel)
            .await?
            .complete
        {
            return Ok(status);
        }
        let stored = self
            .store
            .get_bounded(
                &crate::key::snapshot(identity, snapshot).map_err(AuthorityError::from)?,
                MetadataScanBudget {
                    items: 1,
                    bytes: 9 * 1024 * 1024,
                    timeout: Duration::from_secs(2),
                },
                cancel,
            )
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let record: SnapshotRecord = crate::authority::decode(&stored.bytes)?;
        status.phase = SnapshotReclaimPhase::IndexFences;
        if !self
            .retire_candidate_metadata_step(identity, &record, &mut cursor.indexes, cancel)
            .await?
        {
            return Ok(status);
        }
        status.phase = SnapshotReclaimPhase::Metadata;
        if self
            .reclaim_snapshot_metadata_step(identity, snapshot, &mut cursor.metadata, cancel)
            .await?
            .complete
        {
            status.phase = SnapshotReclaimPhase::Complete;
            status.complete = true;
            self.update_access(identity, snapshot, |access| {
                access.cleanup_complete = true;
                Ok(())
            })
            .await?;
        }
        Ok(status)
    }
}
