// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, DatasetUploads, MetadataScanBudget, ReadCancellation,
    SnapshotId, SnapshotRecord, StoreError, UploadReclaimError,
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
impl DatasetAuthority {
    /// Advances durable owner and metadata retirement without waiting for physical upload cleanup.
    /// Each component resumes its own bounded window after interruption or restart.
    /// # Errors
    /// Rejects protected owners, unsupported content, storage failures and bounded-work exhaustion.
    pub async fn reclaim_snapshot_step(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        uploads: &DatasetUploads,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotReclaimStatus, SnapshotReclaimError> {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.reclaim_snapshot_window(identity, snapshot, uploads, cancel),
        )
        .await
        .map_err(|_| SnapshotReclaimError::Limit)?
    }
    async fn reclaim_snapshot_window(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        uploads: &DatasetUploads,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotReclaimStatus, SnapshotReclaimError> {
        if cancel.is_cancelled() {
            return Err(crate::inspection::InspectionError::Cancelled.into());
        }
        let mut status = SnapshotReclaimStatus {
            snapshot,
            phase: SnapshotReclaimPhase::UploadOwners,
            complete: false,
            physical_cleanup_deferred: true,
        };
        if !self
            .reclaim_snapshot_uploads_step(identity, snapshot, uploads)
            .await?
            .complete
        {
            return Ok(status);
        }
        status.phase = SnapshotReclaimPhase::SplitDrafts;
        if !self
            .cleanup_snapshot_split_drafts_step(identity, snapshot, cancel)
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
            .retire_candidate_metadata_step(identity, &record, cancel)
            .await?
        {
            return Ok(status);
        }
        status.phase = SnapshotReclaimPhase::Metadata;
        if self
            .reclaim_snapshot_metadata_step(identity, snapshot, cancel)
            .await?
            .complete
        {
            status.phase = SnapshotReclaimPhase::Complete;
            status.complete = true;
        }
        Ok(status)
    }
}
