// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, DatasetUploads, FieldLocator, ManifestRecord,
    MetadataScanBudget, ReadCancellation, SnapshotId, StoreError, UploadError,
};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum UploadReclaimError {
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("upload reclaim metadata is corrupt")]
    Corrupt,
    #[error("upload reclaim exceeds its work limit")]
    Limit,
}
/// Caller-owned scan position. It is never stored or accepted from REST clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotUploadCleanup {
    pub version: u16,
    pub sample: usize,
    pub field: usize,
    pub retired_references: u64,
    pub complete: bool,
    target: Option<(DatasetIdentity, SnapshotId)>,
}
impl Default for SnapshotUploadCleanup {
    fn default() -> Self {
        Self {
            version: 1,
            sample: 0,
            field: 0,
            retired_references: 0,
            complete: false,
            target: None,
        }
    }
}
impl DatasetAuthority {
    /// Retires payload ownership only after terminal Snapshot admission fencing.
    /// Physical upload cleanup remains in the durable upload registry; shared owners survive.
    /// # Errors
    /// Rejects protected Snapshots, unsupported Manifest sizes, corrupt state and work limits.
    pub async fn reclaim_snapshot_uploads_step(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        uploads: &DatasetUploads,
        progress: &mut SnapshotUploadCleanup,
    ) -> Result<SnapshotUploadCleanup, UploadReclaimError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.retire_snapshot_upload_window(identity, snapshot, uploads, progress),
        )
        .await
        .map_err(|_| UploadReclaimError::Limit)?
    }
    async fn retire_snapshot_upload_window(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        uploads: &DatasetUploads,
        progress: &mut SnapshotUploadCleanup,
    ) -> Result<SnapshotUploadCleanup, UploadReclaimError> {
        // Bound the retained record before the reclaim protocol reads it again.
        self.store
            .get_bounded(
                &crate::key::snapshot(identity, snapshot).map_err(AuthorityError::from)?,
                budget(9 * 1024 * 1024),
                &ReadCancellation::new(),
            )
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let record = self.begin_reclaim(identity, snapshot).await?;
        self.require_reclaim_ready(identity, snapshot).await?;
        if record.publication.manifest.len() > 8 * 1024 * 1024 {
            return Err(UploadReclaimError::Limit);
        }
        let manifest: ManifestRecord =
            bincode::deserialize(&record.publication.manifest).map_err(|_| UploadReclaimError::Corrupt)?;
        manifest.validate().map_err(AuthorityError::from)?;
        let target = (identity.clone(), snapshot);
        if progress.target.as_ref().is_some_and(|bound| bound != &target) {
            return Err(UploadReclaimError::Corrupt);
        }
        if progress.target.is_none() && progress != &SnapshotUploadCleanup::default() {
            return Err(UploadReclaimError::Corrupt);
        }
        progress.target = Some(target);
        validate(&manifest, progress)?;
        if progress.complete {
            return Ok(progress.clone());
        }
        for _ in 0..20 {
            let Some(sample) = manifest.samples.get(progress.sample) else {
                progress.complete = true;
                break;
            };
            if let Some(field) = sample.fields.get(progress.field) {
                if let FieldLocator::Chunk { location, .. } = &field.value {
                    if DatasetUploads::upload_id(identity, location).is_ok() {
                        uploads
                            .retire_owner(location, record.publication.operation)
                            .await?;
                        progress.retired_references = progress
                            .retired_references
                            .checked_add(1)
                            .ok_or(UploadReclaimError::Corrupt)?;
                    }
                }
                progress.field += 1;
            } else {
                progress.sample += 1;
                progress.field = 0;
            }
        }
        Ok(progress.clone())
    }
}
fn budget(bytes: usize) -> MetadataScanBudget {
    MetadataScanBudget {
        items: 1,
        bytes,
        timeout: Duration::from_secs(2),
    }
}
fn validate(manifest: &ManifestRecord, progress: &SnapshotUploadCleanup) -> Result<(), UploadReclaimError> {
    if progress.version != 1
        || progress.sample > manifest.samples.len()
        || manifest
            .samples
            .get(progress.sample)
            .map_or(progress.field != 0, |sample| progress.field > sample.fields.len())
        || (progress.complete && progress.sample != manifest.samples.len())
    {
        return Err(UploadReclaimError::Corrupt);
    }
    Ok(())
}
