// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key,
    preparation::{budget, decode_preparation, summary_progress},
    InspectionError, InspectionPreparation, PreparationState, SnapshotSummary,
};
use crate::{AuthorityError, DatasetAuthority, DatasetIdentity, ReadCancellation, SnapshotId};

impl DatasetAuthority {
    /// Reads preparation status only when bounded metadata proves a live commit.
    /// # Errors
    /// Returns unavailable publication, unprepared metadata, corruption or read limits.
    pub async fn committed_inspection_preparation_status(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPreparation, InspectionError> {
        let mut work = super::work::InspectionWork::new(self.store.as_ref(), cancel, budget(1024 * 1024))?;
        if !self
            .inspection_snapshot_committed(identity, snapshot, &mut work)
            .await?
        {
            return Err(AuthorityError::SnapshotNotFound.into());
        }
        self.inspection_preparation_status(identity, snapshot, cancel)
            .await
    }
    /// Reads preparation state without loading the publication or Manifest.
    /// # Errors
    /// Returns not-prepared, corrupt, unavailable or over-budget storage errors.
    pub async fn inspection_preparation_status(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPreparation, InspectionError> {
        let stored = self
            .store
            .get_bounded(
                &key::scope(identity, snapshot, b'P').map_err(AuthorityError::from)?,
                budget(16 * 1024),
                cancel,
            )
            .await?;
        if let Some(stored) = stored {
            let record = decode_preparation(&stored.bytes, snapshot)?;
            if record.progress.state == PreparationState::Ready
                && self
                    .read_inspection_summary(identity, snapshot, cancel)
                    .await?
                    .is_none()
            {
                return Err(InspectionError::Corrupt);
            }
            return Ok(record.progress);
        }
        self.read_inspection_summary(identity, snapshot, cancel)
            .await?
            .as_ref()
            .map(summary_progress)
            .ok_or(InspectionError::NotPrepared)
    }

    pub(super) async fn read_inspection_summary(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        cancel: &ReadCancellation,
    ) -> Result<Option<SnapshotSummary>, InspectionError> {
        let stored = self
            .store
            .get_bounded(
                &key::scope(identity, snapshot, b'H').map_err(AuthorityError::from)?,
                budget(16 * 1024),
                cancel,
            )
            .await?;
        stored
            .map(|stored| {
                let summary: SnapshotSummary =
                    serde_json::from_slice(&stored.bytes).map_err(|_| InspectionError::Corrupt)?;
                if summary.version != 1 || summary.snapshot != snapshot {
                    return Err(InspectionError::Corrupt);
                }
                Ok(summary)
            })
            .transpose()
    }
}
