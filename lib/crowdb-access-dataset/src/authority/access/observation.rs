// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::decode_access;
use crate::{
    authority::{decode, decode_head},
    inspection::{work::InspectionWork, InspectionError},
    key, AuthorityError, DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId,
    ReadCancellation, ReclaimProgress, ReclaimState, SnapshotId,
};
use std::time::Duration;

/// Observed protection and retirement facts, never permission to reclaim.
#[derive(Clone, Debug, Eq, PartialEq)]
// Independent protection facts may coexist; they are not exclusive states.
#[allow(clippy::struct_excessive_bools)]
pub struct SnapshotLifecycle {
    pub snapshot: SnapshotId,
    pub operation: Option<OperationId>,
    pub latest: bool,
    pub stable: bool,
    pub retained: bool,
    pub active_read_lease: bool,
    pub reclaim_after: Option<u64>,
    pub retiring: bool,
    pub completed: bool,
}
impl DatasetAuthority {
    /// Reads only small control records under one aggregate metadata deadline.
    /// # Errors
    /// Returns missing publication proof, corrupt records, storage failures or limits.
    pub async fn inspect_snapshot_lifecycle(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotLifecycle, InspectionError> {
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 1,
                bytes: 512 * 1024,
                timeout: Duration::from_secs(5),
            },
        )?;
        let progress = work
            .get(
                &key::reclaim_progress(identity, snapshot).map_err(AuthorityError::from)?,
                2048,
            )
            .await?;
        let progress = progress
            .as_ref()
            .map(|value| decode::<ReclaimProgress>(&value.bytes))
            .transpose()?;
        if progress.as_ref().is_some_and(|value| value.snapshot != snapshot) {
            return Err(InspectionError::Corrupt);
        }
        let completed = progress.is_some_and(|value| value.state == ReclaimState::Completed);
        let access = work
            .get(
                &key::read_lease(identity, snapshot).map_err(AuthorityError::from)?,
                256 * 1024,
            )
            .await?;
        let access = access
            .as_ref()
            .map(|value| decode_access(&value.bytes))
            .transpose()?
            .unwrap_or_default();
        let completed = if access.target.is_some() {
            access.cleanup_complete
        } else {
            completed
        };
        if !completed
            && !access.retiring
            && !self
                .snapshot_has_commit_proof(identity, snapshot, &mut work)
                .await?
        {
            return Err(AuthorityError::SnapshotNotFound.into());
        }
        let head = work
            .get(&key::head(identity).map_err(AuthorityError::from)?, 8192)
            .await?;
        let head = head.as_ref().map(|value| decode_head(&value.bytes)).transpose()?;
        let retained = work
            .get(
                &key::retention(identity, snapshot).map_err(AuthorityError::from)?,
                2048,
            )
            .await?
            .is_some();
        let operation = self
            .inspection_snapshot_operation(identity, snapshot, &mut work)
            .await?;
        Ok(SnapshotLifecycle {
            snapshot,
            operation,
            latest: head.as_ref().is_some_and(|head| head.latest == Some(snapshot)),
            stable: head.as_ref().is_some_and(|head| head.stable == Some(snapshot)),
            retained,
            active_read_lease: false,
            reclaim_after: access.retiring.then_some(access.reclaim_after),
            retiring: access.reclaiming,
            completed,
        })
    }
}
