// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{super::head::head_attempt, legacy::visibility_key, receipt::snapshot_commit_key};
use crate::{
    inspection::{work::InspectionWork, InspectionError},
    CommitTarget, DatasetAuthority, DatasetIdentity, SnapshotId, SuccessfulCommit,
};

impl DatasetAuthority {
    pub(crate) async fn inspection_snapshot_committed(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, InspectionError> {
        use crate::AuthorityError;
        if let Some(value) = work
            .get(
                &crate::key::read_lease(identity, snapshot).map_err(AuthorityError::from)?,
                256 * 1024,
            )
            .await?
        {
            if super::super::access::is_reclaiming(&value.bytes)? {
                return Ok(false);
            }
        }
        if let Some(value) = work
            .get(
                &snapshot_commit_key(identity, snapshot).map_err(AuthorityError::from)?,
                8192,
            )
            .await?
        {
            let receipt = SuccessfulCommit::decode(&value.bytes)?;
            if receipt.attempt.target != (CommitTarget::Snapshot { snapshot }) {
                return Err(InspectionError::Corrupt);
            }
            return Ok(true);
        }
        if let Some(value) = work
            .get(&crate::key::head(identity).map_err(AuthorityError::from)?, 8192)
            .await?
        {
            if head_attempt(&value.bytes)?.is_some_and(|a| a.target == (CommitTarget::Snapshot { snapshot }))
            {
                return Ok(true);
            }
        }
        if let Some(value) = work
            .get(
                &visibility_key(identity, snapshot).map_err(AuthorityError::from)?,
                2048,
            )
            .await?
        {
            super::legacy::validate_visibility(&value.bytes, snapshot)?;
            return Ok(true);
        }
        Ok(false)
    }
}
