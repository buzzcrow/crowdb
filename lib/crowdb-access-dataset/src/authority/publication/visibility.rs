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
        self.snapshot_has_commit_proof(identity, snapshot, work).await
    }
    pub(crate) async fn snapshot_has_commit_proof(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, InspectionError> {
        use crate::AuthorityError;
        let candidate = work
            .get(
                &super::candidate::candidate_key(identity, snapshot).map_err(AuthorityError::from)?,
                4096,
            )
            .await?;
        if let Some(candidate) = &candidate {
            if candidate.bytes == super::complete::RETIRED {
                return Ok(false);
            }
            let candidate = super::candidate::decode_candidate(&candidate.bytes, snapshot)?;
            if candidate.version == 2 {
                return Ok(true);
            }
            if candidate.version == 3 {
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
        let head = work
            .get(&crate::key::head(identity).map_err(AuthorityError::from)?, 8192)
            .await?;
        if let Some(value) = &head {
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
        // A prepared modern candidate is not a legacy publication, even when
        // a malformed old head happens to name it.
        if let Some(candidate) = candidate {
            return self
                .legacy_candidate_available(
                    identity,
                    &super::candidate::decode_candidate(&candidate.bytes, snapshot)?,
                    work,
                )
                .await;
        }
        Ok(self
            .legacy_snapshot_visible(identity, snapshot, head.as_ref(), work)
            .await?)
    }
}

impl DatasetAuthority {
    pub(crate) async fn inspection_snapshot_operation(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        work: &mut InspectionWork<'_>,
    ) -> Result<Option<crate::OperationId>, InspectionError> {
        let Some(value) = work
            .get(
                &super::candidate::candidate_key(identity, snapshot).map_err(crate::AuthorityError::from)?,
                4096,
            )
            .await?
        else {
            return Ok(None);
        };
        if value.bytes == super::complete::RETIRED {
            return Ok(None);
        }
        let binding = super::candidate::decode_candidate(&value.bytes, snapshot)?;
        Ok(Some(binding.operation))
    }
}
