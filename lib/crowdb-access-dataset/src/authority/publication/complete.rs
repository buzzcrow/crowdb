// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::candidate::{candidate_key, decode_candidate};
use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, MetadataScanBudget, ReadCancellation,
    SnapshotId,
};
use std::time::Duration;

pub(crate) const RETIRED: &[u8] = b"retired-snapshot-content-v1";

impl DatasetAuthority {
    /// Seals the content root after the durable Snapshot deletion fence exists.
    /// Missing roots are sealed too, preventing an interrupted preparer from creating one later.
    pub(in crate::authority) async fn retire_snapshot_binding(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        let key = candidate_key(identity, snapshot)?;
        let mut observed = self.store.get(&key).await?;
        for _ in 0..64 {
            if let Some(value) = &observed {
                if value.bytes == RETIRED {
                    return Ok(());
                }
                decode_candidate(&value.bytes, snapshot)?;
            }
            match self
                .store
                .compare_exchange(
                    &key,
                    observed.as_ref().map(|value| value.bytes.as_slice()),
                    RETIRED,
                )
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(value) => observed = value,
            }
        }
        Err(AuthorityError::HeadConflict)
    }

    /// Installs the complete immutable resource root after payload ownership is acquired.
    /// It does not change latest/stable or create a publication receipt.
    /// # Errors
    /// Rejects incomplete content, unavailable parents, deletion and cleanup races.
    pub async fn complete_snapshot_candidate(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        let key = candidate_key(identity, snapshot)?;
        let stored = self
            .store
            .get_bounded(
                &key,
                MetadataScanBudget {
                    items: 1,
                    bytes: 4096,
                    timeout: Duration::from_secs(2),
                },
                &ReadCancellation::new(),
            )
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let mut candidate = decode_candidate(&stored.bytes, snapshot)?;
        self.ensure_snapshot_live(identity, snapshot).await?;
        if candidate.version == 2 {
            return Ok(());
        }
        if let Some(parent) = candidate.parent {
            self.require_committed_snapshot(identity, parent).await?;
        }
        self.validate_initial_split(identity, snapshot, candidate.initial_split)
            .await?;
        self.mark_candidate_published(identity, &candidate).await?;
        candidate.version = 2;
        let complete = serde_json::to_vec(&candidate).map_err(|_| AuthorityError::Corrupt)?;
        match self
            .store
            .compare_exchange(&key, Some(&stored.bytes), &complete)
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(observed))
                if decode_candidate(&observed.bytes, snapshot)? == candidate =>
            {
                Ok(())
            }
            CasOutcome::Conflict(_) => Err(AuthorityError::HeadConflict),
        }
    }

    /// Competes with finalization on the resource root before retiring any content.
    /// Returns true when the complete resource must survive draft cleanup.
    pub(crate) async fn seal_incomplete_snapshot(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<bool, AuthorityError> {
        let key = candidate_key(identity, snapshot)?;
        let mut observed = self.store.get(&key).await?;
        for _ in 0..64 {
            if let Some(value) = &observed {
                if value.bytes == RETIRED {
                    return Ok(false);
                }
                let candidate = decode_candidate(&value.bytes, snapshot)?;
                if candidate.version == 2 {
                    return Ok(true);
                }
                if self.legacy_candidate_ready(identity, &candidate).await? {
                    return Ok(true);
                }
            } else if self.snapshot_commit(identity, snapshot).await?.is_some() {
                return Ok(true);
            }
            match self
                .store
                .compare_exchange(&key, observed.as_ref().map(|v| v.bytes.as_slice()), RETIRED)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(false),
                CasOutcome::Conflict(value) => observed = value,
            }
        }
        Err(AuthorityError::HeadConflict)
    }
}
