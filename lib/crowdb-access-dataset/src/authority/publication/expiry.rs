// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::candidate::{decode_candidate, CANDIDATE_PREFIX};
use crate::{
    authority::decode_head, key, AuthorityError, DatasetAuthority, DatasetIdentity, MetadataScan,
    MetadataScanBudget, ReadCancellation, SnapshotId,
};
use std::time::Duration;

/// Discardable discovery position; immutable resource deadlines survive restart.
#[derive(Default, Debug)]
pub struct SnapshotExpirySweep {
    after: Option<Vec<u8>>,
}
impl SnapshotExpirySweep {
    #[must_use]
    pub fn has_more(&self) -> bool {
        self.after.is_some()
    }
}

impl DatasetAuthority {
    /// Marks expired content bindings for ordinary delayed reclamation.
    /// Heads, retained resources and live ancestry remain protected.
    /// # Errors
    /// Returns malformed resources, cancellation, bounded-work or storage failures.
    pub async fn expire_snapshot_candidates_page(
        &self,
        cursor: &mut SnapshotExpirySweep,
        cancel: &ReadCancellation,
    ) -> Result<(), AuthorityError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.expire_candidates_window(cursor, cancel),
        )
        .await
        .map_err(|_| crate::DatasetError::ReadLimitExceeded)?
    }

    async fn expire_candidates_window(
        &self,
        cursor: &mut SnapshotExpirySweep,
        cancel: &ReadCancellation,
    ) -> Result<(), AuthorityError> {
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: CANDIDATE_PREFIX.to_vec(),
                    after: cursor.after.clone(),
                    budget: MetadataScanBudget {
                        items: 20,
                        bytes: 512 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                },
                cancel,
            )
            .await?;
        for (key, value) in page.entries {
            if cancel.is_cancelled() {
                return Err(crate::DatasetError::ReadCancelled.into());
            }
            // A corrupt or slow resource cannot permanently pin discovery before healthy rows.
            cursor.after = Some(key.clone());
            let (identity, operation) = key::decode_scoped_operation(&key, CANDIDATE_PREFIX)?;
            let snapshot = SnapshotId::from_bytes(operation.as_bytes())?;
            if value.bytes == super::complete::RETIRED {
                continue;
            }
            let candidate = decode_candidate(&value.bytes, snapshot)?;
            // Both modern bindings follow all immutable metadata. A staged binding
            // can outlive a failed dependency/owner step without ever becoming complete.
            if !matches!(candidate.version, 2 | 3)
                || !candidate
                    .expires_at
                    .is_some_and(|time| time <= self.reclaim_time())
            {
                continue;
            }
            if self.expired_snapshot_protected(&identity, snapshot).await? {
                continue;
            }
            match self.begin_reclaim(&identity, snapshot).await {
                Ok(_) | Err(AuthorityError::SnapshotProtected) => {}
                Err(error) => return Err(error),
            }
        }
        cursor.after = page.after;
        Ok(())
    }

    async fn expired_snapshot_protected(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<bool, AuthorityError> {
        // Ordinary reclaim discovery already owns marked resources. Avoid renewing their grace.
        match self.snapshot_reclaim_after(identity, snapshot).await {
            Ok(_) => return Ok(true),
            Err(AuthorityError::SnapshotProtected) => {}
            Err(error) => return Err(error),
        }
        let head = self.store.get(&key::head(identity)?).await?;
        let head = head.as_ref().map(|value| decode_head(&value.bytes)).transpose()?;
        Ok(
            head.is_some_and(|head| head.latest == Some(snapshot) || head.stable == Some(snapshot))
                || self
                    .store
                    .get(&key::retention(identity, snapshot)?)
                    .await?
                    .is_some()
                || self.snapshot_reachable_from_head(identity, snapshot).await?
                || self.has_live_snapshot_children(identity, snapshot).await?
                || self.legacy_live_descendant(identity, snapshot).await?,
        )
    }
}
