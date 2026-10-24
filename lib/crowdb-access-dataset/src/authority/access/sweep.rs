// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::decode_access;
use crate::{
    key, AuthorityError, DatasetAuthority, DatasetUploads, MetadataScan, MetadataScanBudget,
    ReadCancellation, SnapshotReclaimError,
};
use std::time::Duration;

impl DatasetAuthority {
    /// Advances one bounded page of durable deletion marks. The cursor is only
    /// a scan optimization; restarting from the beginning loses no cleanup work.
    /// On an item failure the cursor advances past it, so unrelated deletions
    /// can progress; the failed durable mark is retried on the next scan cycle.
    /// # Errors
    /// Returns storage, corrupt marker, cancellation or cleanup failures.
    pub async fn reclaim_due_page(
        &self,
        uploads: &DatasetUploads,
        after: &mut Option<Vec<u8>>,
        cancel: &ReadCancellation,
    ) -> Result<(), SnapshotReclaimError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.reclaim_due_window(uploads, after, cancel),
        )
        .await
        .map_err(|_| SnapshotReclaimError::Limit)?
    }

    async fn reclaim_due_window(
        &self,
        uploads: &DatasetUploads,
        after: &mut Option<Vec<u8>>,
        cancel: &ReadCancellation,
    ) -> Result<(), SnapshotReclaimError> {
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: key::LEASE_PREFIX.to_vec(),
                    after: after.clone(),
                    budget: MetadataScanBudget {
                        items: 20,
                        bytes: 512 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                },
                cancel,
            )
            .await?;
        for (key, value) in &page.entries {
            if cancel.is_cancelled() {
                return Err(AuthorityError::from(crate::DatasetError::ReadCancelled).into());
            }
            *after = Some(key.clone());
            let access = decode_access(&value.bytes)?;
            if !access.retiring || access.cleanup_complete || access.reclaim_after > self.reclaim_time() {
                continue;
            }
            let Some((identity, snapshot)) = access.target else {
                continue;
            };
            if crate::key::read_lease(&identity, snapshot).map_err(AuthorityError::from)? != *key {
                return Err(AuthorityError::Corrupt.into());
            }
            self.reclaim_snapshot_step(&identity, snapshot, uploads, cancel)
                .await?;
        }
        *after = page.after;
        Ok(())
    }
}
