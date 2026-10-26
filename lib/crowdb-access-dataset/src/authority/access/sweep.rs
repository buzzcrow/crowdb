// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode_access, SnapshotReclaimCursor};
use crate::{
    key, AuthorityError, DatasetAuthority, DatasetUploads, MetadataScan, MetadataScanBudget,
    ReadCancellation, SnapshotReclaimError,
};
use std::time::Duration;

const QUEUE_LIMIT: usize = 32;
const DISCOVERY_ITEMS: usize = 20;

/// Bounded worker-local hints. Durable deletion marks rediscover every lost hint.
#[derive(Default, Debug)]
pub struct SnapshotReclaimSweep {
    after: Option<Vec<u8>>,
    pending: crate::retirement_queue::RetirementQueue<SnapshotReclaimCursor, QUEUE_LIMIT, DISCOVERY_ITEMS>,
    legacy: super::legacy_progress::LegacyProgressSweep,
}
impl SnapshotReclaimSweep {
    #[must_use]
    pub fn has_more(&self) -> bool {
        self.after.is_some() || !self.pending.is_empty() || self.legacy.has_more()
    }
    #[must_use]
    pub fn pending_snapshots(&self) -> usize {
        self.pending.len()
    }
}

impl DatasetAuthority {
    /// Discovers bounded durable deletion marks and advances one local cleanup hint.
    /// A failed or interrupted window retains its hint; restart rescans the marks.
    /// # Errors
    /// Returns storage, corrupt marker, cancellation or cleanup failures.
    pub async fn reclaim_due_page(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut SnapshotReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SnapshotReclaimError> {
        if let Ok(result) = tokio::time::timeout(
            Duration::from_secs(2),
            self.reclaim_due_window(uploads, cursor, cancel),
        )
        .await
        {
            result
        } else {
            cursor.pending.timeout();
            Err(SnapshotReclaimError::Limit)
        }
    }

    async fn reclaim_due_window(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut SnapshotReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SnapshotReclaimError> {
        if cursor.pending.can_discover() {
            self.discover_reclaims(cursor, cancel).await?;
        }
        if let Some(work) = cursor.pending.begin() {
            let result = self.advance_snapshot_reclaim(uploads, work, cancel).await;
            cursor.pending.finish(
                result.as_ref().is_ok_and(|status| status.complete),
                result.is_err(),
            );
            result?;
        }
        self.sweep_legacy_progress(&mut cursor.legacy, cancel).await
    }

    async fn discover_reclaims(
        &self,
        cursor: &mut SnapshotReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SnapshotReclaimError> {
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: key::LEASE_PREFIX.to_vec(),
                    after: cursor.after.clone(),
                    budget: MetadataScanBudget {
                        items: DISCOVERY_ITEMS,
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
            cursor.after = Some(key.clone());
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
            if !cursor
                .pending
                .iter()
                .any(|work| work.identity == identity && work.snapshot == snapshot)
            {
                cursor
                    .pending
                    .push(SnapshotReclaimCursor::new(identity, snapshot));
            }
        }
        cursor.after = page.after;
        Ok(())
    }
}
