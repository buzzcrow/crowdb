// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{cleanup::SplitReclaimCursor, SplitLifecycleError};
use crate::{DatasetAuthority, MetadataScan, MetadataScanBudget, ReadCancellation};
use std::time::Duration;

const DISCOVERY_ITEMS: usize = 8;
const QUEUE_LIMIT: usize = 32;

/// Bounded local work hints rediscovered from historical draft resource facts.
#[derive(Debug, Default)]
pub struct SplitReclaimSweep {
    after: Option<Vec<u8>>,
    pending: crate::retirement_queue::RetirementQueue<SplitReclaimCursor, QUEUE_LIMIT, DISCOVERY_ITEMS>,
    exhausted: bool,
}
impl SplitReclaimSweep {
    #[must_use]
    pub fn has_more(&self) -> bool {
        self.after.is_some() || !self.pending.is_empty()
    }
    #[must_use]
    pub fn pending_drafts(&self) -> usize {
        self.pending.len()
    }
}
impl DatasetAuthority {
    /// Discovers historical Split drafts and advances one local cleanup window.
    /// # Errors
    /// Returns corrupt discovery keys, storage failures, cancellation and work limits.
    pub async fn reclaim_splits_page(
        &self,
        cursor: &mut SplitReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SplitLifecycleError> {
        if let Ok(result) =
            tokio::time::timeout(Duration::from_secs(2), self.reclaim_split_window(cursor, cancel)).await
        {
            result
        } else {
            cursor.pending.timeout();
            Err(SplitLifecycleError::Limit)
        }
    }
    async fn reclaim_split_window(
        &self,
        cursor: &mut SplitReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SplitLifecycleError> {
        if cursor.exhausted && (cursor.pending.is_empty() || cursor.pending.needs_rescan()) {
            cursor.exhausted = false;
            cursor.pending.restarting_scan();
        }
        if !cursor.exhausted && cursor.pending.can_discover() {
            self.discover_splits(cursor, cancel).await?;
        }
        let Some(work) = cursor.pending.begin() else {
            return Ok(());
        };
        let result = self.advance_split_reclaim(work, cancel).await;
        let skipped = matches!(
            result,
            Err(SplitLifecycleError::Frozen
                | SplitLifecycleError::Authority(
                    crate::AuthorityError::ReclaimPending { .. } | crate::AuthorityError::SnapshotProtected
                ))
        );
        cursor.pending.finish(
            result.as_ref().is_ok_and(|progress| progress.complete) || skipped,
            result.is_err() && !skipped,
        );
        match result {
            Err(
                SplitLifecycleError::Frozen
                | SplitLifecycleError::Authority(
                    crate::AuthorityError::ReclaimPending { .. } | crate::AuthorityError::SnapshotProtected,
                ),
            ) => Ok(()),
            other => other.map(|_| ()),
        }
    }
    async fn discover_splits(
        &self,
        cursor: &mut SplitReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SplitLifecycleError> {
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: b"DS\0SD\0".to_vec(),
                    after: cursor.after.clone(),
                    budget: MetadataScanBudget {
                        items: DISCOVERY_ITEMS,
                        bytes: 9 * 1024 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                },
                cancel,
            )
            .await?;
        for (key, value) in &page.entries {
            cursor.after = Some(key.clone());
            if value.bytes == super::cleanup::SEALED {
                continue;
            }
            let (identity, operation) = crate::key::decode_scoped_operation(key, b"DS\0SD\0")?;
            if !cursor
                .pending
                .iter()
                .any(|work| work.identity == identity && work.operation == operation)
            {
                cursor.pending.push(SplitReclaimCursor::new(identity, operation));
            }
        }
        cursor.after = page.after;
        cursor.exhausted = cursor.after.is_none();
        Ok(())
    }
}
