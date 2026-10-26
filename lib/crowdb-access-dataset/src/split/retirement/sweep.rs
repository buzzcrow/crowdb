// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::SplitRevisionReclaimCursor;
use crate::{
    split::SplitLifecycleError, AuthorityError, DatasetAuthority, MetadataScan, MetadataScanBudget,
    ReadCancellation,
};
use std::time::Duration;

const DISCOVERY: usize = 8;

/// Local SR discovery and fair work queue; permanent resource roots survive restart.
#[derive(Debug, Default)]
pub struct SplitRevisionReclaimSweep {
    after: Option<Vec<u8>>,
    pending: crate::retirement_queue::RetirementQueue<SplitRevisionReclaimCursor, 32, DISCOVERY>,
    exhausted: bool,
}
impl SplitRevisionReclaimSweep {
    #[must_use]
    pub fn has_more(&self) -> bool {
        self.after.is_some() || !self.pending.is_empty()
    }
}
impl DatasetAuthority {
    /// Discovers deletion marks and late output belonging to already retired owners.
    /// # Errors
    /// Returns malformed discovery keys, cancellation, limits and storage failures.
    pub async fn reclaim_split_revisions_page(
        &self,
        cursor: &mut SplitRevisionReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SplitLifecycleError> {
        if let Ok(result) = tokio::time::timeout(
            Duration::from_secs(2),
            self.reclaim_revision_window(cursor, cancel),
        )
        .await
        {
            result
        } else {
            cursor.pending.timeout();
            Err(SplitLifecycleError::Limit)
        }
    }
    async fn reclaim_revision_window(
        &self,
        cursor: &mut SplitRevisionReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), SplitLifecycleError> {
        if cursor.exhausted && (cursor.pending.is_empty() || cursor.pending.needs_rescan()) {
            cursor.exhausted = false;
            cursor.pending.restarting_scan();
        }
        if !cursor.exhausted && cursor.pending.can_discover() {
            let page = self
                .store
                .scan_page(
                    &MetadataScan {
                        prefix: b"DS\0SR\0".to_vec(),
                        after: cursor.after.clone(),
                        budget: MetadataScanBudget {
                            items: DISCOVERY,
                            bytes: 9 * 1024 * 1024,
                            timeout: Duration::from_secs(2),
                        },
                    },
                    cancel,
                )
                .await?;
            for (key, _) in page.entries {
                cursor.after = Some(key.clone());
                let (identity, snapshot, revision) = crate::split::key::revision_identity(&key)?;
                if !cursor.pending.iter().any(|work| {
                    work.identity == identity && work.snapshot == snapshot && work.revision == revision
                }) {
                    cursor
                        .pending
                        .push(SplitRevisionReclaimCursor::new(identity, snapshot, revision));
                }
            }
            cursor.after = page.after;
            cursor.exhausted = cursor.after.is_none();
        }
        let Some(work) = cursor.pending.begin() else {
            return Ok(());
        };
        let result = self.advance_split_revision_reclaim(work, cancel).await;
        let waiting = work.waiting;
        let skipped = matches!(
            &result,
            Err(SplitLifecycleError::Frozen
                | SplitLifecycleError::NotFound
                | SplitLifecycleError::Authority(
                    AuthorityError::SnapshotProtected | AuthorityError::SnapshotNotFound
                ))
        );
        cursor.pending.finish(
            result.as_ref().is_ok_and(|value| value.complete) || waiting || skipped,
            result.is_err() && !skipped,
        );
        if skipped {
            Ok(())
        } else {
            result.map(|_| ())
        }
    }
}
