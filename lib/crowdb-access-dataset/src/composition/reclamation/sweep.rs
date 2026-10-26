// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::CompositionReclaimCursor;
use crate::{
    composition::CompositionError, DatasetAuthority, DatasetUploads, MetadataScan, MetadataScanBudget,
    ReadCancellation,
};
use std::time::Duration;

const DISCOVERY_ITEMS: usize = 8;
const QUEUE_LIMIT: usize = 32;

/// Bounded local work hints rediscovered from historical draft resource facts.
#[derive(Debug, Default)]
pub struct CompositionReclaimSweep {
    after: Option<Vec<u8>>,
    pending: crate::retirement_queue::RetirementQueue<CompositionReclaimCursor, QUEUE_LIMIT, DISCOVERY_ITEMS>,
    exhausted: bool,
}
impl CompositionReclaimSweep {
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
    /// Discovers historical Composition drafts and advances one local cleanup window.
    /// # Errors
    /// Returns corrupt discovery keys, storage failures, cancellation and work limits.
    pub async fn reclaim_compositions_page(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut CompositionReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), CompositionError> {
        if let Ok(result) = tokio::time::timeout(
            Duration::from_secs(2),
            self.reclaim_composition_window(uploads, cursor, cancel),
        )
        .await
        {
            result
        } else {
            cursor.pending.timeout();
            Err(CompositionError::Limit)
        }
    }
    async fn reclaim_composition_window(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut CompositionReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), CompositionError> {
        if cursor.exhausted && (cursor.pending.is_empty() || cursor.pending.needs_rescan()) {
            cursor.exhausted = false;
            cursor.pending.restarting_scan();
        }
        if !cursor.exhausted && cursor.pending.can_discover() {
            self.discover_compositions(cursor, cancel).await?;
        }
        let Some(work) = cursor.pending.begin() else {
            return Ok(());
        };
        let result = self.advance_composition_reclaim(uploads, work, cancel).await;
        cursor
            .pending
            .finish(result.as_ref().is_ok_and(|complete| *complete), result.is_err());
        result.map(|_| ())
    }
    async fn discover_compositions(
        &self,
        cursor: &mut CompositionReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), CompositionError> {
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: b"DS\0CD\0".to_vec(),
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
        for (key, _) in &page.entries {
            cursor.after = Some(key.clone());
            let (identity, operation) = crate::key::decode_scoped_operation(key, b"DS\0CD\0")?;
            if !cursor
                .pending
                .iter()
                .any(|work| work.identity == identity && work.operation == operation)
            {
                cursor
                    .pending
                    .push(CompositionReclaimCursor::new(identity, operation));
            }
        }
        cursor.after = page.after;
        cursor.exhausted = cursor.after.is_none();
        Ok(())
    }
}
