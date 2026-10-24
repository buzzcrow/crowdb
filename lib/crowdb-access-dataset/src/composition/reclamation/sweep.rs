// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{target, CompositionReclaimCursor};
use crate::{
    composition::CompositionError, DatasetAuthority, DatasetUploads, MetadataScan, MetadataScanBudget,
    ReadCancellation,
};
use std::{collections::VecDeque, time::Duration};

const DISCOVERY_ITEMS: usize = 8;
const QUEUE_LIMIT: usize = 32;

/// Bounded local work hints rediscovered from historical draft resource facts.
#[derive(Debug, Default)]
pub struct CompositionReclaimSweep {
    after: Option<Vec<u8>>,
    pending: VecDeque<CompositionReclaimCursor>,
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
            if cursor.pending.len() > 1 {
                cursor.pending.rotate_left(1);
            }
            Err(CompositionError::Limit)
        }
    }
    async fn reclaim_composition_window(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut CompositionReclaimSweep,
        cancel: &ReadCancellation,
    ) -> Result<(), CompositionError> {
        if cursor.exhausted && cursor.pending.is_empty() {
            cursor.exhausted = false;
        }
        if !cursor.exhausted && cursor.pending.len() <= QUEUE_LIMIT - DISCOVERY_ITEMS {
            self.discover_compositions(cursor, cancel).await?;
        }
        let Some(work) = cursor.pending.front_mut() else {
            return Ok(());
        };
        let result = self.advance_composition_reclaim(uploads, work, cancel).await;
        if result.as_ref().is_ok_and(|complete| *complete) {
            cursor.pending.pop_front();
        } else {
            cursor.pending.rotate_left(1);
        }
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
            let (identity, operation) = target::decode(key)?;
            if !cursor
                .pending
                .iter()
                .any(|work| work.identity == identity && work.operation == operation)
            {
                cursor
                    .pending
                    .push_back(CompositionReclaimCursor::new(identity, operation));
            }
        }
        cursor.after = page.after;
        cursor.exhausted = cursor.after.is_none();
        Ok(())
    }
}
