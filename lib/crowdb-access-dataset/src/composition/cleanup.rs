// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{evaluation::Step, key, state::CompositionState, CompositionError, DraftProgress};
use crate::{
    inspection::work::InspectionWork, CasOutcome, DatasetAuthority, DatasetIdentity, MetadataScanBudget,
    OperationId, ReadCancellation,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Keep small negative fences: deleting a key would let an already-issued CAS(None)
// recreate an obsolete work page or row after cleanup completed.
const SEALED: &[u8] = b"composition-retired-v1";
const END: &[u8] = b"composition-retired-end-v1";
const RECORD_CAP: usize = 1024 * 1024 + 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompositionScratchCleanup {
    pub input_revision: u64,
    pub next_page: u64,
    pub complete: bool,
}
#[derive(Clone, Debug)]
pub(super) struct ScratchCursor {
    revision: u64,
    page: u64,
}
impl Default for ScratchCursor {
    fn default() -> Self {
        Self { revision: 1, page: 0 }
    }
}
impl DatasetAuthority {
    /// Retires one obsolete evaluation window, retaining small CAS fences.
    /// Active input revisions remain readable; terminal drafts retire all revisions.
    /// # Errors
    /// Returns corrupt cursors, cancellation, bounded-work limits and storage conflicts.
    pub(super) async fn cleanup_composition_scratch_step(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cursor: &mut ScratchCursor,
        cancel: &ReadCancellation,
    ) -> Result<CompositionScratchCleanup, CompositionError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.cleanup_scratch_window(identity, operation, cursor, cancel),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }
    async fn cleanup_scratch_window(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cursor: &mut ScratchCursor,
        cancel: &ReadCancellation,
    ) -> Result<CompositionScratchCleanup, CompositionError> {
        let (_, mut state) = self.load_composition(identity, operation).await?;
        let end = state.draft.record.revision
            + u64::from(matches!(
                state.draft.record.progress,
                DraftProgress::Prepared { .. }
                    | DraftProgress::Committing { .. }
                    | DraftProgress::Published { .. }
                    | DraftProgress::Abandoned
            ));
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 20,
                bytes: 14 * 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )?;
        if cursor.revision == 0 || cursor.revision > end {
            return Err(CompositionError::Corrupt);
        }
        if cursor.revision < end {
            state.draft.record.revision = cursor.revision;
            let page_key = key::page(b"DS\0CW\0", identity, operation, cursor.revision, cursor.page)?;
            if self
                .seal_scratch_page(identity, &state, &page_key, &mut work)
                .await?
            {
                cursor.revision += 1;
                cursor.page = 0;
            } else {
                cursor.page = cursor.page.checked_add(1).ok_or(CompositionError::Limit)?;
            }
        }
        Ok(CompositionScratchCleanup {
            input_revision: cursor.revision,
            next_page: cursor.page,
            complete: cursor.revision == end,
        })
    }
    async fn seal_scratch_page(
        &self,
        identity: &DatasetIdentity,
        state: &CompositionState,
        page_key: &[u8],
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, CompositionError> {
        let stored = work.get(page_key, RECORD_CAP).await?;
        let Some(stored) = stored else {
            return match self.store.compare_exchange(page_key, None, END).await? {
                CasOutcome::Applied(_) => Ok(true),
                CasOutcome::Conflict(Some(v)) if v.bytes == END => Ok(true),
                CasOutcome::Conflict(_) => Err(CompositionError::Conflict),
            };
        };
        if stored.bytes == END {
            return Ok(true);
        }
        if stored.bytes == SEALED {
            return Ok(false);
        }
        let step: Step = crate::split::codec::decode(&stored.bytes)?;
        if step.next.preview.input_revision != state.draft.record.revision || step.rows.len() > 20 {
            return Err(CompositionError::Corrupt);
        }
        for row in step.rows {
            let row_key = super::evaluation::row_key(identity, state, &row.id)?;
            self.seal_scratch_row(&row_key, work).await?;
        }
        match self
            .store
            .compare_exchange(page_key, Some(&stored.bytes), SEALED)
            .await?
        {
            CasOutcome::Applied(_) => Ok(false),
            CasOutcome::Conflict(Some(v)) if v.bytes == SEALED => Ok(false),
            CasOutcome::Conflict(_) => Err(CompositionError::Conflict),
        }
    }
    async fn seal_scratch_row(
        &self,
        key: &[u8],
        work: &mut InspectionWork<'_>,
    ) -> Result<(), CompositionError> {
        let stored = work.get(key, RECORD_CAP).await?;
        if stored.as_ref().is_some_and(|v| v.bytes == SEALED) {
            return Ok(());
        }
        match self
            .store
            .compare_exchange(key, stored.as_ref().map(|v| v.bytes.as_slice()), SEALED)
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(v)) if v.bytes == SEALED => Ok(()),
            CasOutcome::Conflict(_) => Err(CompositionError::Conflict),
        }
    }
}
