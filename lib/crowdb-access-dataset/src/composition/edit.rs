// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{CompositionDraft, CompositionError, DraftProgress};
use crate::{DatasetAuthority, DatasetIdentity, OperationId};

impl DatasetAuthority {
    /// Reads the stored composition without replaying edits or publication requests.
    /// # Errors
    /// Returns unknown operations, corrupt metadata and storage failures.
    pub async fn composition_status(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<CompositionDraft, CompositionError> {
        let (_, state) = self.load_composition(identity, operation).await?;
        let mut draft = state.draft;
        if let DraftProgress::Committing { target, .. } = draft.record.progress {
            draft.record.progress = DraftProgress::Prepared { target };
        }
        Ok(draft)
    }
    /// Abandons an unpublished composition and releases only its owned source lease.
    /// # Errors
    /// Rejects stale revisions and publications in progress or already committed.
    pub async fn abandon_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        expected_revision: u64,
    ) -> Result<CompositionDraft, CompositionError> {
        let (old, state) = self.load_composition(identity, operation).await?;
        if state.draft.record.revision != expected_revision {
            return Err(CompositionError::Conflict);
        }
        if matches!(
            state.draft.record.progress,
            DraftProgress::Committing { .. } | DraftProgress::Published { .. }
        ) {
            return Err(CompositionError::Frozen);
        }
        let mut next = state.clone();
        next.draft.record.progress = DraftProgress::Abandoned;
        next.edit = None;
        self.replace_composition(identity, &old, &state, &next).await?;
        if let Some(preparation) = &next.draft.preparation {
            self.abort_uncommitted_candidate(identity, preparation.candidate)
                .await?;
        }
        Ok(next.draft)
    }
}
