// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{identity::candidate_operation, CompositionError, DraftProgress};
use crate::{DatasetAuthority, DatasetIdentity, OperationId, ReadCancellation};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompositionCandidateCleanup {
    pub input_revision: u64,
    pub complete: bool,
}
#[derive(Clone, Debug)]
pub(super) struct CandidateCursor {
    revision: u64,
    indexes: crate::inspection::CandidateRetirementCursor,
    split: Option<crate::split::SplitReclaimCursor>,
}
impl Default for CandidateCursor {
    fn default() -> Self {
        Self {
            revision: 1,
            indexes: crate::inspection::CandidateRetirementCursor::default(),
            split: None,
        }
    }
}
impl DatasetAuthority {
    /// Recovers failed and superseded preparations by their deterministic operation identity.
    /// # Errors
    /// Returns unavailable fences, ownership protection, corrupt records and work limits.
    pub(super) async fn cleanup_composition_candidates_step(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cursor: &mut CandidateCursor,
        cancel: &ReadCancellation,
    ) -> Result<CompositionCandidateCleanup, CompositionError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.cleanup_candidate_window(identity, operation, cursor, cancel),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }
    async fn cleanup_candidate_window(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cursor: &mut CandidateCursor,
        cancel: &ReadCancellation,
    ) -> Result<CompositionCandidateCleanup, CompositionError> {
        if cancel.is_cancelled() {
            return Err(crate::inspection::InspectionError::Cancelled.into());
        }
        let (_, state) = self.load_composition(identity, operation).await?;
        let end = state
            .draft
            .record
            .revision
            .checked_add(u64::from(matches!(
                state.draft.record.progress,
                DraftProgress::Abandoned
            )))
            .ok_or(CompositionError::Limit)?;
        if cursor.revision == 0 || cursor.revision > end {
            return Err(CompositionError::Corrupt);
        }
        if cursor.revision < end {
            let candidate = candidate_operation(operation, cursor.revision)?;
            if self
                .retire_composition_candidate(
                    identity,
                    candidate,
                    &mut cursor.indexes,
                    &mut cursor.split,
                    cancel,
                )
                .await?
            {
                cursor.revision += 1;
                cursor.indexes = crate::inspection::CandidateRetirementCursor::default();
                cursor.split = None;
            }
        }
        Ok(CompositionCandidateCleanup {
            input_revision: cursor.revision,
            complete: cursor.revision == end,
        })
    }
    async fn retire_composition_candidate(
        &self,
        identity: &DatasetIdentity,
        candidate: OperationId,
        indexes: &mut crate::inspection::CandidateRetirementCursor,
        split: &mut Option<crate::split::SplitReclaimCursor>,
        cancel: &ReadCancellation,
    ) -> Result<bool, CompositionError> {
        let Some(record) = self.fence_candidate_reservation(identity, candidate).await? else {
            return Ok(true);
        };
        let split_operation = candidate_operation(candidate, 1)?;
        let initial_exists = self
            .fence_initial_split_reservation(identity, split_operation)
            .await?;
        if self
            .abort_uncommitted_candidate(identity, record.publication.snapshot)
            .await?
        {
            return Ok(true);
        }
        let mut retired = record;
        retired.publication.state = crate::PublicationState::Aborted;
        if !self
            .retire_candidate_metadata_step(identity, &retired, indexes, cancel)
            .await?
        {
            return Ok(false);
        }
        if !initial_exists {
            return Ok(true);
        }
        let split = split
            .get_or_insert_with(|| crate::split::SplitReclaimCursor::new(identity.clone(), split_operation));
        match self.advance_split_reclaim(split, cancel).await {
            Ok(progress) => Ok(progress.complete),
            Err(crate::split::SplitLifecycleError::NotFound) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }
}
