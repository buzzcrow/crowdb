// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, prepare::candidate_operation, CompositionError, DraftProgress};
use crate::{CasOutcome, DatasetAuthority, DatasetIdentity, OperationId, ReadCancellation};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompositionCandidateCleanup {
    pub input_revision: u64,
    pub complete: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u16,
    revision: u64,
}
impl DatasetAuthority {
    /// Recovers failed and superseded preparations by their deterministic operation identity.
    /// # Errors
    /// Returns unavailable fences, ownership protection, corrupt records and work limits.
    pub async fn cleanup_composition_candidates_step(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cancel: &ReadCancellation,
    ) -> Result<CompositionCandidateCleanup, CompositionError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.cleanup_candidate_window(identity, operation, cancel),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }
    async fn cleanup_candidate_window(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
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
        let cursor_key = key::scope(b"DS\0CCP\0", identity, operation)?;
        let stored = self.composition_get(&cursor_key).await?;
        let mut cursor: Cursor = stored.as_ref().map_or_else(
            || {
                Ok(Cursor {
                    version: 1,
                    revision: 1,
                })
            },
            |v| crate::split::codec::decode(&v.bytes),
        )?;
        if cursor.version != 1 || cursor.revision == 0 || cursor.revision > end {
            return Err(CompositionError::Corrupt);
        }
        if cursor.revision < end {
            let candidate = candidate_operation(operation, cursor.revision)?;
            if self
                .retire_composition_candidate(identity, candidate, cancel)
                .await?
            {
                cursor.revision += 1;
                let bytes = crate::split::codec::encode(&cursor)?;
                match self
                    .store
                    .compare_exchange(&cursor_key, stored.as_ref().map(|v| v.bytes.as_slice()), &bytes)
                    .await?
                {
                    CasOutcome::Applied(_) => {}
                    CasOutcome::Conflict(Some(value)) if value.bytes == bytes => {}
                    CasOutcome::Conflict(_) => return Err(CompositionError::Conflict),
                }
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
            .retire_candidate_metadata_step(identity, &retired, cancel)
            .await?
        {
            return Ok(false);
        }
        if !initial_exists {
            return Ok(true);
        }
        match self
            .cleanup_split_scratch_step(identity, split_operation, cancel)
            .await
        {
            Ok(progress) => Ok(progress.complete),
            Err(crate::split::SplitLifecycleError::NotFound) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }
}
