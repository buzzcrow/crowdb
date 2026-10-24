// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    CompositionCandidateCleanup, CompositionEditCleanup, CompositionError, CompositionScratchCleanup,
    CompositionUploadCleanup,
};
use super::CompositionReclaimCursor;
use crate::{DatasetAuthority, DatasetUploads, ReadCancellation};

impl DatasetAuthority {
    /// Advances only legacy candidate fences in an isolated fixture.
    /// # Errors
    /// Returns invalid facts, storage errors, cancellation and work limits.
    pub async fn cleanup_composition_candidates_step_for_tests(
        &self,
        cursor: &mut CompositionReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<CompositionCandidateCleanup, CompositionError> {
        self.cleanup_composition_candidates_step(
            &cursor.identity,
            cursor.operation,
            &mut cursor.candidates,
            cancel,
        )
        .await
    }
    /// Advances only legacy scratch fences in an isolated fixture.
    /// # Errors
    /// Returns invalid facts, storage errors, cancellation and work limits.
    pub async fn cleanup_composition_scratch_step_for_tests(
        &self,
        cursor: &mut CompositionReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<CompositionScratchCleanup, CompositionError> {
        self.cleanup_composition_scratch_step(&cursor.identity, cursor.operation, &mut cursor.scratch, cancel)
            .await
    }
    /// Advances only legacy edit fences in an isolated fixture.
    /// # Errors
    /// Returns live drafts, invalid facts, storage errors, cancellation and work limits.
    pub async fn cleanup_composition_edits_step_for_tests(
        &self,
        cursor: &mut CompositionReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<CompositionEditCleanup, CompositionError> {
        self.cleanup_composition_edits_step(&cursor.identity, cursor.operation, &mut cursor.edits, cancel)
            .await
    }
    /// Advances only legacy upload owner retirement in an isolated fixture.
    /// # Errors
    /// Returns invalid facts, storage errors and work limits.
    pub async fn cleanup_composition_uploads_step_for_tests(
        &self,
        cursor: &mut CompositionReclaimCursor,
        uploads: &DatasetUploads,
    ) -> Result<CompositionUploadCleanup, CompositionError> {
        self.cleanup_composition_uploads_step(
            &cursor.identity,
            cursor.operation,
            uploads,
            &mut cursor.uploads,
        )
        .await
    }
}
