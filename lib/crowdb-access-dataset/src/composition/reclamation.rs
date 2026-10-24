// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Restartable cleanup of historical draft facts without process checkpoints.

mod sweep;
mod target;
#[cfg(feature = "test-util")]
mod test_hooks;
pub use sweep::CompositionReclaimSweep;

use super::{
    candidates::CandidateCursor, cleanup::ScratchCursor, uploads::UploadCursor, CompositionEditCleanup,
    CompositionError,
};
use crate::{DatasetAuthority, DatasetIdentity, DatasetUploads, OperationId, ReadCancellation};
use std::time::Duration;

/// Local-only position bound to one historical draft. Restart repeats resource retirement.
#[derive(Clone, Debug)]
pub struct CompositionReclaimCursor {
    identity: DatasetIdentity,
    operation: OperationId,
    phase: u8,
    candidates: CandidateCursor,
    uploads: UploadCursor,
    scratch: ScratchCursor,
    edits: CompositionEditCleanup,
}
impl CompositionReclaimCursor {
    #[must_use]
    pub fn new(identity: DatasetIdentity, operation: OperationId) -> Self {
        Self {
            identity,
            operation,
            phase: 0,
            candidates: CandidateCursor::default(),
            uploads: UploadCursor::default(),
            scratch: ScratchCursor::default(),
            edits: CompositionEditCleanup::default(),
        }
    }
}

impl DatasetAuthority {
    /// Advances one cleanup component without waiting for physical capacity recovery.
    /// # Errors
    /// Returns malformed historical facts, storage failures, cancellation and work limits.
    pub async fn advance_composition_reclaim(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut CompositionReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<bool, CompositionError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.composition_reclaim_window(uploads, cursor, cancel),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }
    async fn composition_reclaim_window(
        &self,
        uploads: &DatasetUploads,
        cursor: &mut CompositionReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<bool, CompositionError> {
        if cancel.is_cancelled() {
            return Err(crate::inspection::InspectionError::Cancelled.into());
        }
        let complete = match cursor.phase {
            0 => {
                self.cleanup_composition_candidates_step(
                    &cursor.identity,
                    cursor.operation,
                    &mut cursor.candidates,
                    cancel,
                )
                .await?
                .complete
            }
            1 => {
                self.cleanup_composition_uploads_step(
                    &cursor.identity,
                    cursor.operation,
                    uploads,
                    &mut cursor.uploads,
                )
                .await?
                .complete
            }
            2 => {
                self.cleanup_composition_scratch_step(
                    &cursor.identity,
                    cursor.operation,
                    &mut cursor.scratch,
                    cancel,
                )
                .await?
                .complete
            }
            3 => match self
                .cleanup_composition_edits_step(&cursor.identity, cursor.operation, &mut cursor.edits, cancel)
                .await
            {
                Ok(progress) => progress.complete,
                Err(CompositionError::Frozen) => true,
                Err(error) => return Err(error),
            },
            4 => return self.delete_obsolete_composition_checkpoints(cursor, cancel).await,
            _ => return Err(CompositionError::Corrupt),
        };
        if complete {
            cursor.phase += 1;
        }
        Ok(false)
    }
    async fn delete_obsolete_composition_checkpoints(
        &self,
        cursor: &CompositionReclaimCursor,
        cancel: &ReadCancellation,
    ) -> Result<bool, CompositionError> {
        for prefix in [b"DS\0CCP\0".as_slice(), b"DS\0CCG\0", b"DS\0CEG\0"] {
            if cancel.is_cancelled() {
                return Err(crate::inspection::InspectionError::Cancelled.into());
            }
            self.store
                .delete(&super::key::scope(prefix, &cursor.identity, cursor.operation)?)
                .await?;
        }
        Ok(true)
    }
}
