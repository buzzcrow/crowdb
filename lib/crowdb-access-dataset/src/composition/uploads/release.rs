// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{state::CompositionState, CompositionError, DraftProgress};
use crate::{CommitTarget, DatasetAuthority, DatasetIdentity, DatasetUploads, OperationId};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompositionUploadCleanup {
    pub retired_owners: u64,
    pub complete: bool,
}

#[derive(Clone, Debug, Default)]
pub(in crate::composition) struct UploadCursor {
    staged: usize,
    candidate: usize,
    retired: usize,
    fenced: bool,
    retired_owners: u64,
}
impl DatasetAuthority {
    /// Retires up to twenty obsolete ownership references using local progress.
    /// Complete resources retain their owners; other owners of shared uploads are untouched.
    pub(in crate::composition) async fn cleanup_composition_uploads_step(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        uploads: &DatasetUploads,
        cursor: &mut UploadCursor,
    ) -> Result<CompositionUploadCleanup, CompositionError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.release_composition_upload_window(identity, operation, uploads, cursor),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }
    async fn release_composition_upload_window(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        uploads: &DatasetUploads,
        cursor: &mut UploadCursor,
    ) -> Result<CompositionUploadCleanup, CompositionError> {
        let (_, state) = self.load_composition(identity, operation).await?;
        let Some(ownership) = &state.uploads else {
            return Ok(CompositionUploadCleanup {
                retired_owners: 0,
                complete: true,
            });
        };
        if cursor.staged > ownership.staged.len() || cursor.candidate > ownership.candidates.len() {
            return Err(CompositionError::Corrupt);
        }
        let release_staged = staging_releasable(&state)?;
        let mut remaining = 20;
        if release_staged {
            let owner = super::staging_owner(operation)?;
            while remaining > 0 && cursor.staged < ownership.staged.len() {
                let id = ownership.staged[cursor.staged];
                uploads
                    .retire_owner(&DatasetUploads::location(identity, id)?, owner)
                    .await?;
                cursor.staged += 1;
                cursor.retired_owners += 1;
                remaining -= 1;
            }
        }
        while remaining > 0 {
            let Some(candidate) = ownership.candidates.get(cursor.candidate) else {
                break;
            };
            if !obsolete(&state, candidate.input_revision, candidate.snapshot) || candidate.keep_committed {
                cursor.next_candidate();
                continue;
            }
            if !cursor.fenced {
                remaining -= 1;
                if self
                    .abort_uncommitted_candidate(identity, candidate.snapshot)
                    .await?
                {
                    cursor.next_candidate();
                    continue;
                }
                cursor.fenced = true;
            }
            while remaining > 0 && cursor.retired < candidate.uploads.len() {
                let id = candidate.uploads[cursor.retired];
                uploads
                    .retire_owner(&DatasetUploads::location(identity, id)?, candidate.owner)
                    .await?;
                cursor.retired += 1;
                cursor.retired_owners += 1;
                remaining -= 1;
            }
            if cursor.retired == candidate.uploads.len() {
                cursor.next_candidate();
            }
        }
        Ok(CompositionUploadCleanup {
            retired_owners: cursor.retired_owners,
            complete: (!release_staged || cursor.staged == ownership.staged.len())
                && cursor.candidate == ownership.candidates.len(),
        })
    }
}
impl UploadCursor {
    fn next_candidate(&mut self) {
        self.candidate += 1;
        self.retired = 0;
        self.fenced = false;
    }
}
fn obsolete(state: &CompositionState, revision: u64, snapshot: crate::SnapshotId) -> bool {
    if matches!(state.draft.record.progress, DraftProgress::Published { target:CommitTarget::Snapshot { snapshot:committed }, .. } if committed == snapshot)
    {
        return false;
    }
    revision < state.draft.record.revision || matches!(state.draft.record.progress, DraftProgress::Abandoned)
}
fn staging_releasable(state: &CompositionState) -> Result<bool, CompositionError> {
    Ok(matches!(
        state.draft.record.progress,
        DraftProgress::Abandoned | DraftProgress::Published { .. }
    ) || (matches!(
        state.draft.record.progress,
        DraftProgress::Prepared { .. } | DraftProgress::Committing { .. }
    ) && super::publication_ready(state)?))
}
