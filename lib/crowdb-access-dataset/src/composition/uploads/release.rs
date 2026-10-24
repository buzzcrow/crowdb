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
impl DatasetAuthority {
    /// Retires up to twenty obsolete ownership references with durable restart progress.
    /// Published candidates retain their owners; other owners of shared uploads are untouched.
    /// # Errors
    /// Rejects publication still in flight, unavailable fences and failed owner retirement.
    pub async fn cleanup_composition_uploads_step(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        uploads: &DatasetUploads,
    ) -> Result<CompositionUploadCleanup, CompositionError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.release_composition_upload_window(identity, operation, uploads),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }
    async fn release_composition_upload_window(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        uploads: &DatasetUploads,
    ) -> Result<CompositionUploadCleanup, CompositionError> {
        let (_, observed) = self.load_composition(identity, operation).await?;
        if matches!(observed.draft.record.progress, DraftProgress::Committing { .. }) {
            return Err(CompositionError::Frozen);
        }
        if matches!(observed.draft.record.progress, DraftProgress::Abandoned) {
            // Help any interrupted candidate fence before retiring payload protection.
            self.abandon_composition(identity, operation, observed.draft.record.revision)
                .await?;
        }
        let (old, state) = self.load_composition(identity, operation).await?;
        let mut next = state.clone();
        next.edit = None;
        let release_staged = staging_releasable(&state)?;
        let ownership = next.uploads.as_mut().ok_or(CompositionError::Conflict)?;
        let mut remaining = 20;
        if release_staged {
            let owner = super::staging_owner(operation)?;
            while remaining > 0 && ownership.staged_retired < ownership.staged.len() {
                let id = ownership.staged[ownership.staged_retired];
                uploads
                    .retire_owner(&DatasetUploads::location(identity, id)?, owner)
                    .await?;
                ownership.staged_retired += 1;
                remaining -= 1;
            }
        }
        for candidate in &mut ownership.candidates {
            if !obsolete(&state, candidate.input_revision, candidate.snapshot) || candidate.keep_committed {
                continue;
            }
            if remaining == 0 {
                break;
            }
            if !candidate.fenced {
                remaining -= 1;
                if self
                    .abort_uncommitted_candidate(identity, candidate.snapshot)
                    .await?
                {
                    candidate.keep_committed = true;
                    continue;
                }
                candidate.fenced = true;
            }
            while remaining > 0 && candidate.retired < candidate.uploads.len() {
                let id = candidate.uploads[candidate.retired];
                uploads
                    .retire_owner(&DatasetUploads::location(identity, id)?, candidate.owner)
                    .await?;
                candidate.retired += 1;
                remaining -= 1;
            }
        }
        self.replace_composition(identity, &old, &state, &next).await?;
        cleanup_status(&next)
    }
}
fn obsolete(state: &CompositionState, revision: u64, snapshot: crate::SnapshotId) -> bool {
    if matches!(state.draft.record.progress, DraftProgress::Published { target:CommitTarget::Snapshot { snapshot:committed }, .. } if committed == snapshot)
    {
        return false;
    }
    revision < state.draft.record.revision || matches!(state.draft.record.progress, DraftProgress::Abandoned)
}
fn cleanup_status(state: &CompositionState) -> Result<CompositionUploadCleanup, CompositionError> {
    let ownership = state.uploads.as_ref().ok_or(CompositionError::Conflict)?;
    let release_staged = staging_releasable(state)?;
    Ok(CompositionUploadCleanup {
        retired_owners: ownership.staged_retired as u64
            + ownership.candidates.iter().map(|c| c.retired as u64).sum::<u64>(),
        complete: (!release_staged || ownership.staged_retired == ownership.staged.len())
            && ownership.candidates.iter().all(|c| {
                !obsolete(state, c.input_revision, c.snapshot)
                    || c.keep_committed
                    || (c.fenced && c.retired == c.uploads.len())
            }),
    })
}
fn staging_releasable(state: &CompositionState) -> Result<bool, CompositionError> {
    Ok(matches!(
        state.draft.record.progress,
        DraftProgress::Abandoned | DraftProgress::Published { .. }
    ) || (matches!(state.draft.record.progress, DraftProgress::Prepared { .. })
        && super::publication_ready(state)?))
}
