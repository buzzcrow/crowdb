// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{prepare::candidate_operation, state::now, CompositionError, DraftProgress},
    state::CandidateOwnership,
    CompositionUploadStatus,
};
use crate::{DatasetAuthority, DatasetIdentity, DatasetUploads, OperationId, ReadCancellation};
use std::{collections::BTreeSet, time::Duration};

impl DatasetAuthority {
    /// Advances one bounded window of candidate payload ownership before data publication.
    /// Every owner is durably registered before its upload claim can be sent.
    /// # Errors
    /// Rejects incomplete/frozen candidates, divergent payload metadata and storage failures.
    pub async fn prepare_composition_uploads(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        uploads: &DatasetUploads,
        cancel: &ReadCancellation,
    ) -> Result<CompositionUploadStatus, CompositionError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.prepare_upload_window(identity, operation, uploads, cancel),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }
    async fn prepare_upload_window(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        uploads: &DatasetUploads,
        cancel: &ReadCancellation,
    ) -> Result<CompositionUploadStatus, CompositionError> {
        let (old, state) = self.load_composition(identity, operation).await?;
        if !matches!(state.draft.record.progress, DraftProgress::Prepared { .. }) {
            return Err(CompositionError::Frozen);
        }
        let preparation = state
            .draft
            .preparation
            .as_ref()
            .ok_or(CompositionError::Corrupt)?;
        let mut next = state.clone();
        next.edit = None;
        let ownership = next.uploads.as_mut().ok_or(CompositionError::Conflict)?;
        let position = register_candidate(
            ownership,
            operation,
            state.draft.record.revision,
            preparation.candidate,
        )?;
        let candidate = &ownership.candidates[position];
        if candidate.snapshot != preparation.candidate || candidate.retired != 0 {
            return Err(CompositionError::Corrupt);
        }
        if candidate.complete {
            self.complete_snapshot_candidate(identity, candidate.snapshot)
                .await?;
            return super::state::status(&state);
        }
        let page = self
            .composition_candidate_uploads_page(identity, candidate.snapshot, candidate.after.clone(), cancel)
            .await?;
        for reference in &page.entries {
            let record = uploads
                .metadata(&DatasetUploads::location(identity, reference.id)?)
                .await?;
            if record.length != reference.length || record.md5 != reference.md5 {
                return Err(crate::UploadError::Integrity.into());
            }
        }
        let candidate = &mut ownership.candidates[position];
        let owner = candidate.owner;
        let mut registered: BTreeSet<_> = candidate.uploads.iter().copied().collect();
        registered.extend(page.entries.iter().map(|r| r.id));
        candidate.uploads = registered.into_iter().collect();
        self.replace_composition(identity, &old, &state, &next).await?;
        for reference in page.entries {
            uploads
                .claim(&DatasetUploads::location(identity, reference.id)?, owner, now())
                .await?;
        }
        let (stored, current) = self.load_composition(identity, operation).await?;
        if current != next {
            return Err(CompositionError::Conflict);
        }
        let mut completed = current.clone();
        let candidate = &mut completed
            .uploads
            .as_mut()
            .ok_or(CompositionError::Corrupt)?
            .candidates[position];
        candidate.complete = page.after.is_none();
        candidate.after = page.after;
        self.replace_composition(identity, &stored, &current, &completed)
            .await?;
        if completed
            .uploads
            .as_ref()
            .ok_or(CompositionError::Corrupt)?
            .candidates[position]
            .complete
        {
            self.complete_snapshot_candidate(identity, preparation.candidate)
                .await?;
        }
        super::state::status(&completed)
    }
}

fn register_candidate(
    ownership: &mut super::state::UploadOwnership,
    operation: OperationId,
    input_revision: u64,
    snapshot: crate::SnapshotId,
) -> Result<usize, CompositionError> {
    if let Some(index) = ownership
        .candidates
        .iter()
        .position(|c| c.input_revision == input_revision)
    {
        return Ok(index);
    }
    ownership.candidates.push(CandidateOwnership {
        input_revision,
        snapshot,
        owner: candidate_operation(operation, input_revision)?,
        uploads: vec![],
        after: None,
        complete: false,
        retired: 0,
        keep_committed: false,
        fenced: false,
    });
    Ok(ownership.candidates.len() - 1)
}
