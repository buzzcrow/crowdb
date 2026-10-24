// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    evaluation::row_prefix, state::CompositionState, CompositionDraft, CompositionError,
    CompositionPreparation, DraftProgress,
};
use crate::{
    inspection::work::InspectionWork, AuthorityError, CommitTarget, DatasetAuthority, DatasetIdentity,
    ManifestRecord, MetadataScanBudget, OperationId, ReadCancellation, SampleRecord, SnapshotId,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

impl DatasetAuthority {
    /// Advances candidate and initial Split preparation; only complete content becomes Prepared.
    /// # Errors
    /// Rejects stale/incomplete previews, empty data, policy violations and bounded-work limits.
    pub async fn prepare_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        expected_revision: u64,
        cancel: &ReadCancellation,
    ) -> Result<CompositionDraft, CompositionError> {
        let (old, state) = self.load_composition(identity, operation).await?;
        if state.draft.record.revision != expected_revision {
            return Err(CompositionError::Conflict);
        }
        if matches!(state.draft.record.progress, DraftProgress::Prepared { .. }) {
            if state.uploads.is_none() {
                self.complete_snapshot_candidate(
                    identity,
                    state
                        .draft
                        .preparation
                        .as_ref()
                        .ok_or(CompositionError::Corrupt)?
                        .candidate,
                )
                .await?;
            }
            return Ok(state.draft);
        }
        if !matches!(state.draft.record.progress, DraftProgress::Evaluated { .. }) {
            return Err(CompositionError::Frozen);
        }
        self.check_composition_source(identity, &state.draft).await?;
        let mut preparation = if let Some(preparation) = &state.draft.preparation {
            preparation.clone()
        } else {
            self.create_composition_candidate(identity, &state, cancel)
                .await?
        };
        let mut complete = true;
        if let Some(split_operation) = preparation.split_operation {
            let draft = self
                .begin_initial_split(
                    identity,
                    split_operation,
                    preparation.candidate,
                    state.draft.policy,
                    state.draft.splits.clone(),
                )
                .await?;
            if !matches!(draft.progress, DraftProgress::Prepared { .. }) {
                let page = match draft.progress {
                    DraftProgress::Evaluating { next_page, .. } => next_page,
                    _ => 0,
                };
                let preview = self
                    .evaluate_split_draft(identity, split_operation, 1, page, cancel)
                    .await?;
                complete = preview.complete;
                preparation.split_preview = Some(preview);
                if complete {
                    self.prepare_split_draft(identity, split_operation, 1).await?;
                }
            }
        }
        let mut next = state.clone();
        next.edit = None;
        if complete {
            next.draft.record.progress = DraftProgress::Prepared {
                target: CommitTarget::Snapshot {
                    snapshot: preparation.candidate,
                },
            };
        }
        next.draft.preparation = Some(preparation);
        self.replace_composition(identity, &old, &state, &next).await?;
        if complete && next.uploads.is_none() {
            self.complete_snapshot_candidate(
                identity,
                next.draft
                    .preparation
                    .as_ref()
                    .ok_or(CompositionError::Corrupt)?
                    .candidate,
            )
            .await?;
        }
        Ok(next.draft)
    }
    async fn create_composition_candidate(
        &self,
        identity: &DatasetIdentity,
        state: &CompositionState,
        cancel: &ReadCancellation,
    ) -> Result<CompositionPreparation, CompositionError> {
        let manifest = self.composition_manifest(identity, state, cancel).await?;
        let bytes = bincode::serialize(&manifest).map_err(|_| CompositionError::Corrupt)?;
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(CompositionError::Limit);
        }
        let operation = candidate_operation(state.draft.record.operation, state.draft.record.revision)?;
        let split_operation = if state.draft.splits.is_empty() {
            None
        } else {
            Some(candidate_operation(operation, 1)?)
        };
        let initial = split_operation
            .map(|operation| crate::split::candidate_revision(operation, 1))
            .transpose()?;
        let snapshot = self
            .prepare_candidate_with_split(identity, state.draft.record.parent, bytes, operation, initial)
            .await?;
        Ok(CompositionPreparation {
            candidate: snapshot,
            split_operation,
            split_preview: None,
        })
    }
    async fn composition_manifest(
        &self,
        identity: &DatasetIdentity,
        state: &CompositionState,
        cancel: &ReadCancellation,
    ) -> Result<ManifestRecord, CompositionError> {
        let preview = &state
            .evaluation
            .as_ref()
            .ok_or(CompositionError::Corrupt)?
            .preview;
        if !preview.complete || preview.members == 0 {
            return Err(CompositionError::Conflict);
        }
        let operation = candidate_operation(state.draft.record.operation, state.draft.record.revision)?;
        let mut manifest = ManifestRecord {
            version: 1,
            snapshot: SnapshotId::from_bytes(operation.as_bytes()).map_err(AuthorityError::from)?,
            schema: state.draft.schema.clone(),
            samples: vec![],
        };
        let budget = MetadataScanBudget {
            items: 20,
            bytes: 16 * 1024 * 1024,
            timeout: Duration::from_secs(30),
        };
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let prefix = row_prefix(identity, state)?;
        let mut after = None;
        let mut binding = [0; 32];
        let mut bytes = 0usize;
        loop {
            let page = work.scan(prefix.clone(), after, 20).await?;
            for (_, stored) in page.entries {
                bytes = bytes
                    .checked_add(stored.bytes.len())
                    .ok_or(CompositionError::Limit)?;
                if bytes > 8 * 1024 * 1024 {
                    return Err(CompositionError::Limit);
                }
                let sample: Option<SampleRecord> = crate::split::codec::decode(&stored.bytes)?;
                if let Some(sample) = sample {
                    let mut hash = Sha256::new();
                    hash.update(binding);
                    hash.update(Sha256::digest(&stored.bytes));
                    binding = hash.finalize().into();
                    manifest.samples.push(sample);
                }
            }
            after = page.after;
            if after.is_none() {
                break;
            }
        }
        if binding != preview.binding || manifest.samples.len() as u64 != preview.members {
            return Err(CompositionError::Corrupt);
        }
        manifest.validate().map_err(AuthorityError::from)?;
        Ok(manifest)
    }
}
pub(super) fn candidate_operation(
    operation: OperationId,
    revision: u64,
) -> Result<OperationId, CompositionError> {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-composition-candidate-v1\0");
    hash.update(operation.as_bytes());
    hash.update(revision.to_be_bytes());
    OperationId::from_bytes(&hash.finalize()[..16]).map_err(|e| AuthorityError::from(e).into())
}
