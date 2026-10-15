// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    evaluation::row_prefix, state::CompositionState, CompositionDraft, CompositionError, DraftProgress,
};
use crate::{
    inspection::work::InspectionWork, AuthorityError, CommitTarget, DatasetAuthority, DatasetIdentity,
    ManifestRecord, MetadataScanBudget, OperationId, ReadCancellation, SampleRecord, SnapshotId,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

impl DatasetAuthority {
    /// Freezes a fully evaluated data-only composition as an unpublished candidate.
    /// # Errors
    /// Rejects stale/incomplete previews, empty data, initial Splits and bounded-work limits.
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
            return Ok(state.draft);
        }
        if !matches!(state.draft.record.progress, DraftProgress::Evaluated { .. }) {
            return Err(CompositionError::Frozen);
        }
        if !state.draft.splits.is_empty() {
            return Err(CompositionError::Conflict);
        }
        self.renew_composition_source(identity, &state.draft).await?;
        let manifest = self.composition_manifest(identity, &state, cancel).await?;
        let bytes = bincode::serialize(&manifest).map_err(|_| CompositionError::Corrupt)?;
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(CompositionError::Limit);
        }
        let snapshot = self
            .prepare_snapshot_candidate(
                identity,
                state.draft.record.parent,
                bytes,
                candidate_operation(operation, expected_revision)?,
            )
            .await?;
        let mut next = state.clone();
        next.edit = None;
        next.draft.record.progress = DraftProgress::Prepared {
            target: CommitTarget::Snapshot { snapshot },
        };
        self.replace_composition(identity, &old, &state, &next).await?;
        Ok(next.draft)
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
fn candidate_operation(operation: OperationId, revision: u64) -> Result<OperationId, CompositionError> {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-composition-candidate-v1\0");
    hash.update(operation.as_bytes());
    hash.update(revision.to_be_bytes());
    OperationId::from_bytes(&hash.finalize()[..16]).map_err(|e| AuthorityError::from(e).into())
}
