// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, state::CompositionState, CompositionError, DraftProgress};
use crate::{CasOutcome, DatasetAuthority, DatasetIdentity, OperationId, ReadCancellation, SampleRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompositionPreview {
    pub input_revision: u64,
    pub next_page: u64,
    pub members: u64,
    pub complete: bool,
    pub binding: [u8; 32],
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Phase {
    Source { after: Option<Vec<u8>> },
    Edits { page: usize },
    Validate { after: Option<Vec<u8>> },
    Done,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Evaluation {
    pub preview: CompositionPreview,
    pub phase: Phase,
}
#[derive(Serialize, Deserialize)]
pub(super) struct RowChange {
    pub id: Vec<u8>,
    pub expected: Option<[u8; 32]>,
    pub sample: Option<SampleRecord>,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Step {
    pub next: Evaluation,
    pub rows: Vec<RowChange>,
}
impl DatasetAuthority {
    /// Advances one durable source/edit/validation window. Old page retries never reapply edits.
    /// # Errors
    /// Rejects stale inputs, invalid Samples, cancelled work and unavailable source data.
    pub async fn evaluate_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        expected_revision: u64,
        expected_page: u64,
        cancel: &ReadCancellation,
    ) -> Result<CompositionPreview, CompositionError> {
        if cancel.is_cancelled() {
            return Err(crate::inspection::InspectionError::Cancelled.into());
        }
        let (old, state) = self.load_composition(identity, operation).await?;
        if state.draft.record.revision != expected_revision {
            return Err(CompositionError::Conflict);
        }
        if !matches!(
            state.draft.record.progress,
            DraftProgress::Editing | DraftProgress::Evaluating { .. } | DraftProgress::Evaluated { .. }
        ) {
            return Err(CompositionError::Frozen);
        }
        let current = state.evaluation.clone().unwrap_or(Evaluation {
            preview: CompositionPreview {
                input_revision: expected_revision,
                next_page: 0,
                members: 0,
                complete: false,
                binding: [0; 32],
            },
            phase: Phase::Source { after: None },
        });
        if expected_page > current.preview.next_page {
            return Err(CompositionError::Conflict);
        }
        if expected_page < current.preview.next_page || current.preview.complete {
            return Ok(current.preview);
        }
        self.renew_composition_source(identity, &state.draft).await?;
        let step_key = key::page(b"DS\0CW\0", identity, operation, expected_revision, expected_page)?;
        let step = if let Some(value) = self.composition_get(&step_key).await? {
            crate::split::codec::decode::<Step>(&value.bytes)?
        } else {
            let mut step = self
                .composition_evaluation_step(identity, &state, &current, cancel)
                .await?;
            step.next.preview.next_page = expected_page.checked_add(1).ok_or(CompositionError::Limit)?;
            self.put_composition(&step_key, &crate::split::codec::encode(&step)?)
                .await?;
            step
        };
        self.apply_composition_rows(identity, &state, &step.rows).await?;
        let mut next = state.clone();
        next.edit = None;
        next.evaluation = Some(step.next.clone());
        next.draft.record.progress = if step.next.preview.complete {
            DraftProgress::Evaluated {
                input_revision: expected_revision,
                binding: step.next.preview.binding,
            }
        } else {
            DraftProgress::Evaluating {
                input_revision: expected_revision,
                next_page: step.next.preview.next_page,
            }
        };
        self.replace_composition(identity, &old, &state, &next).await?;
        Ok(step.next.preview)
    }
    async fn apply_composition_rows(
        &self,
        identity: &DatasetIdentity,
        state: &CompositionState,
        rows: &[RowChange],
    ) -> Result<(), CompositionError> {
        for row in rows {
            let key = row_key(identity, state, &row.id)?;
            let bytes = crate::split::codec::encode(&row.sample)?;
            let stored = self.composition_get(&key).await?;
            if stored.as_ref().is_some_and(|s| s.bytes == bytes) {
                continue;
            }
            if stored
                .as_ref()
                .map(|s| <[u8; 32]>::from(Sha256::digest(&s.bytes)))
                != row.expected
            {
                // Another worker may have advanced a later step. Never overwrite its result.
                return Err(CompositionError::Conflict);
            }
            match self
                .store
                .compare_exchange(&key, stored.as_ref().map(|s| s.bytes.as_slice()), &bytes)
                .await?
            {
                CasOutcome::Applied(_) => {}
                CasOutcome::Conflict(Some(value)) if value.bytes == bytes => {}
                CasOutcome::Conflict(_) => return Err(CompositionError::Conflict),
            }
        }
        Ok(())
    }
}
pub(super) fn row_prefix(
    identity: &DatasetIdentity,
    state: &CompositionState,
) -> Result<Vec<u8>, CompositionError> {
    let mut prefix = key::scope(b"DS\0CM\0", identity, state.draft.record.operation)?;
    prefix.extend_from_slice(&state.draft.record.revision.to_be_bytes());
    Ok(prefix)
}
pub(super) fn row_key(
    identity: &DatasetIdentity,
    state: &CompositionState,
    id: &[u8],
) -> Result<Vec<u8>, CompositionError> {
    let mut key = row_prefix(identity, state)?;
    for byte in id {
        if *byte == 0 {
            key.extend_from_slice(&[0, 255]);
        } else {
            key.push(*byte);
        }
    }
    key.extend_from_slice(&[0, 0]);
    Ok(key)
}
