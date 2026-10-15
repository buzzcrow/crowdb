// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    evaluation::{row_key, row_prefix, Evaluation, Phase, RowChange, Step},
    key,
    state::{budget, CompositionState},
    CompositionEdit, CompositionError, CompositionSource, SampleChange,
};
use crate::{
    inspection::work::InspectionWork, AuthorityError, DatasetAuthority, DatasetIdentity, ManifestRecord,
    ReadCancellation, SampleRecord, SnapshotId,
};
use sha2::{Digest, Sha256};

impl DatasetAuthority {
    pub(super) async fn composition_evaluation_step(
        &self,
        identity: &DatasetIdentity,
        state: &CompositionState,
        current: &Evaluation,
        cancel: &ReadCancellation,
    ) -> Result<Step, CompositionError> {
        let mut step = Step {
            next: current.clone(),
            rows: vec![],
        };
        match &current.phase {
            Phase::Source { after } => {
                self.composition_source_step(identity, state, after.clone(), &mut step, cancel)
                    .await?;
            }
            Phase::Edits { page } => {
                self.composition_patch_step(identity, state, *page, &mut step, cancel)
                    .await?;
            }
            Phase::Validate { after } => {
                self.composition_validate_step(identity, state, after.clone(), &mut step, cancel)
                    .await?;
            }
            Phase::Done => return Err(CompositionError::Corrupt),
        }
        Ok(step)
    }
    async fn composition_source_step(
        &self,
        identity: &DatasetIdentity,
        state: &CompositionState,
        after: Option<Vec<u8>>,
        step: &mut Step,
        cancel: &ReadCancellation,
    ) -> Result<(), CompositionError> {
        let Some(parent) = state.draft.record.parent else {
            step.next.phase = Phase::Edits { page: 0 };
            return Ok(());
        };
        let selection = match &state.draft.record.source {
            CompositionSource::Selection(selection) => Some(selection),
            CompositionSource::Split { selection, .. } => selection.as_ref(),
            CompositionSource::All => None,
        };
        let mut page = match selection {
            Some(selection) => {
                self.inspection_selection_page(identity, parent, selection, after, budget(), cancel)
                    .await?
            }
            None => {
                self.inspection_samples_page(identity, parent, after, budget(), cancel)
                    .await?
            }
        };
        if let CompositionSource::Split { source, .. } = &state.draft.record.source {
            let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget())?;
            let record = self
                .read_split_revision(identity, source.snapshot, source.revision, &mut work)
                .await?;
            let mut members = Vec::new();
            for id in page.entries {
                if self
                    .frozen_split_contains(identity, &record, &source.name, &id, &mut work)
                    .await?
                {
                    members.push(id);
                }
            }
            page.entries = members;
        }
        let samples = self
            .composition_source_samples(
                identity,
                parent,
                &page.entries,
                &state.draft.schema,
                budget(),
                cancel,
            )
            .await?;
        for sample in samples {
            step.rows.push(RowChange {
                id: sample.sample_id.clone(),
                expected: None,
                sample: Some(sample),
            });
        }
        step.next.phase = match page.after {
            Some(after) => Phase::Source { after: Some(after) },
            None => Phase::Edits { page: 0 },
        };
        Ok(())
    }
    async fn composition_patch_step(
        &self,
        identity: &DatasetIdentity,
        state: &CompositionState,
        page: usize,
        step: &mut Step,
        cancel: &ReadCancellation,
    ) -> Result<(), CompositionError> {
        let Some(edit_id) = state.journal.get(page) else {
            step.next.phase = Phase::Validate { after: None };
            return Ok(());
        };
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget())?;
        let mut edit_key = key::scope(b"DS\0CE\0", identity, state.draft.record.operation)?;
        edit_key.extend_from_slice(edit_id.as_bytes());
        let stored = work
            .get(&edit_key, 1024 * 1024)
            .await?
            .ok_or(CompositionError::Corrupt)?;
        let edit: CompositionEdit = crate::split::codec::decode(&stored.bytes)?;
        if edit.id != *edit_id {
            return Err(CompositionError::Corrupt);
        }
        for change in edit.samples {
            let id = match &change {
                SampleChange::Upsert(sample) => &sample.sample_id,
                SampleChange::Remove(id) => id,
            }
            .clone();
            let stored = work.get(&row_key(identity, state, &id)?, 1024 * 1024).await?;
            let expected = stored.as_ref().map(|v| Sha256::digest(&v.bytes).into());
            let existing: Option<SampleRecord> = match &stored {
                Some(stored) => crate::split::codec::decode(&stored.bytes)?,
                None => None,
            };
            let sample = match change {
                SampleChange::Remove(_) => None,
                SampleChange::Upsert(patch) => {
                    let mut sample = match existing {
                        Some(sample) => sample,
                        None if stored.is_none() && state.draft.record.parent.is_some() => self
                            .composition_source_samples(
                                identity,
                                state.draft.record.parent.ok_or(CompositionError::Corrupt)?,
                                std::slice::from_ref(&id),
                                &state.draft.schema,
                                budget(),
                                cancel,
                            )
                            .await?
                            .pop()
                            .unwrap_or_else(|| SampleRecord {
                                sample_id: id.clone(),
                                fields: vec![],
                            }),
                        None => SampleRecord {
                            sample_id: id.clone(),
                            fields: vec![],
                        },
                    };
                    for field in patch.fields {
                        sample.fields.retain(|f| f.name != field.name);
                        if state.draft.schema.fields.iter().any(|f| f.name == field.name) {
                            sample.fields.push(field);
                        }
                    }
                    sample
                        .fields
                        .retain(|f| state.draft.schema.fields.iter().any(|s| s.name == f.name));
                    Some(sample)
                }
            };
            step.rows.push(RowChange { id, expected, sample });
        }
        step.next.phase = Phase::Edits { page: page + 1 };
        Ok(())
    }
    async fn composition_validate_step(
        &self,
        identity: &DatasetIdentity,
        state: &CompositionState,
        after: Option<Vec<u8>>,
        step: &mut Step,
        cancel: &ReadCancellation,
    ) -> Result<(), CompositionError> {
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget())?;
        let page = work.scan(row_prefix(identity, state)?, after, 20).await?;
        for (key, stored) in page.entries {
            let sample: Option<SampleRecord> = crate::split::codec::decode(&stored.bytes)?;
            if let Some(sample) = sample {
                if row_key(identity, state, &sample.sample_id)? != key {
                    return Err(CompositionError::Corrupt);
                }
                ManifestRecord {
                    version: 1,
                    snapshot: SnapshotId::random(),
                    schema: state.draft.schema.clone(),
                    samples: vec![sample],
                }
                .validate()
                .map_err(AuthorityError::from)?;
                let mut hash = Sha256::new();
                hash.update(step.next.preview.binding);
                hash.update(Sha256::digest(&stored.bytes));
                step.next.preview.binding = hash.finalize().into();
                step.next.preview.members = step
                    .next
                    .preview
                    .members
                    .checked_add(1)
                    .ok_or(CompositionError::Limit)?;
            }
        }
        step.next.phase = if let Some(after) = page.after {
            Phase::Validate { after: Some(after) }
        } else {
            step.next.preview.complete = true;
            Phase::Done
        };
        Ok(())
    }
}
