// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key,
    state::{CompositionState, EditMarker, EditReceipt},
    CompositionDraft, CompositionEdit, CompositionError, DraftProgress, SampleChange,
};
use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, ManifestRecord, OperationId, SchemaRecord, SnapshotId,
};
use sha2::{Digest, Sha256};

impl DatasetAuthority {
    /// Returns durable composition state, including edits whose response was lost.
    /// # Errors
    /// Returns unknown operations, corrupt metadata and storage failures.
    pub async fn composition_status(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<CompositionDraft, CompositionError> {
        let (_, state) = self.load_composition(identity, operation).await?;
        self.help_composition_edit(identity, &state).await?;
        Ok(state.draft)
    }
    /// Appends a bounded Sample patch page and changes schema/Splits with a revision CAS.
    /// # Errors
    /// Rejects stale/divergent retries, invalid patches and frozen drafts.
    pub async fn edit_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        edit: &CompositionEdit,
    ) -> Result<CompositionDraft, CompositionError> {
        OperationId::from_bytes(edit.id.as_bytes()).map_err(AuthorityError::from)?;
        let bytes = crate::split::codec::encode(edit)?;
        let marker = EditMarker {
            id: edit.id,
            digest: Sha256::digest(&bytes).into(),
        };
        if let Some(draft) = self
            .composition_edit_receipt(identity, operation, &marker)
            .await?
        {
            return Ok(draft);
        }
        let (old, state) = self.load_composition(identity, operation).await?;
        self.help_composition_edit(identity, &state).await?;
        if let Some(draft) = self
            .composition_edit_receipt(identity, operation, &marker)
            .await?
        {
            return Ok(draft);
        }
        if state.draft.record.revision != edit.expected_revision {
            return Err(CompositionError::Conflict);
        }
        let mut next = edited_state(&state, edit, marker)?;
        if let Some(parent) = next.draft.record.parent {
            validate_schema_extension(
                &self.composition_parent_schema(identity, parent).await?,
                &next.draft.schema,
            )?;
        }
        self.validate_composition_splits(identity, &next.draft).await?;
        self.renew_composition_source(identity, &next.draft).await?;
        next.draft.record.source_lease_deadline = super::state::now().saturating_add(3600);
        next.encode()?;
        // Edit IDs own immutable pages. Losing a root CAS cannot occupy the winner's ordinal.
        let mut page_key = key::scope(b"DS\0CE\0", identity, operation)?;
        page_key.extend_from_slice(edit.id.as_bytes());
        self.put_composition(&page_key, &bytes).await?;
        self.replace_composition(identity, &old, &state, &next).await?;
        Ok(next.draft)
    }
    async fn composition_edit_receipt(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        marker: &EditMarker,
    ) -> Result<Option<CompositionDraft>, CompositionError> {
        let Some(stored) = self
            .composition_get(&key::edit_receipt(identity, operation, marker.id)?)
            .await?
        else {
            return Ok(None);
        };
        let receipt: EditReceipt = crate::split::codec::decode(&stored.bytes)?;
        receipt.draft.record.validate()?;
        if receipt.draft.record.operation != operation {
            return Err(CompositionError::Corrupt);
        }
        if receipt.marker != *marker {
            return Err(CompositionError::Conflict);
        }
        Ok(Some(receipt.draft))
    }
    /// Abandons an unpublished composition and releases only its owned source lease.
    /// # Errors
    /// Rejects stale revisions and publications in progress or already committed.
    pub async fn abandon_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        expected_revision: u64,
    ) -> Result<CompositionDraft, CompositionError> {
        let (old, state) = self.load_composition(identity, operation).await?;
        if state.draft.record.revision != expected_revision {
            return Err(CompositionError::Conflict);
        }
        if matches!(
            state.draft.record.progress,
            DraftProgress::Committing { .. } | DraftProgress::Published { .. }
        ) {
            return Err(CompositionError::Frozen);
        }
        let mut next = state.clone();
        next.draft.record.progress = DraftProgress::Abandoned;
        next.edit = None;
        self.replace_composition(identity, &old, &state, &next).await?;
        if let Some(parent) = next.draft.record.parent {
            self.release_read_handle(identity, parent, operation).await?;
        }
        Ok(next.draft)
    }
}
fn edited_state(
    state: &CompositionState,
    edit: &CompositionEdit,
    marker: EditMarker,
) -> Result<CompositionState, CompositionError> {
    let mut next = state.clone();
    next.draft
        .record
        .progress
        .apply_edit(&mut next.draft.record.revision, edit.expected_revision)
        .map_err(|_| CompositionError::Frozen)?;
    if let Some(schema) = &edit.schema {
        next.draft.schema.clone_from(schema);
    }
    if let Some(splits) = &edit.splits {
        next.draft.splits.clone_from(splits);
    }
    if let Some(policy) = edit.policy {
        next.draft.policy = policy;
    }
    validate_changes(&next.draft.schema, &edit.samples)?;
    if next.journal.len() >= 4096 {
        return Err(CompositionError::Limit);
    }
    next.journal.push(edit.id);
    next.draft.record.edit_pages += 1;
    next.edit = Some(marker);
    next.evaluation = None;
    Ok(next)
}
pub(super) fn validate_schema_extension(
    parent: &SchemaRecord,
    schema: &SchemaRecord,
) -> Result<(), CompositionError> {
    super::state::validate_schema(schema)?;
    if parent
        .fields
        .iter()
        .filter(|f| f.required)
        .any(|p| !schema.fields.iter().any(|f| f.name == p.name && f.required))
    {
        return Err(CompositionError::Conflict);
    }
    Ok(())
}
fn validate_changes(schema: &SchemaRecord, changes: &[SampleChange]) -> Result<(), CompositionError> {
    if changes.len() > 20 {
        return Err(CompositionError::Limit);
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut partial = schema.clone();
    for field in &mut partial.fields {
        field.required = false;
    }
    for change in changes {
        let id = match change {
            SampleChange::Upsert(sample) => &sample.sample_id,
            SampleChange::Remove(id) => id,
        };
        if id.is_empty() || id.len() > 16 * 1024 || !ids.insert(id) {
            return Err(CompositionError::Conflict);
        }
        if let SampleChange::Upsert(sample) = change {
            if sample.fields.iter().any(|f| matches!(&f.value, crate::FieldLocator::Chunk { location, .. } if location.len() > 64 * 1024)) { return Err(CompositionError::Limit); }
            ManifestRecord {
                version: 1,
                snapshot: SnapshotId::random(),
                schema: partial.clone(),
                samples: vec![sample.clone()],
            }
            .validate()
            .map_err(AuthorityError::from)?;
        }
    }
    Ok(())
}
