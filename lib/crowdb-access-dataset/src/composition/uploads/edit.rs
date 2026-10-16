// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    state::{now, EditMarker},
    CompositionDraft, CompositionEdit, CompositionError, DraftProgress, SampleChange,
};
use crate::{DatasetAuthority, DatasetIdentity, DatasetUploads, FieldLocator, OperationId};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, time::Duration};

impl DatasetAuthority {
    /// Registers every staging owner before claiming uploaded fields and applying an edit.
    /// An interrupted claim is tracked even if the content edit never becomes visible.
    /// # Errors
    /// Rejects stale/frozen edits, unmanaged drafts, invalid uploads and storage limits.
    pub async fn edit_composition_with_uploads(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        edit: &CompositionEdit,
        uploads: &DatasetUploads,
    ) -> Result<CompositionDraft, CompositionError> {
        let marker = EditMarker {
            id: edit.id,
            digest: Sha256::digest(crate::split::codec::encode(edit)?).into(),
        };
        if let Some(draft) = self
            .composition_edit_receipt(identity, operation, &marker)
            .await?
        {
            return Ok(draft);
        }
        tokio::time::timeout(
            Duration::from_secs(2),
            self.claim_edit_uploads(identity, operation, edit, uploads),
        )
        .await
        .map_err(|_| CompositionError::Limit)??;
        self.edit_composition(identity, operation, edit).await
    }
    async fn claim_edit_uploads(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        edit: &CompositionEdit,
        uploads: &DatasetUploads,
    ) -> Result<(), CompositionError> {
        let (old, state) = self.load_composition(identity, operation).await?;
        if state.draft.record.revision != edit.expected_revision {
            return Err(CompositionError::Conflict);
        }
        if !matches!(
            state.draft.record.progress,
            DraftProgress::Editing | DraftProgress::Evaluating { .. } | DraftProgress::Evaluated { .. }
        ) {
            return Err(CompositionError::Frozen);
        }
        let mut ids = BTreeSet::new();
        for change in &edit.samples {
            if let SampleChange::Upsert(sample) = change {
                for field in &sample.fields {
                    if let FieldLocator::Chunk {
                        location,
                        length,
                        md5,
                    } = &field.value
                    {
                        let id = DatasetUploads::upload_id(identity, location)?;
                        let record = uploads.metadata(location).await?;
                        if record.length != *length || record.md5 != *md5 {
                            return Err(crate::UploadError::Integrity.into());
                        }
                        ids.insert(id);
                    }
                }
            }
        }
        let mut next = state.clone();
        next.edit = None;
        let ownership = next.uploads.as_mut().ok_or(CompositionError::Conflict)?;
        let mut registered: BTreeSet<_> = ownership.staged.iter().copied().collect();
        registered.extend(ids.iter().copied());
        ownership.staged = registered.into_iter().collect();
        self.replace_composition(identity, &old, &state, &next).await?;
        let owner = super::staging_owner(operation)?;
        for id in ids {
            uploads
                .claim(&DatasetUploads::location(identity, id)?, owner, now())
                .await?;
        }
        Ok(())
    }
}
