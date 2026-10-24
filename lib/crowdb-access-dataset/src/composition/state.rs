// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, CompositionDraft, CompositionError};
use crate::{
    split::{DraftSplit, MembershipOrigin},
    AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId,
    ReadCancellation, StoredValue,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct EditMarker {
    pub id: OperationId,
    pub digest: [u8; 32],
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct CompositionState {
    pub draft: CompositionDraft,
    pub edit: Option<EditMarker>,
    pub journal: Vec<OperationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registered_edits: Vec<OperationId>,
    pub evaluation: Option<super::evaluation::Evaluation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uploads: Option<super::uploads::UploadOwnership>,
}
impl CompositionState {
    pub(super) fn encode(&self) -> Result<Vec<u8>, CompositionError> {
        self.draft.record.validate()?;
        if let Some(uploads) = &self.uploads {
            if !matches!(self.draft.record.version, 2 | 3) {
                return Err(CompositionError::Corrupt);
            }
            uploads.validate()?;
        } else if self.draft.record.version == 2 {
            return Err(CompositionError::Corrupt);
        }
        if self.registered_edits.len() > 4096
            || self.registered_edits.windows(2).any(|pair| pair[0] >= pair[1])
            || (!self.registered_edits.is_empty() && self.draft.record.version != 3)
        {
            return Err(CompositionError::Corrupt);
        }
        if self.journal.len() > 4096 || self.journal.len() as u64 != self.draft.record.edit_pages {
            return Err(CompositionError::Corrupt);
        }
        if let Some(evaluation) = &self.evaluation {
            let preview = &evaluation.preview;
            if preview.input_revision != self.draft.record.revision
                || preview.complete != matches!(evaluation.phase, super::evaluation::Phase::Done)
                || matches!(self.draft.record.progress, super::DraftProgress::Evaluating { next_page, .. } if next_page != preview.next_page)
                || matches!(self.draft.record.progress, super::DraftProgress::Evaluated { binding, .. } if !preview.complete || binding != preview.binding)
            {
                return Err(CompositionError::Corrupt);
            }
        } else if matches!(
            self.draft.record.progress,
            super::DraftProgress::Evaluating { .. } | super::DraftProgress::Evaluated { .. }
        ) {
            return Err(CompositionError::Corrupt);
        }
        validate_schema(&self.draft.schema)?;
        validate_splits(&self.draft.splits)?;
        Ok(crate::split::codec::encode(self)?)
    }
    fn decode(bytes: &[u8], operation: OperationId) -> Result<Self, CompositionError> {
        let state: Self = crate::split::codec::decode(bytes)?;
        state.encode()?;
        if state.draft.record.operation != operation {
            return Err(CompositionError::Corrupt);
        }
        Ok(state)
    }
}
impl DatasetAuthority {
    pub(super) async fn composition_get(&self, key: &[u8]) -> Result<Option<StoredValue>, CompositionError> {
        Ok(tokio::time::timeout(
            Duration::from_secs(2),
            self.store.get_bounded(
                key,
                MetadataScanBudget {
                    items: 1,
                    bytes: 1024 * 1024 + 64 * 1024,
                    timeout: Duration::from_secs(2),
                },
                &ReadCancellation::new(),
            ),
        )
        .await
        .map_err(|_| CompositionError::Limit)??)
    }
    pub(super) async fn load_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<(StoredValue, CompositionState), CompositionError> {
        let stored = self
            .composition_get(&key::scope(b"DS\0CD\0", identity, operation)?)
            .await?
            .ok_or(CompositionError::NotFound)?;
        let state = CompositionState::decode(&stored.bytes, operation)?;
        Ok((stored, state))
    }
    pub(super) async fn replace_composition(
        &self,
        identity: &DatasetIdentity,
        old: &StoredValue,
        state: &CompositionState,
        next: &CompositionState,
    ) -> Result<(), CompositionError> {
        let bytes = next.encode()?;
        match self
            .store
            .compare_exchange(
                &key::scope(b"DS\0CD\0", identity, state.draft.record.operation)?,
                Some(&old.bytes),
                &bytes,
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(CompositionError::Conflict),
        }
    }
}

pub(super) fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: Duration::from_secs(2),
    }
}
pub(super) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub(super) fn validate_schema(schema: &crate::SchemaRecord) -> Result<(), CompositionError> {
    if schema.fields.len() > 256 || schema.fields.iter().any(|f| f.name.len() > 1024) {
        return Err(CompositionError::Limit);
    }
    crate::ManifestRecord {
        version: 1,
        snapshot: crate::SnapshotId::random(),
        schema: schema.clone(),
        samples: vec![],
    }
    .validate()
    .map_err(AuthorityError::from)?;
    Ok(())
}
pub(super) fn validate_splits(splits: &[DraftSplit]) -> Result<(), CompositionError> {
    if splits.len() > 64 {
        return Err(CompositionError::Limit);
    }
    let mut names = std::collections::BTreeSet::new();
    for split in splits {
        if !names.insert(&split.name) {
            return Err(CompositionError::Conflict);
        }
        split.definition.validate()?;
        if let MembershipOrigin::Inherited(source) = &split.origin {
            crate::SnapshotId::from_bytes(source.snapshot.as_bytes()).map_err(AuthorityError::from)?;
            crate::SplitRevisionId::from_bytes(source.revision.as_bytes()).map_err(AuthorityError::from)?;
        }
    }
    Ok(())
}
