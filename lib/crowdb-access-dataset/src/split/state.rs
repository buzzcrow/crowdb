// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{codec, SplitDraftRecord, SplitLifecycleError};
use crate::{
    CasOutcome, DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId, ReadCancellation,
    StoredValue,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EditMarker {
    pub id: OperationId,
    pub digest: [u8; 32],
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DraftState {
    pub record: SplitDraftRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_version: Option<u16>,
    #[serde(default)]
    pub initial: bool,
    pub edit: Option<EditMarker>,
    #[serde(default)]
    pub evaluation: Option<super::evaluation::EvaluationProgress>,
    #[serde(default)]
    pub source_leases: Vec<crate::SnapshotId>,
}
impl DraftState {
    pub(super) fn encode(&self) -> Result<Vec<u8>, SplitLifecycleError> {
        self.record.validate()?;
        if self.work_version.is_some_and(|version| version != 1) {
            return Err(SplitLifecycleError::Corrupt);
        }
        if self.source_leases.len() > 1
            || super::inheritance::source_owners(&self.record)
                .iter()
                .any(|id| !self.source_leases.contains(id))
        {
            return Err(SplitLifecycleError::Corrupt);
        }
        for source in &self.source_leases {
            crate::SnapshotId::from_bytes(source.as_bytes()).map_err(crate::AuthorityError::from)?;
        }
        if let Some(evaluation) = &self.evaluation {
            evaluation.validate(self)?;
        } else if matches!(
            self.record.progress,
            crate::composition::DraftProgress::Evaluating { .. }
                | crate::composition::DraftProgress::Evaluated { .. }
        ) {
            return Err(SplitLifecycleError::Corrupt);
        }
        if let Some(edit) = &self.edit {
            OperationId::from_bytes(edit.id.as_bytes()).map_err(crate::AuthorityError::from)?;
        }
        Ok(codec::encode(self)?)
    }
    pub(super) fn decode(bytes: &[u8], operation: OperationId) -> Result<Self, SplitLifecycleError> {
        let state: Self = codec::decode(bytes)?;
        state.encode()?;
        if state.record.operation != operation {
            return Err(SplitLifecycleError::Corrupt);
        }
        Ok(state)
    }
}
impl DatasetAuthority {
    pub(super) async fn load_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<(StoredValue, DraftState), SplitLifecycleError> {
        let stored = self
            .split_state_get(&state_key(b"DS\0SD\0", identity, operation, None)?)
            .await?
            .ok_or(SplitLifecycleError::NotFound)?;
        let state = DraftState::decode(&stored.bytes, operation)?;
        Ok((stored, state))
    }
    pub(super) async fn split_state_get(
        &self,
        key: &[u8],
    ) -> Result<Option<StoredValue>, SplitLifecycleError> {
        Ok(tokio::time::timeout(
            Duration::from_secs(2),
            self.store.get_bounded(
                key,
                MetadataScanBudget {
                    items: 1,
                    bytes: super::retirement::resource::RESOURCE_CAP + 64 * 1024,
                    timeout: Duration::from_secs(2),
                },
                &ReadCancellation::new(),
            ),
        )
        .await
        .map_err(|_| SplitLifecycleError::Limit)??)
    }
    pub(super) async fn put_split_state(&self, key: &[u8], bytes: &[u8]) -> Result<(), SplitLifecycleError> {
        match self.store.compare_exchange(key, None, bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(SplitLifecycleError::Conflict),
        }
    }
    pub(super) async fn replace_split_draft(
        &self,
        identity: &DatasetIdentity,
        old: &StoredValue,
        state: &DraftState,
        next: &DraftState,
    ) -> Result<(), SplitLifecycleError> {
        let bytes = next.encode()?;
        match self
            .store
            .compare_exchange(
                &state_key(b"DS\0SD\0", identity, state.record.operation, None)?,
                Some(&old.bytes),
                &bytes,
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(SplitLifecycleError::Conflict),
        }
    }
}
pub(super) fn state_key(
    prefix: &[u8],
    identity: &DatasetIdentity,
    operation: OperationId,
    edit: Option<OperationId>,
) -> Result<Vec<u8>, SplitLifecycleError> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&identity.key().map_err(crate::AuthorityError::from)?);
    key.extend_from_slice(operation.as_bytes());
    if let Some(edit) = edit {
        key.extend_from_slice(edit.as_bytes());
    }
    Ok(key)
}
