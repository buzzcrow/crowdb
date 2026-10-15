// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{codec, SplitDraftRecord, SplitLifecycleError};
use crate::{
    CasOutcome, DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId, ReadCancellation,
    StoredValue,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    #[serde(default)]
    pub initial: bool,
    pub edit: Option<EditMarker>,
    #[serde(default)]
    pub evaluation: Option<super::evaluation::EvaluationProgress>,
    #[serde(default)]
    pub source_leases: Vec<crate::SnapshotId>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EditReceipt {
    pub marker: EditMarker,
    pub record: SplitDraftRecord,
}

impl DraftState {
    pub(super) fn encode(&self) -> Result<Vec<u8>, SplitLifecycleError> {
        self.record.validate()?;
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
    fn decode(bytes: &[u8], operation: OperationId) -> Result<Self, SplitLifecycleError> {
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
                    bytes: 1024 * 1024 + 64 * 1024,
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
    pub(super) async fn help_split_edit(
        &self,
        identity: &DatasetIdentity,
        state: &DraftState,
    ) -> Result<(), SplitLifecycleError> {
        if let Some(marker) = &state.edit {
            let receipt = EditReceipt {
                marker: marker.clone(),
                record: state.record.clone(),
            };
            self.put_split_state(
                &state_key(b"DS\0SX\0", identity, state.record.operation, Some(marker.id))?,
                &codec::encode(&receipt)?,
            )
            .await?;
        }
        Ok(())
    }
    pub(super) async fn replace_split_draft(
        &self,
        identity: &DatasetIdentity,
        old: &StoredValue,
        state: &DraftState,
        next: &DraftState,
    ) -> Result<(), SplitLifecycleError> {
        self.help_split_edit(identity, state).await?;
        let bytes = next.encode()?;
        // Validate receipt serialization before making the edit observable.
        if let Some(marker) = &next.edit {
            codec::encode(&EditReceipt {
                marker: marker.clone(),
                record: next.record.clone(),
            })?;
        }
        match self
            .store
            .compare_exchange(
                &state_key(b"DS\0SD\0", identity, state.record.operation, None)?,
                Some(&old.bytes),
                &bytes,
            )
            .await?
        {
            CasOutcome::Applied(_) => self.help_split_edit(identity, next).await,
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => {
                self.help_split_edit(identity, next).await
            }
            CasOutcome::Conflict(_) => Err(SplitLifecycleError::Conflict),
        }
    }
    pub(super) async fn renew_split_draft_lease(
        &self,
        identity: &DatasetIdentity,
        state: &DraftState,
    ) -> Result<(), SplitLifecycleError> {
        let record = &state.record;
        if state.initial {
            self.require_initial_split_owner(identity, record.snapshot, record.operation)
                .await?;
        } else {
            self.require_committed_snapshot(identity, record.snapshot).await?;
            self.validate_split_sources(identity, record).await?;
        }
        for source in super::inheritance::source_owners(record) {
            self.touch_read_handle(identity, source, record.operation, now(), 3600)
                .await?;
        }
        self.touch_read_handle(identity, record.snapshot, record.operation, now(), 3600)
            .await?;
        Ok(())
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
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
