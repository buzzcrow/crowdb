// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    codec,
    state::{state_key, DraftState, EditMarker, EditReceipt},
    DraftSplit, SplitDraftRecord, SplitError, SplitPolicy,
};
use crate::{
    composition::DraftProgress, AuthorityError, DatasetAuthority, DatasetIdentity, OperationId, SnapshotId,
    SplitRevisionId, StoreError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum SplitLifecycleError {
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Split(#[from] SplitError),
    #[error(transparent)]
    Inspection(#[from] crate::inspection::InspectionError),
    #[error("Split operation was not found")]
    NotFound,
    #[error("Split draft or publication precondition changed")]
    Conflict,
    #[error("Split operation metadata is corrupt")]
    Corrupt,
    #[error("Split operation exceeded its work limit")]
    Limit,
    #[error("Split draft has no complete current evaluation")]
    NotEvaluated,
    #[error("Split overlap or coverage policy is violated")]
    PolicyViolation,
    #[error("Split draft cannot be edited in its current state")]
    Frozen,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitDraftEdit {
    pub id: OperationId,
    pub expected_revision: u64,
    pub policy: SplitPolicy,
    pub splits: Vec<DraftSplit>,
}
impl DatasetAuthority {
    /// Begins a whole-set revision draft with immutable creation identity.
    /// # Errors
    /// Rejects unavailable owners, invalid definitions and divergent operation retries.
    pub async fn begin_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        snapshot: SnapshotId,
        expected_revision: Option<SplitRevisionId>,
        policy: SplitPolicy,
        splits: Vec<DraftSplit>,
    ) -> Result<SplitDraftRecord, SplitLifecycleError> {
        let record = SplitDraftRecord {
            version: 1,
            operation,
            snapshot,
            expected_revision,
            revision: 1,
            policy,
            splits,
            progress: DraftProgress::Editing,
        };
        let initial = record.encode()?;
        self.validate_split_sources(identity, &record).await?;
        self.require_committed_snapshot(identity, snapshot).await?;
        self.put_split_state(&state_key(b"DS\0SB\0", identity, operation, None)?, &initial)
            .await?;
        let source_leases = super::inheritance::source_owners(&record).into_iter().collect();
        let state = DraftState {
            work_version: Some(1),
            record,
            initial: false,
            source_leases,
            edit: None,
            evaluation: None,
        };
        let key = state_key(b"DS\0SD\0", identity, operation, None)?;
        // A repeated begin can observe an already edited draft, but not change its original input.
        if self.split_state_get(&key).await?.is_none() {
            match self.put_split_state(&key, &state.encode()?).await {
                Ok(()) | Err(SplitLifecycleError::Conflict) => {}
                Err(error) => return Err(error),
            }
        }
        let (_, current) = self.load_split_draft(identity, operation).await?;
        if !matches!(
            current.record.progress,
            DraftProgress::Abandoned | DraftProgress::Published { .. }
        ) {
            self.check_split_draft_sources(identity, &current).await?;
        }
        Ok(current.record)
    }

    /// Reads the durable editing/evaluation/preparation state after restart.
    /// # Errors
    /// Returns unknown operations, corrupt records or storage failures.
    pub async fn split_draft_status(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<SplitDraftRecord, SplitLifecycleError> {
        self.reconcile_split_draft(identity, operation).await
    }

    /// Replaces the whole definition set under a draft revision CAS.
    /// Edits invalidate evaluation and preserve exact retry identity across later edits.
    /// # Errors
    /// Rejects stale/divergent edits, frozen drafts and unavailable owners.
    pub async fn edit_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        edit: &SplitDraftEdit,
    ) -> Result<SplitDraftRecord, SplitLifecycleError> {
        OperationId::from_bytes(edit.id.as_bytes()).map_err(AuthorityError::from)?;
        let marker = EditMarker {
            id: edit.id,
            digest: Sha256::digest(codec::encode(edit)?).into(),
        };
        if let Some(record) = self.split_edit_receipt(identity, operation, &marker).await? {
            return Ok(record);
        }
        let (old, state) = self.load_split_draft(identity, operation).await?;
        if state.initial {
            return Err(SplitLifecycleError::Frozen);
        }
        self.help_split_edit(identity, &state).await?;
        if let Some(record) = self.split_edit_receipt(identity, operation, &marker).await? {
            return Ok(record);
        }
        if state.record.revision != edit.expected_revision {
            return Err(SplitLifecycleError::Conflict);
        }
        self.check_split_draft_sources(identity, &state).await?;
        let mut next = state.clone();
        next.record
            .progress
            .apply_edit(&mut next.record.revision, edit.expected_revision)
            .map_err(|_| SplitLifecycleError::Frozen)?;
        next.record.policy = edit.policy;
        next.record.splits.clone_from(&edit.splits);
        next.edit = Some(marker);
        next.evaluation = None;
        self.validate_split_sources(identity, &next.record).await?;
        let mut sources: std::collections::BTreeSet<_> = next.source_leases.iter().copied().collect();
        sources.extend(super::inheritance::source_owners(&next.record));
        next.source_leases = sources.into_iter().collect();
        self.replace_split_draft(identity, &old, &state, &next).await?;
        self.check_split_draft_sources(identity, &next).await?;
        Ok(next.record)
    }

    async fn split_edit_receipt(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        marker: &EditMarker,
    ) -> Result<Option<SplitDraftRecord>, SplitLifecycleError> {
        let Some(value) = self
            .split_state_get(&state_key(b"DS\0SX\0", identity, operation, Some(marker.id))?)
            .await?
        else {
            return Ok(None);
        };
        let receipt: EditReceipt = codec::decode(&value.bytes)?;
        receipt.record.validate()?;
        if receipt.record.operation != operation {
            return Err(SplitLifecycleError::Corrupt);
        }
        if receipt.marker != *marker {
            return Err(SplitLifecycleError::Conflict);
        }
        Ok(Some(receipt.record))
    }

    /// Freezes an unpublished draft as abandoned and releases its owner lease.
    /// # Errors
    /// Rejects stale revisions or an already published draft.
    pub async fn abandon_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        expected_revision: u64,
    ) -> Result<SplitDraftRecord, SplitLifecycleError> {
        let (old, state) = self.load_split_draft(identity, operation).await?;
        if state.initial {
            return Err(SplitLifecycleError::Frozen);
        }
        if state.record.revision != expected_revision {
            return Err(SplitLifecycleError::Conflict);
        }
        if matches!(
            state.record.progress,
            DraftProgress::Published { .. } | DraftProgress::Committing { .. }
        ) {
            return Err(SplitLifecycleError::Frozen);
        }
        let mut next = state.clone();
        next.record.progress = DraftProgress::Abandoned;
        next.edit = None;
        self.replace_split_draft(identity, &old, &state, &next).await?;

        Ok(next.record)
    }
}
