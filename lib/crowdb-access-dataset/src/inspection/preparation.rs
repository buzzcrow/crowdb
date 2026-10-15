// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::rows::IndexPosition;
use super::{InspectionState, SnapshotSummary};
use crate::{AuthorityError, MetadataScanBudget, SnapshotId, StoreError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum InspectionError {
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error(transparent)]
    Store(StoreError),
    #[error("inspection work exceeds its configured limit")]
    Limit,
    #[error("inspection metadata is not prepared")]
    NotPrepared,
    #[error("inspection metadata is malformed")]
    Corrupt,
    #[error("inspection work was cancelled")]
    Cancelled,
}

#[derive(Clone, Copy, Debug)]
pub struct PreparationLimits {
    pub rows: usize,
    pub bytes: usize,
    pub legacy_bytes: usize,
}
impl Default for PreparationLimits {
    fn default() -> Self {
        Self {
            rows: 100,
            bytes: 1024 * 1024,
            legacy_bytes: 16 * 1024 * 1024,
        }
    }
}
impl PreparationLimits {
    pub(super) fn validate(self) -> Result<(), InspectionError> {
        if self.rows == 0
            || self.rows > 100
            || self.bytes == 0
            || self.bytes > 1024 * 1024
            || self.legacy_bytes == 0
            || self.legacy_bytes > 16 * 1024 * 1024
        {
            return Err(InspectionError::Limit);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum PreparationState {
    Preparing,
    Ready,
    Unsupported { reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionPreparation {
    pub snapshot: SnapshotId,
    pub revision: u64,
    pub rows_written: u64,
    pub state: PreparationState,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparationRecord {
    pub version: u16,
    pub source_sha256: [u8; 32],
    pub progress: InspectionPreparation,
    pub position: IndexPosition,
}

impl From<StoreError> for InspectionError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Limit => Self::Limit,
            StoreError::Invalid(crate::DatasetError::ReadCancelled) => Self::Cancelled,
            error => Self::Store(error),
        }
    }
}

pub(super) fn decode_preparation(
    bytes: &[u8],
    snapshot: SnapshotId,
) -> Result<PreparationRecord, InspectionError> {
    if bytes.len() > 16 * 1024 {
        return Err(InspectionError::Corrupt);
    }
    let record: PreparationRecord = serde_json::from_slice(bytes).map_err(|_| InspectionError::Corrupt)?;
    if record.version != 1
        || record.progress.snapshot != snapshot
        || record.progress.rows_written != record.position.rows
    {
        return Err(InspectionError::Corrupt);
    }
    Ok(record)
}
pub(super) fn summary_progress(summary: &SnapshotSummary) -> InspectionPreparation {
    InspectionPreparation {
        snapshot: summary.snapshot,
        revision: 0,
        rows_written: 0,
        state: match &summary.state {
            InspectionState::Ready => PreparationState::Ready,
            InspectionState::Unsupported { reason } => PreparationState::Unsupported {
                reason: reason.clone(),
            },
        },
    }
}
pub(super) fn budget(bytes: usize) -> MetadataScanBudget {
    MetadataScanBudget {
        items: 1,
        bytes,
        timeout: Duration::from_secs(2),
    }
}
