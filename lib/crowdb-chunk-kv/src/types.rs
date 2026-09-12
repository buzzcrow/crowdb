// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_stream::StreamName;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ChunkKvError, Result};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionId {
    pub high: u64,
    pub low: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RequestId {
    pub client_high: u64,
    pub client_low: u64,
    pub client_sequence: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRange {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
}

impl PartitionRange {
    /// Validates that the range is nonempty when both endpoints are bounded.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkKvError::InvalidRequest`] for reversed or empty bounds.
    pub fn validate(&self) -> Result<()> {
        if self
            .start
            .as_ref()
            .zip(self.end.as_ref())
            .is_some_and(|(start, end)| start >= end)
        {
            return Err(ChunkKvError::InvalidRequest(
                "partition range must be nonempty".into(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_deref().map_or(true, |start| key >= start)
            && self.end.as_deref().map_or(true, |end| key < end)
    }

    #[must_use]
    pub fn contains_interval(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
        let lower = match (self.start.as_deref(), start) {
            (Some(bound), Some(request)) => request >= bound,
            (Some(_), None) => false,
            _ => true,
        };
        let upper = match (self.end.as_deref(), end) {
            (Some(bound), Some(request)) => request <= bound,
            (Some(_), None) => false,
            _ => true,
        };
        lower && upper && start.zip(end).map_or(true, |(first, last)| first <= last)
    }

    /// Splits this range at one strictly interior key.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkKvError::InvalidRequest`] when `key` is not strictly
    /// inside the range.
    pub fn split(&self, key: &[u8]) -> Result<(Self, Self)> {
        if !self.contains(key) || self.start.as_deref() == Some(key) {
            return Err(ChunkKvError::InvalidRequest(
                "split key must be strictly interior".into(),
            ));
        }
        Ok((
            Self {
                start: self.start.clone(),
                end: Some(key.to_vec()),
            },
            Self {
                start: Some(key.to_vec()),
                end: self.end.clone(),
            },
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionLifecycle {
    Closed,
    Recovering,
    WriteStalled,
    Prepared,
    Serving,
    SplitPreparing,
    SplitFenced,
    Retired,
    Faulted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueRevision {
    pub revision: u64,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareCondition {
    Revision(u64),
    Value(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationOperation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    PutIfAbsent {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    CompareExchange {
        key: Vec<u8>,
        condition: CompareCondition,
        value: Vec<u8>,
    },
    ConditionalDelete {
        key: Vec<u8>,
        condition: CompareCondition,
    },
}

impl MutationOperation {
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. }
            | Self::Delete { key }
            | Self::PutIfAbsent { key, .. }
            | Self::CompareExchange { key, .. }
            | Self::ConditionalDelete { key, .. } => key,
        }
    }

    #[must_use]
    pub fn successful_value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. }
            | Self::PutIfAbsent { value, .. }
            | Self::CompareExchange { value, .. } => Some(value),
            Self::Delete { .. } | Self::ConditionalDelete { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationResult {
    Applied { revision: u64 },
    ConditionFailed { observed: Option<ValueRevision> },
}

impl MutationResult {
    #[must_use]
    pub fn applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalPosition {
    pub stream_name: StreamName,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub tree_id: u64,
    pub tree_manifest: u64,
    pub applied_seq: u64,
    pub stream_name: StreamName,
    pub replay_offset: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransitionId {
    pub high: u64,
    pub low: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitChild {
    pub partition_id: PartitionId,
    pub range: PartitionRange,
    pub ownership_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitPlan {
    pub transition_id: TransitionId,
    pub parent_id: PartitionId,
    pub parent_range: PartitionRange,
    pub parent_epoch: u64,
    pub split_key: Vec<u8>,
    pub left: SplitChild,
    pub right: SplitChild,
}

impl SplitPlan {
    /// Validates exact child identity, epoch, and half-open range coverage.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero identity/epoch, a non-interior split key,
    /// duplicate partition IDs, or child ranges that do not exactly cover the
    /// parent.
    pub fn validate(&self) -> Result<()> {
        self.parent_range.validate()?;
        self.left.range.validate()?;
        self.right.range.validate()?;
        if self.transition_id == TransitionId::default()
            || self.parent_epoch == 0
            || self.left.ownership_epoch == 0
            || self.right.ownership_epoch == 0
        {
            return Err(ChunkKvError::InvalidRequest(
                "split identities and epochs must be nonzero".into(),
            ));
        }
        if self.parent_id == self.left.partition_id
            || self.parent_id == self.right.partition_id
            || self.left.partition_id == self.right.partition_id
        {
            return Err(ChunkKvError::InvalidRequest(
                "split partition identities must be distinct".into(),
            ));
        }
        let (expected_left, expected_right) = self.parent_range.split(&self.split_key)?;
        if self.left.range != expected_left || self.right.range != expected_right {
            return Err(ChunkKvError::InvalidRequest(
                "split child ranges must exactly cover the parent".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedChildArtifact {
    pub partition_id: PartitionId,
    pub range: PartitionRange,
    pub ownership_epoch: u64,
    pub tree_id: u64,
    pub tree_manifest: u64,
    pub stream_name: StreamName,
    pub applied_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitArtifact {
    pub transition_id: TransitionId,
    pub parent_id: PartitionId,
    pub parent_epoch: u64,
    pub cutover_seq: u64,
    pub left: PreparedChildArtifact,
    pub right: PreparedChildArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitCommitProof {
    pub catalog_revision: u64,
    pub artifact: SplitArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitAbortProof {
    pub catalog_revision: u64,
    pub transition_id: TransitionId,
    pub parent_id: PartitionId,
    pub parent_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRecord {
    pub partition_id: PartitionId,
    pub ownership_epoch: u64,
    pub mutation_seq: u64,
    pub request_id: RequestId,
    pub operation_digest: [u8; 32],
    pub result: MutationResult,
    pub operation: MutationOperation,
}

#[must_use]
pub fn canonical_operation_digest(operation: &MutationOperation) -> [u8; 32] {
    let mut digest = Sha256::new();
    match operation {
        MutationOperation::Put { key, value } => {
            digest.update([0]);
            update_field(&mut digest, key);
            update_field(&mut digest, value);
        }
        MutationOperation::Delete { key } => {
            digest.update([1]);
            update_field(&mut digest, key);
        }
        MutationOperation::PutIfAbsent { key, value } => {
            digest.update([2]);
            update_field(&mut digest, key);
            update_field(&mut digest, value);
        }
        MutationOperation::CompareExchange {
            key,
            condition,
            value,
        } => {
            digest.update([3]);
            update_field(&mut digest, key);
            update_condition(&mut digest, condition);
            update_field(&mut digest, value);
        }
        MutationOperation::ConditionalDelete { key, condition } => {
            digest.update([4]);
            update_field(&mut digest, key);
            update_condition(&mut digest, condition);
        }
    }
    digest.finalize().into()
}

fn update_condition(digest: &mut Sha256, condition: &CompareCondition) {
    match condition {
        CompareCondition::Revision(revision) => {
            digest.update([0]);
            digest.update(revision.to_le_bytes());
        }
        CompareCondition::Value(value) => {
            digest.update([1]);
            update_field(digest, value);
        }
    }
}

fn update_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(value);
}
