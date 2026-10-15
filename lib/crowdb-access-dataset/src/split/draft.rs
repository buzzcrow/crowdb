// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{codec, MembershipOrigin, SplitDefinition, SplitError, SplitName, SplitPolicy};
use crate::{composition::DraftProgress, CommitTarget, OperationId, SnapshotId, SplitRevisionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DraftSplit {
    pub name: SplitName,
    pub definition: SplitDefinition,
    pub origin: MembershipOrigin,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitDraftRecord {
    pub version: u16,
    pub operation: OperationId,
    pub snapshot: SnapshotId,
    #[serde(deserialize_with = "required_revision")]
    pub expected_revision: Option<SplitRevisionId>,
    pub revision: u64,
    pub policy: SplitPolicy,
    pub splits: Vec<DraftSplit>,
    pub progress: DraftProgress,
}

impl SplitDraftRecord {
    /// # Errors
    /// Rejects malformed identities, duplicate definitions and stale evaluation.
    pub fn validate(&self) -> Result<(), SplitError> {
        if self.version != 1 {
            return Err(SplitError::InvalidRecord);
        }
        SnapshotId::from_bytes(self.snapshot.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        OperationId::from_bytes(self.operation.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        if let Some(revision) = self.expected_revision {
            SplitRevisionId::from_bytes(revision.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        }
        self.progress.validate(self.revision)?;
        if self.splits.len() > 64 {
            return Err(SplitError::Limit);
        }
        let mut names = std::collections::BTreeSet::new();
        for split in &self.splits {
            if !names.insert(&split.name) {
                return Err(SplitError::InvalidRecord);
            }
            split.definition.validate()?;
            if let MembershipOrigin::Inherited(source) = &split.origin {
                SnapshotId::from_bytes(source.snapshot.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
                SplitRevisionId::from_bytes(source.revision.as_bytes())
                    .map_err(|_| SplitError::InvalidRecord)?;
                if source.snapshot == self.snapshot {
                    return Err(SplitError::InvalidRecord);
                }
            }
        }
        if let DraftProgress::Prepared { target }
        | DraftProgress::Published { target, .. }
        | DraftProgress::Committing { target, .. } = self.progress
        {
            if !matches!(target, CommitTarget::SplitRevision { snapshot, .. } if snapshot == self.snapshot) {
                return Err(SplitError::InvalidRecord);
            }
        }
        Ok(())
    }

    /// # Errors
    /// Rejects malformed or oversized draft records.
    pub fn encode(&self) -> Result<Vec<u8>, SplitError> {
        self.validate()?;
        codec::encode(self)
    }

    /// # Errors
    /// Rejects unsupported, corrupt or oversized draft records.
    pub fn decode(bytes: &[u8]) -> Result<Self, SplitError> {
        let record: Self = codec::decode(bytes)?;
        record.validate()?;
        Ok(record)
    }
}

fn required_revision<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<SplitRevisionId>, D::Error> {
    Option::deserialize(d)
}
