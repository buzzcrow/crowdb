// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable attempt and receipt identities, independent of Snapshot ancestry.

use crate::{AuthorityError, OperationId, SnapshotId, SplitRevisionId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_COMMIT_RECORD_BYTES: usize = 4096;

/// Opaque comparison token for the exact stored head, including its fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitGeneration([u8; 32]);

impl CommitGeneration {
    #[must_use]
    pub fn from_head(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CommitTarget {
    Snapshot {
        snapshot: SnapshotId,
    },
    SplitRevision {
        snapshot: SnapshotId,
        revision: SplitRevisionId,
    },
}

impl CommitTarget {
    pub(crate) fn validate(self) -> Result<(), AuthorityError> {
        let snapshot = match self {
            Self::Snapshot { snapshot } => snapshot,
            Self::SplitRevision { snapshot, revision } => {
                SplitRevisionId::from_bytes(revision.as_bytes())?;
                snapshot
            }
        };
        SnapshotId::from_bytes(snapshot.as_bytes())?;
        Ok(())
    }
}

/// An attempt may be retried unchanged. A new head precondition needs a new ID,
/// while the immutable candidate target remains the same.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitAttempt {
    pub version: u16,
    pub id: OperationId,
    pub target: CommitTarget,
    #[serde(deserialize_with = "required_generation")]
    pub expected_head: Option<CommitGeneration>,
}

impl CommitAttempt {
    /// # Errors
    /// Rejects unsupported record versions and malformed target identities.
    pub fn validate(&self) -> Result<(), AuthorityError> {
        if self.version != 1 {
            return Err(AuthorityError::Corrupt);
        }
        OperationId::from_bytes(self.id.as_bytes())?;
        self.target.validate()
    }

    /// # Errors
    /// Rejects invalid commit identities and versions.
    pub fn encode(&self) -> Result<Vec<u8>, AuthorityError> {
        self.validate()?;
        encode(self)
    }

    /// # Errors
    /// Rejects malformed, oversized, unsupported or incomplete attempt records.
    pub fn decode(bytes: &[u8]) -> Result<Self, AuthorityError> {
        let record: Self = decode(bytes)?;
        record.validate()?;
        Ok(record)
    }
}

/// Written only from a successful head CAS or by a helper observing its marker.
/// Success remains durable after the current head moves to a different branch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessfulCommit {
    pub version: u16,
    pub attempt: CommitAttempt,
    pub committed_head: CommitGeneration,
}

impl SuccessfulCommit {
    /// # Errors
    /// Rejects unsupported or malformed successful commit records.
    pub fn encode(&self) -> Result<Vec<u8>, AuthorityError> {
        if self.version != 1 {
            return Err(AuthorityError::Corrupt);
        }
        self.attempt.validate()?;
        encode(self)
    }

    /// # Errors
    /// Rejects malformed, oversized, unsupported or incomplete receipt records.
    pub fn decode(bytes: &[u8]) -> Result<Self, AuthorityError> {
        let record: Self = decode(bytes)?;
        if record.version != 1 {
            return Err(AuthorityError::Corrupt);
        }
        record.attempt.validate()?;
        Ok(record)
    }
}

fn required_generation<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<CommitGeneration>, D::Error> {
    Option::<CommitGeneration>::deserialize(d)
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, AuthorityError> {
    // These records contain fixed-width identities only, never unbounded payloads.
    let bytes = serde_json::to_vec(value).map_err(|_| AuthorityError::Corrupt)?;
    if bytes.len() > MAX_COMMIT_RECORD_BYTES {
        return Err(AuthorityError::Corrupt);
    }
    Ok(bytes)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, AuthorityError> {
    if bytes.len() > MAX_COMMIT_RECORD_BYTES {
        return Err(AuthorityError::Corrupt);
    }
    serde_json::from_slice(bytes).map_err(|_| AuthorityError::Corrupt)
}
