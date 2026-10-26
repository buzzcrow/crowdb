// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{codec, MembershipOrigin, SplitDefinition, SplitError, SplitName, SplitPolicy};
use crate::{OperationId, SnapshotId, SplitRevisionId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_SPLIT_RECORD_BYTES: u64 = 1024 * 1024;
pub const MAX_MEMBERSHIP_PAGE_ITEMS: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipPageBinding {
    pub page: u32,
    pub count: u32,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedSplitRecord {
    pub name: SplitName,
    pub definition: SplitDefinition,
    pub origin: MembershipOrigin,
    pub count: u64,
    pub pages: Vec<MembershipPageBinding>,
}

/// Immutable complete-set binding. Its existence alone does not imply publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitRevisionRecord {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub revision: SplitRevisionId,
    pub operation: OperationId,
    pub evaluator_version: u16,
    pub policy: SplitPolicy,
    pub splits: Vec<NamedSplitRecord>,
}

impl SplitRevisionRecord {
    /// # Errors
    /// Rejects unsupported versions, identities, definitions and incomplete bindings.
    pub fn validate(&self) -> Result<(), SplitError> {
        validate_scope(self.version, self.snapshot, self.revision)?;
        OperationId::from_bytes(self.operation.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        if self.evaluator_version != 1 {
            return Err(SplitError::InvalidRecord);
        }
        let mut names = std::collections::BTreeSet::new();
        for split in &self.splits {
            if !names.insert(&split.name) {
                return Err(SplitError::InvalidRecord);
            }
            split.definition.validate()?;
            if let MembershipOrigin::Inherited(source) = &split.origin {
                validate_scope(1, source.snapshot, source.revision)?;
                if source.snapshot == self.snapshot {
                    return Err(SplitError::InvalidRecord);
                }
            }
            let mut count = 0u64;
            for (index, page) in split.pages.iter().enumerate() {
                if page.page as usize != index
                    || page.count == 0
                    || page.count as usize > MAX_MEMBERSHIP_PAGE_ITEMS
                {
                    return Err(SplitError::InvalidRecord);
                }
                count = count
                    .checked_add(u64::from(page.count))
                    .ok_or(SplitError::Limit)?;
            }
            if count != split.count {
                return Err(SplitError::InvalidRecord);
            }
        }
        Ok(())
    }

    /// # Errors
    /// Rejects invalid or oversized revision bindings.
    pub fn encode(&self) -> Result<Vec<u8>, SplitError> {
        self.validate()?;
        codec::encode(self)
    }

    /// # Errors
    /// Rejects malformed, oversized or unsupported records; no legacy revision exists.
    pub fn decode(bytes: &[u8]) -> Result<Self, SplitError> {
        let record: Self = codec::decode(bytes)?;
        record.validate()?;
        Ok(record)
    }
}

/// `None` differs from a revision whose complete named Split set is empty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitHead {
    pub version: u16,
    pub snapshot: SnapshotId,
    #[serde(deserialize_with = "required_revision")]
    pub revision: Option<SplitRevisionId>,
    pub fence: OperationId,
}

impl SplitHead {
    fn validate(&self) -> Result<(), SplitError> {
        if !matches!(self.version, 1 | 3) {
            return Err(SplitError::InvalidRecord);
        }
        SnapshotId::from_bytes(self.snapshot.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        OperationId::from_bytes(self.fence.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        if let Some(revision) = self.revision {
            SplitRevisionId::from_bytes(revision.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        }
        Ok(())
    }

    /// # Errors
    /// Rejects invalid identities and versions.
    pub fn encode(&self) -> Result<Vec<u8>, SplitError> {
        self.validate()?;
        codec::encode(self)
    }

    /// # Errors
    /// Rejects corrupt or unsupported head records.
    pub fn decode(bytes: &[u8]) -> Result<Self, SplitError> {
        let record: Self = codec::decode(bytes)?;
        record.validate()?;
        Ok(record)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipPage {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub revision: SplitRevisionId,
    pub name: SplitName,
    pub page: u32,
    pub members: Vec<Vec<u8>>,
}

impl MembershipPage {
    /// Binds the complete encoded page, including its Snapshot, revision and name.
    /// # Errors
    /// Rejects invalid or oversized pages.
    pub fn binding(&self) -> Result<MembershipPageBinding, SplitError> {
        Ok(MembershipPageBinding {
            page: self.page,
            count: u32::try_from(self.members.len()).map_err(|_| SplitError::Limit)?,
            sha256: Sha256::digest(self.encode()?).into(),
        })
    }

    /// # Errors
    /// Rejects a page whose scope, count, position or bytes differ from the binding.
    pub fn verify_binding(
        &self,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        name: &SplitName,
        binding: &MembershipPageBinding,
    ) -> Result<(), SplitError> {
        if self.snapshot != snapshot
            || self.revision != revision
            || &self.name != name
            || self.binding()? != *binding
        {
            return Err(SplitError::InvalidRecord);
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), SplitError> {
        validate_scope(self.version, self.snapshot, self.revision)?;
        if self.members.is_empty() || self.members.len() > MAX_MEMBERSHIP_PAGE_ITEMS {
            return Err(SplitError::Limit);
        }
        if self.members.iter().any(Vec::is_empty) || self.members.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(SplitError::InvalidRecord);
        }
        Ok(())
    }

    /// # Errors
    /// Rejects unordered, duplicate, empty or oversized membership pages.
    pub fn encode(&self) -> Result<Vec<u8>, SplitError> {
        self.validate()?;
        codec::encode(self)
    }

    /// # Errors
    /// Rejects corrupt, unbounded or unsupported membership pages.
    pub fn decode(bytes: &[u8]) -> Result<Self, SplitError> {
        let record: Self = codec::decode(bytes)?;
        record.validate()?;
        Ok(record)
    }
}

fn validate_scope(version: u16, snapshot: SnapshotId, revision: SplitRevisionId) -> Result<(), SplitError> {
    if version != 1 {
        return Err(SplitError::InvalidRecord);
    }
    SnapshotId::from_bytes(snapshot.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
    SplitRevisionId::from_bytes(revision.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
    Ok(())
}

fn required_revision<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<SplitRevisionId>, D::Error> {
    Option::<SplitRevisionId>::deserialize(d)
}
