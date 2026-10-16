// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::SplitError;
use crate::{Selection, SnapshotId, SplitRevisionId};
use serde::{Deserialize, Serialize};

/// Opaque UTF-8 name; comparison uses exact bytes, including case and normalization.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SplitName(String);

impl SplitName {
    /// # Errors
    /// Rejects empty names and names exceeding 255 encoded bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, SplitError> {
        let value = value.into();
        if value.is_empty() || value.len() > 255 {
            return Err(SplitError::InvalidName);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SplitName {
    type Error = SplitError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SplitName> for String {
    fn from(value: SplitName) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum OverlapPolicy {
    #[default]
    Disjoint,
    AllowOverlap,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CoveragePolicy {
    RequireComplete,
    #[default]
    AllowUnassigned,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SplitPolicy {
    pub overlap: OverlapPolicy,
    pub coverage: CoveragePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SplitSource {
    pub snapshot: SnapshotId,
    pub revision: SplitRevisionId,
    pub name: SplitName,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MembershipOrigin {
    Evaluated,
    Inherited(SplitSource),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionWeight {
    pub name: SplitName,
    pub weight: u32,
}

/// The whole ordered weight list is persisted; reordering changes partition intervals.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SplitDefinition {
    Selection(Selection),
    WeightedPartition {
        seed: u64,
        weights: Vec<PartitionWeight>,
        index: u32,
    },
}

impl SplitDefinition {
    pub(crate) fn validate(&self) -> Result<(), SplitError> {
        match self {
            Self::Selection(selection) => validate_selection(selection, 0),
            Self::WeightedPartition { weights, index, .. } => {
                let mut names = std::collections::BTreeSet::new();
                let mut total = 0u32;
                for item in weights {
                    if item.weight == 0 || !names.insert(&item.name) {
                        return Err(SplitError::InvalidDefinition);
                    }
                    total = total
                        .checked_add(item.weight)
                        .ok_or(SplitError::InvalidDefinition)?;
                }
                if total == 0 || usize::try_from(*index).map_or(true, |index| index >= weights.len()) {
                    return Err(SplitError::InvalidDefinition);
                }
                Ok(())
            }
        }
    }
}

pub(crate) fn validate_selection(selection: &Selection, depth: usize) -> Result<(), SplitError> {
    if depth > 32 {
        return Err(SplitError::Limit);
    }
    match selection {
        Selection::And(items) | Selection::Or(items) => {
            if items.is_empty() {
                return Err(SplitError::InvalidDefinition);
            }
            for item in items {
                validate_selection(item, depth + 1)?;
            }
            Ok(())
        }
        Selection::Not(item) => validate_selection(item, depth + 1),
        // V2 equality distinguishes an empty inline value from missing data.
        Selection::FieldMatch { field, .. } if !field.is_empty() => Ok(()),
        _ => selection.validate().map_err(|_| SplitError::InvalidDefinition),
    }
}
