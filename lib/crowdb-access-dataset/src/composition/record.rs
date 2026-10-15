// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    split::{SplitError, SplitSource},
    CommitTarget, OperationId, Selection, SnapshotId, SplitRevisionId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CompositionSource {
    All,
    Selection(Selection),
    Split {
        source: SplitSource,
        #[serde(deserialize_with = "required_selection")]
        selection: Option<Selection>,
    },
}

/// Evaluation output is valid only at the editing revision that produced it.
/// Prepared content is frozen; commit attempts are separate durable records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DraftProgress {
    Editing,
    Evaluating {
        input_revision: u64,
        next_page: u64,
    },
    Evaluated {
        input_revision: u64,
        binding: [u8; 32],
    },
    Prepared {
        target: CommitTarget,
    },
    Published {
        target: CommitTarget,
        attempt: OperationId,
    },
    Abandoned,
}

impl DraftProgress {
    /// # Errors
    /// Rejects zero/stale revisions, edits to frozen content and revision overflow.
    pub fn apply_edit(&mut self, revision: &mut u64, expected: u64) -> Result<(), SplitError> {
        self.validate(*revision)?;
        if *revision == 0
            || *revision != expected
            || !matches!(
                self,
                Self::Editing | Self::Evaluating { .. } | Self::Evaluated { .. }
            )
        {
            return Err(SplitError::InvalidRecord);
        }
        let next = revision.checked_add(1).ok_or(SplitError::Limit)?;
        *revision = next;
        *self = Self::Editing;
        Ok(())
    }

    pub(crate) fn validate(&self, revision: u64) -> Result<(), SplitError> {
        if revision == 0 {
            return Err(SplitError::InvalidRecord);
        }
        match self {
            Self::Evaluating { input_revision, .. } | Self::Evaluated { input_revision, .. }
                if *input_revision != revision =>
            {
                Err(SplitError::InvalidRecord)
            }
            Self::Prepared { target } => validate_target(*target),
            Self::Published { target, attempt } => {
                OperationId::from_bytes(attempt.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
                validate_target(*target)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionRecord {
    pub version: u16,
    pub operation: OperationId,
    #[serde(deserialize_with = "required_parent")]
    pub parent: Option<SnapshotId>,
    #[serde(deserialize_with = "required_revision")]
    pub parent_split_revision: Option<SplitRevisionId>,
    pub source: CompositionSource,
    pub revision: u64,
    pub edit_pages: u64,
    pub source_lease_deadline: u64,
    pub progress: DraftProgress,
}

impl CompositionRecord {
    /// # Errors
    /// Rejects malformed versions, mismatched frozen sources or stale evaluation.
    pub fn validate(&self) -> Result<(), SplitError> {
        if self.version != 1 {
            return Err(SplitError::InvalidRecord);
        }
        OperationId::from_bytes(self.operation.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        self.progress.validate(self.revision)?;
        if let Some(parent) = self.parent {
            SnapshotId::from_bytes(parent.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        } else if self.parent_split_revision.is_some() || self.source != CompositionSource::All {
            return Err(SplitError::InvalidRecord);
        }
        if let Some(revision) = self.parent_split_revision {
            SplitRevisionId::from_bytes(revision.as_bytes()).map_err(|_| SplitError::InvalidRecord)?;
        }
        self.validate_source()?;
        match self.progress {
            DraftProgress::Prepared { target } | DraftProgress::Published { target, .. } => {
                let CommitTarget::Snapshot { snapshot } = target else {
                    return Err(SplitError::InvalidRecord);
                };
                if self.parent == Some(snapshot) {
                    return Err(SplitError::InvalidRecord);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_source(&self) -> Result<(), SplitError> {
        match &self.source {
            CompositionSource::All => Ok(()),
            CompositionSource::Selection(selection) => validate_selection(selection),
            CompositionSource::Split { source, selection } => {
                if self.parent != Some(source.snapshot) || self.parent_split_revision != Some(source.revision)
                {
                    return Err(SplitError::InvalidRecord);
                }
                if let Some(selection) = selection {
                    validate_selection(selection)?;
                }
                Ok(())
            }
        }
    }

    /// # Errors
    /// Rejects malformed, stale or oversized composition records.
    pub fn encode(&self) -> Result<Vec<u8>, SplitError> {
        self.validate()?;
        crate::split::codec::encode(self)
    }

    /// # Errors
    /// Rejects unsupported versions, malformed state and oversized records.
    pub fn decode(bytes: &[u8]) -> Result<Self, SplitError> {
        let record: Self = crate::split::codec::decode(bytes)?;
        record.validate()?;
        Ok(record)
    }
}

fn validate_selection(selection: &Selection) -> Result<(), SplitError> {
    // Uses the same v2 rule validation as Split definitions, without evaluating
    // values or changing legacy read-plan semantics.
    crate::split::definition::validate_selection(selection, 0)
}

fn validate_target(target: CommitTarget) -> Result<(), SplitError> {
    target.validate().map_err(|_| SplitError::InvalidRecord)
}

fn required_parent<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<SnapshotId>, D::Error> {
    Option::deserialize(d)
}
fn required_revision<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<SplitRevisionId>, D::Error> {
    Option::deserialize(d)
}
fn required_selection<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Selection>, D::Error> {
    Option::deserialize(d)
}
