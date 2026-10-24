// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{id_bytes, snapshot, ApiError},
    predicate::Predicate,
};
use crowdb_access_dataset::{split as native, SplitRevisionId};
use serde::{Deserialize, Serialize};

pub(super) fn revision(value: &str) -> Result<SplitRevisionId, ApiError> {
    SplitRevisionId::from_bytes(&id_bytes(value)?).map_err(|_| ApiError::invalid())
}
pub(super) fn decimal(value: &str) -> Result<u64, ApiError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(ApiError::invalid());
    }
    value.parse().map_err(|_| ApiError::invalid())
}
pub(super) fn required_option<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Policy {
    pub overlap: Overlap,
    pub coverage: Coverage,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Overlap {
    Disjoint,
    AllowOverlap,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Coverage {
    RequireComplete,
    AllowUnassigned,
}
impl From<Policy> for native::SplitPolicy {
    fn from(p: Policy) -> Self {
        Self {
            overlap: match p.overlap {
                Overlap::Disjoint => native::OverlapPolicy::Disjoint,
                Overlap::AllowOverlap => native::OverlapPolicy::AllowOverlap,
            },
            coverage: match p.coverage {
                Coverage::RequireComplete => native::CoveragePolicy::RequireComplete,
                Coverage::AllowUnassigned => native::CoveragePolicy::AllowUnassigned,
            },
        }
    }
}
impl From<native::SplitPolicy> for Policy {
    fn from(p: native::SplitPolicy) -> Self {
        Self {
            overlap: match p.overlap {
                native::OverlapPolicy::Disjoint => Overlap::Disjoint,
                native::OverlapPolicy::AllowOverlap => Overlap::AllowOverlap,
            },
            coverage: match p.coverage {
                native::CoveragePolicy::RequireComplete => Coverage::RequireComplete,
                native::CoveragePolicy::AllowUnassigned => Coverage::AllowUnassigned,
            },
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    pub snapshot: String,
    pub revision: String,
    pub name: String,
}
impl Source {
    pub(super) fn native(self) -> Result<native::SplitSource, ApiError> {
        Ok(native::SplitSource {
            snapshot: snapshot(&self.snapshot)?,
            revision: revision(&self.revision)?,
            name: native::SplitName::new(self.name).map_err(|_| ApiError::invalid())?,
        })
    }
}
impl From<native::SplitSource> for Source {
    fn from(s: native::SplitSource) -> Self {
        Self {
            snapshot: s.snapshot.to_string(),
            revision: s.revision.to_string(),
            name: s.name.into(),
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Origin {
    Evaluated,
    Inherited { source: Source },
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Weight {
    pub name: String,
    pub weight: u32,
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Definition {
    Selection {
        predicate: Predicate,
    },
    WeightedPartition {
        seed: String,
        weights: Vec<Weight>,
        index: u32,
    },
}
impl Definition {
    fn native(self) -> Result<native::SplitDefinition, ApiError> {
        Ok(match self {
            Self::Selection { predicate } => native::SplitDefinition::Selection(predicate.native()?),
            Self::WeightedPartition { seed, weights, index } => native::SplitDefinition::WeightedPartition {
                seed: decimal(&seed)?,
                index,
                weights: weights
                    .into_iter()
                    .map(|w| {
                        Ok(native::PartitionWeight {
                            name: native::SplitName::new(w.name).map_err(|_| ApiError::invalid())?,
                            weight: w.weight,
                        })
                    })
                    .collect::<Result<_, ApiError>>()?,
            },
        })
    }
}
impl From<native::SplitDefinition> for Definition {
    fn from(d: native::SplitDefinition) -> Self {
        match d {
            native::SplitDefinition::Selection(predicate) => Self::Selection {
                predicate: predicate.into(),
            },
            native::SplitDefinition::WeightedPartition { seed, weights, index } => Self::WeightedPartition {
                seed: seed.to_string(),
                weights: weights
                    .into_iter()
                    .map(|w| Weight {
                        name: w.name.into(),
                        weight: w.weight,
                    })
                    .collect(),
                index,
            },
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DraftSplit {
    pub name: String,
    pub definition: Definition,
    pub origin: Origin,
}
impl DraftSplit {
    pub(super) fn native(self) -> Result<native::DraftSplit, ApiError> {
        Ok(native::DraftSplit {
            name: native::SplitName::new(self.name).map_err(|_| ApiError::invalid())?,
            definition: self.definition.native()?,
            origin: match self.origin {
                Origin::Evaluated => native::MembershipOrigin::Evaluated,
                Origin::Inherited { source } => native::MembershipOrigin::Inherited(source.native()?),
            },
        })
    }
}
impl From<native::DraftSplit> for DraftSplit {
    fn from(d: native::DraftSplit) -> Self {
        Self {
            name: d.name.into(),
            definition: d.definition.into(),
            origin: match d.origin {
                native::MembershipOrigin::Evaluated => Origin::Evaluated,
                native::MembershipOrigin::Inherited(source) => Origin::Inherited {
                    source: source.into(),
                },
            },
        }
    }
}
