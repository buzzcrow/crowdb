// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{required_option, Decimal, Generation, OpaqueId, SampleId};
use serde::{Deserialize, Serialize};

/// Binary operands use canonical base64; explicit IDs retain binary identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Predicate {
    All,
    Explicit { ids: Vec<SampleId> },
    Prefix { value: String },
    Keyword { value: String },
    Range { start: String, end: String },
    FieldMatch { field: String, value: String },
    And { items: Vec<Self> },
    Or { items: Vec<Self> },
    Not { item: Box<Self> },
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlapPolicy {
    #[default]
    Disjoint,
    AllowOverlap,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoveragePolicy {
    #[default]
    AllowUnassigned,
    RequireComplete,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitPolicy {
    pub overlap: OverlapPolicy,
    pub coverage: CoveragePolicy,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitSource {
    pub snapshot: OpaqueId,
    pub revision: OpaqueId,
    pub name: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SplitOrigin {
    Evaluated,
    Inherited { source: SplitSource },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionWeight {
    pub name: String,
    pub weight: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SplitDefinition {
    Selection {
        predicate: Predicate,
    },
    WeightedPartition {
        seed: Decimal,
        weights: Vec<PartitionWeight>,
        index: usize,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DraftSplit {
    pub name: String,
    pub definition: SplitDefinition,
    pub origin: SplitOrigin,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishedSplit {
    pub name: String,
    pub definition: SplitDefinition,
    pub origin: SplitOrigin,
    pub count: Decimal,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitRevision {
    pub snapshot: OpaqueId,
    pub revision: OpaqueId,
    pub operation: OpaqueId,
    pub evaluator_version: u16,
    pub policy: SplitPolicy,
    pub splits: Vec<PublishedSplit>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SplitRevisionDeletion {
    pub snapshot: OpaqueId,
    pub revision: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub reclaim_after: Option<Decimal>,
    pub complete: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitHead {
    #[serde(deserialize_with = "required_option")]
    pub revision: Option<OpaqueId>,
    #[serde(deserialize_with = "required_option")]
    pub generation: Option<Generation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitRevisionSummary {
    pub snapshot: OpaqueId,
    pub revision: OpaqueId,
    pub operation: OpaqueId,
    pub split_count: u32,
}
