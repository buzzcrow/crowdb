// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{required_option, Decimal, OpaqueId, Predicate, SplitSource};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSplit {
    #[serde(deserialize_with = "required_option")]
    pub revision: Option<OpaqueId>,
    pub name: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shuffle {
    pub seed: Decimal,
    pub group_size: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPlan {
    /// Concrete Snapshot ID, latest or stable. The server pins it at first admission.
    pub snapshot: String,
    pub selection: Predicate,
    pub fields: Vec<String>,
    pub batch_size: usize,
    pub partition: crate::WorkerPartition,
    #[serde(deserialize_with = "required_option")]
    pub shuffle: Option<Shuffle>,
    #[serde(deserialize_with = "required_option")]
    pub split: Option<ReadSplit>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBinding {
    pub snapshot: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub split: Option<SplitSource>,
    pub cursor: String,
}
