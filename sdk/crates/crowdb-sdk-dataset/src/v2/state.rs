// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{required_option, Generation, OpaqueId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitTarget {
    Snapshot { snapshot: OpaqueId },
    SplitRevision { snapshot: OpaqueId, revision: OpaqueId },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitReceipt {
    pub attempt: OpaqueId,
    pub target: CommitTarget,
    #[serde(deserialize_with = "required_option")]
    pub expected_head: Option<Generation>,
    pub committed_head: Generation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Head {
    #[serde(deserialize_with = "required_option")]
    pub latest: Option<OpaqueId>,
    #[serde(deserialize_with = "required_option")]
    pub stable: Option<OpaqueId>,
    #[serde(deserialize_with = "required_option")]
    pub generation: Option<Generation>,
}
