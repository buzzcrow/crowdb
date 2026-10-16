// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{required_option, Decimal, Generation, OpaqueId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitTarget {
    Snapshot { snapshot: OpaqueId },
    SplitRevision { snapshot: OpaqueId, revision: OpaqueId },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DraftProgress {
    Editing,
    Evaluating {
        input_revision: Decimal,
        next_page: Decimal,
    },
    Evaluated {
        input_revision: Decimal,
        binding: String,
    },
    Prepared {
        target: CommitTarget,
    },
    Committing {
        target: CommitTarget,
        attempt: OpaqueId,
    },
    Published {
        target: CommitTarget,
        attempt: OpaqueId,
    },
    Abandoned,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DraftRevision {
    pub expected_revision: Decimal,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluateDraft {
    pub expected_revision: Decimal,
    pub expected_page: Decimal,
}
