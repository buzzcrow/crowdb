// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod binding;
mod finish;
mod planning;
mod state;

use super::{ReadSession, SessionSpec};
use crate::{
    split::{SplitName, SplitSource},
    SplitRevisionId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSplit {
    /// None resolves the current revision once when preparation begins.
    pub revision: Option<SplitRevisionId>,
    pub name: SplitName,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSpecV2 {
    pub read: SessionSpec,
    pub split: Option<SessionSplit>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadPreparation {
    pub snapshot: crate::SnapshotId,
    pub split: Option<SplitSource>,
    pub next_page: u64,
    pub admitted: u64,
    pub complete: bool,
    pub session: Option<ReadSession>,
}
