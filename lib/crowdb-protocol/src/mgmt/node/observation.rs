// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Current local execution evidence, separate from accepted setup progress.

use super::{deployment::NodeDeploymentPolicy, setup::NodeSetupStatus, NodeBinding};
use crate::mgmt::cluster::DependencyState;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeRuntimeObservation {
    pub version: u32,
    pub discovery_id: String,
    pub policy: NodeDeploymentPolicy,
    pub binding: Option<NodeBinding>,
    pub setup: Option<NodeSetupStatus>,
    pub observed_at_unix_ms: u64,
    pub services: Vec<NodeServiceObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeServiceObservation {
    pub service_id: String,
    pub kind: String,
    /// The executed operation; aggregate readers compare current committed intent.
    pub operation_id: Option<String>,
    pub state: DependencyState,
    pub observed_at_unix_ms: u64,
    pub max_age_ms: u64,
    pub reason_code: Option<String>,
}
