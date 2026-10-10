// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Monitor discovery observations; none of these records authorize admission.

use serde::{Deserialize, Serialize};

pub const NODE_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeAdvertisement {
    pub discovery_id: String,
    pub protocol_version: u32,
    pub monitor_endpoints: Vec<String>,
    pub cluster_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateState {
    Unbound,
    SameCluster,
    ForeignCluster,
    Incompatible,
    IdentityConflict,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidateNode {
    pub advertisement: NodeAdvertisement,
    pub state: CandidateState,
}

/// Read-only monitor handshake, never proof of management authorization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeHandshake {
    pub advertisement: NodeAdvertisement,
    pub physical_host_id: String,
    pub rack_hint: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidateSnapshot {
    pub nodes: Vec<CandidateNode>,
    pub discovery_diagnostics: Vec<String>,
}
