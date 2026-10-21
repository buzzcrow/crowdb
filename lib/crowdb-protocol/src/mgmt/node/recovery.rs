// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated member endpoints; discovery advertisements carry no authority.

use super::NodeBinding;
use crate::mgmt::SystemBootstrapIdentity;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberEndpoint {
    pub discovery_id: String,
    pub node_id: u64,
    pub boot_id: String,
    pub addresses: Vec<IpAddr>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberRecoveryIdentity {
    pub version: u32,
    pub binding: NodeBinding,
    pub endpoint: MemberEndpoint,
}

/// Local monitor-owned snapshot, scoped to one live observer incarnation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberEndpoints {
    pub version: u32,
    pub bootstrap: SystemBootstrapIdentity,
    pub observer_boot_id: String,
    pub observed_at_unix_ms: u64,
    pub members: Vec<MemberEndpoint>,
    pub conflicts: Vec<String>,
}

/// Physical owners captured from accepted bootstrap or committed topology.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryGroup {
    pub store_id: u64,
    pub group_id: u64,
    pub deployment: Option<crate::mgmt::GroupDeploymentIdentity>,
    pub members: Vec<RecoveryReplica>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryReplica {
    pub replica_id: u64,
    pub node_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryGroups {
    pub bootstrap: SystemBootstrapIdentity,
    pub groups: Vec<RecoveryGroup>,
}
