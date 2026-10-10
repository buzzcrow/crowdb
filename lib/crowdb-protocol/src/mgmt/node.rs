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
    #[serde(default)]
    pub hardware: NodeHardware,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeHardware {
    pub architecture: String,
    pub logical_cpus: usize,
    pub memory_bytes: u64,
    pub data_root: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidateSnapshot {
    pub nodes: Vec<CandidateNode>,
    pub discovery_diagnostics: Vec<String>,
}

/// Authenticated local monitor control, carried over a private Unix socket or SSH.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum NodeControl {
    Identity,
    InstallKey {
        operation_id: String,
        public_key: String,
    },
    RemoveKey {
        operation_id: String,
    },
    TrustHost {
        host: String,
        port: u16,
        public_key: String,
    },
    StartKv {
        node_id: u64,
        bootstrap: super::SystemBootstrapIdentity,
        #[serde(default)]
        manifest: Option<serde_json::Value>,
        #[serde(default)]
        credentials: Option<NodeServiceCredentials>,
        #[serde(default)]
        admission: Option<NodeAdmissionGrant>,
    },
    CancelAdmission {
        bootstrap: super::SystemBootstrapIdentity,
        node_id: u64,
        admission: NodeAdmissionGrant,
    },
    Bind {
        binding: NodeBinding,
    },
    Cleanup {
        bootstrap: super::SystemBootstrapIdentity,
        confirm_delete_system_store: bool,
    },
    Service {
        intent: NodeServiceIntent,
    },
    ProvisionServiceCredentials {
        bootstrap: super::SystemBootstrapIdentity,
        credentials: NodeServiceCredentials,
    },
    ServiceCredentials {
        cluster_id: String,
    },
}

/// Group 0 admission identity; the target verifies the current committed record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeAdmissionGrant {
    pub operation_id: String,
    pub management_seeds: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeServiceCredentials {
    pub environment: String,
}

impl std::fmt::Debug for NodeServiceCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NodeServiceCredentials([redacted])")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeServiceAction {
    Start,
    Stop,
    Restart,
    Delete,
}

/// Current committed management command, checked again by its target monitor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeServiceIntent {
    pub cluster_id: String,
    pub operation_id: String,
    pub node_id: u64,
    pub service_id: String,
    pub kind: String,
    pub action: NodeServiceAction,
    pub configuration: String,
    pub environment: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeBinding {
    pub node_id: u64,
    pub bootstrap: super::SystemBootstrapIdentity,
    pub management_seeds: Vec<String>,
}
