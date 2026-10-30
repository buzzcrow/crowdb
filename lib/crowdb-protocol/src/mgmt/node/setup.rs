// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed node admission choices and bounded monitor progress observations.

use super::storage::{DiskBackend, DEFAULT_FILE_CAPACITY_BYTES, DEFAULT_FILE_RESERVE_BYTES};
use serde::{Deserialize, Serialize};

mod validation;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SetupServiceKind {
    PaxosKv,
    Diskdb,
    Diskio,
    Chunkdb,
    ChunkKv,
    AccessServer,
}

impl SetupServiceKind {
    pub const ALL: [Self; 6] = [
        Self::PaxosKv,
        Self::Diskdb,
        Self::Diskio,
        Self::Chunkdb,
        Self::ChunkKv,
        Self::AccessServer,
    ];
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupListeners {
    pub http_port: Option<u16>,
    pub rpc_port: Option<u16>,
    pub s3_port: Option<u16>,
    pub dataset_port: Option<u16>,
    pub health_port: Option<u16>,
    pub keepalive_interval_secs: Option<std::num::NonZeroU32>,
    pub dynamic_ownership: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupService {
    pub kind: SetupServiceKind,
    pub enabled: bool,
    #[serde(default)]
    pub listeners: SetupListeners,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SetupStorage {
    Automatic {
        capacity_bytes: u64,
        reserve_bytes: u64,
        sparse: bool,
    },
    Diskless,
    Supplied {
        disks: Vec<SetupMedium>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SetupStorageMode {
    Automatic,
    Diskless,
    Supplied,
}

impl SetupStorage {
    #[must_use]
    pub const fn mode(&self) -> SetupStorageMode {
        match self {
            Self::Automatic { .. } => SetupStorageMode::Automatic,
            Self::Diskless => SetupStorageMode::Diskless,
            Self::Supplied { .. } => SetupStorageMode::Supplied,
        }
    }
}

impl Default for SetupStorage {
    fn default() -> Self {
        Self::Automatic {
            capacity_bytes: DEFAULT_FILE_CAPACITY_BYTES,
            reserve_bytes: DEFAULT_FILE_RESERVE_BYTES,
            sparse: false,
        }
    }
}

/// Exact authenticated observation, not a declaration that a medium is blank.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupMedium {
    pub path: String,
    pub backend: DiskBackend,
    pub backing_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_identity: Option<String>,
    pub capacity_bytes: u64,
    pub media_type: Option<i32>,
    pub initialize: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSetupChoices {
    #[serde(default)]
    pub storage: SetupStorage,
    #[serde(default = "default_services")]
    pub services: Vec<SetupService>,
}

impl NodeSetupChoices {
    /// Return the management and RPC listeners reserved for the bootstrap KV.
    ///
    /// The node launcher may shift these listeners for host-network containers
    /// that share one physical host.  Callers that need the Group 0 endpoint
    /// must use the accepted policy rather than assuming the default ports.
    #[must_use]
    pub fn paxos_kv_ports(&self) -> Option<(u16, u16)> {
        self.services
            .iter()
            .find(|service| service.kind == SetupServiceKind::PaxosKv && service.enabled)
            .and_then(|service| Some((service.listeners.http_port?, service.listeners.rpc_port?)))
    }
}

fn default_services() -> Vec<SetupService> {
    SetupServiceKind::ALL
        .into_iter()
        .map(|kind| SetupService {
            kind,
            enabled: true,
            listeners: SetupListeners::default(),
        })
        .collect()
}

impl Default for NodeSetupChoices {
    fn default() -> Self {
        Self {
            storage: SetupStorage::default(),
            services: default_services(),
        }
    }
}

/// Before binding, only the fixed UUID and admission operation identify the node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSetupRequest {
    pub version: u32,
    pub discovery_id: String,
    pub operation_id: String,
    pub choices: NodeSetupChoices,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeSetupPhase {
    Accepted,
    WaitingForBinding,
    Running,
    Ready,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SetupStepPhase {
    Pending,
    Waiting,
    Preparing,
    Accepted,
    Ready,
    Failed,
    Warning,
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SetupServiceStatus {
    pub kind: SetupServiceKind,
    pub phase: SetupStepPhase,
    pub detail: Option<String>,
    pub operation_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SetupDiskStatus {
    pub path: String,
    pub phase: SetupStepPhase,
    pub detail: Option<String>,
    pub operation_id: String,
    pub disk_id: Option<String>,
}

/// Configuration and credential contents are never part of progress replies.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeSetupStatus {
    pub version: u32,
    pub discovery_id: String,
    pub operation_id: String,
    pub phase: NodeSetupPhase,
    #[serde(default)]
    pub storage_mode: Option<SetupStorageMode>,
    pub node_id: Option<u64>,
    pub cluster_id: Option<String>,
    pub disks: Vec<SetupDiskStatus>,
    pub services: Vec<SetupServiceStatus>,
    pub error: Option<String>,
}
