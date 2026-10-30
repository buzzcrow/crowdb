// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Accepted startup inputs, distinct from discovered or confirmed membership.

use super::setup::{NodeSetupChoices, SetupService, SetupServiceKind, SetupStorage};
use crate::{
    ACCESS_SERVER_DATASET_HTTP_BASE, ACCESS_SERVER_HEALTH_HTTP_BASE, ACCESS_SERVER_HTTP_BASE,
    ACCESS_SERVER_ICEBERG_HTTP_BASE, CHUNKDB_HTTP_BASE, CHUNKDB_RPC_BASE, CHUNK_KV_HTTP_BASE,
    CHUNK_KV_RPC_BASE, DISKDB_HTTP_BASE, DISKDB_LISTEN_BASE, DISKDB_RPC_BASE, DISKIO_RPC_BASE,
    KV_SERVER_LISTEN_BASE, KV_SERVER_MGMT_BASE,
};
use serde::{Deserialize, Serialize};

mod public_access;
pub use public_access::PublicAccessUris;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentMode {
    Local,
    Production,
}

/// Untrusted discovery hints. Admission still authenticates the actual peer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormationPolicy {
    pub version: u32,
    pub mode: DeploymentMode,
    pub cluster: String,
    pub initial_voters: u8,
}

impl FormationPolicy {
    /// # Errors
    /// Rejects unsupported versions, malformed scopes and voter counts.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1 || !matches!(self.initial_voters, 1 | 3) || !valid_cluster(&self.cluster) {
            return Err("invalid node formation policy");
        }
        Ok(())
    }
}

/// Sealed before admission; omitted restart inputs retain these values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDeploymentPolicy {
    pub version: u32,
    pub mode: DeploymentMode,
    pub cluster: String,
    pub initial_voters: u8,
    pub setup: NodeSetupChoices,
    pub public_access: PublicAccessUris,
}

/// Explicit inputs from a configuration file or command/environment overrides.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDeploymentOverrides {
    pub version: Option<u32>,
    pub mode: Option<DeploymentMode>,
    pub cluster: Option<String>,
    pub initial_voters: Option<u8>,
    pub storage: Option<SetupStorage>,
    pub services: Option<Vec<SetupService>>,
    #[serde(default)]
    pub public_access: PublicAccessUris,
}

impl Default for NodeDeploymentPolicy {
    fn default() -> Self {
        let mut setup = NodeSetupChoices::default();
        if let SetupStorage::Automatic { sparse, .. } = &mut setup.storage {
            *sparse = true;
        }
        for service in &mut setup.services {
            let listeners = &mut service.listeners;
            match service.kind {
                SetupServiceKind::PaxosKv => {
                    listeners.http_port = Some(KV_SERVER_MGMT_BASE);
                    listeners.rpc_port = Some(KV_SERVER_LISTEN_BASE);
                }
                SetupServiceKind::Diskdb => {
                    listeners.listen_port = Some(DISKDB_LISTEN_BASE);
                    listeners.http_port = Some(DISKDB_HTTP_BASE);
                    listeners.rpc_port = Some(DISKDB_RPC_BASE);
                }
                SetupServiceKind::Diskio => listeners.rpc_port = Some(DISKIO_RPC_BASE),
                SetupServiceKind::Chunkdb => {
                    listeners.http_port = Some(CHUNKDB_HTTP_BASE);
                    listeners.rpc_port = Some(CHUNKDB_RPC_BASE);
                }
                SetupServiceKind::ChunkKv => {
                    listeners.http_port = Some(CHUNK_KV_HTTP_BASE);
                    listeners.rpc_port = Some(CHUNK_KV_RPC_BASE);
                }
                SetupServiceKind::AccessServer => {
                    listeners.s3_port = Some(ACCESS_SERVER_HTTP_BASE);
                    listeners.http_port = Some(ACCESS_SERVER_ICEBERG_HTTP_BASE);
                    listeners.dataset_port = Some(ACCESS_SERVER_DATASET_HTTP_BASE);
                    listeners.health_port = Some(ACCESS_SERVER_HEALTH_HTTP_BASE);
                }
            }
        }
        Self {
            version: 1,
            mode: DeploymentMode::Local,
            cluster: "default".into(),
            initial_voters: 1,
            setup,
            public_access: PublicAccessUris {
                s3: Some(format!("http://127.0.0.1:{ACCESS_SERVER_HTTP_BASE}")),
                iceberg: Some(format!("http://127.0.0.1:{ACCESS_SERVER_ICEBERG_HTTP_BASE}")),
                dataset: Some(format!("http://127.0.0.1:{ACCESS_SERVER_DATASET_HTTP_BASE}")),
            },
        }
    }
}

impl NodeDeploymentPolicy {
    #[must_use]
    pub fn formation(&self) -> FormationPolicy {
        FormationPolicy {
            version: self.version,
            mode: self.mode,
            cluster: self.cluster.clone(),
            initial_voters: self.initial_voters,
        }
    }

    /// Check the entire accepted input before performing node effects.
    ///
    /// # Errors
    /// Rejects unknown versions, invalid scopes/counts and unsafe production files.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1 {
            return Err("unsupported node deployment policy version");
        }
        if !matches!(self.initial_voters, 1 | 3) {
            return Err("initial voters must be one or three");
        }
        if !valid_cluster(&self.cluster) {
            return Err("cluster scope must contain 1 to 48 name characters");
        }
        if self.mode == DeploymentMode::Production
            && matches!(self.setup.storage, SetupStorage::Automatic { sparse: true, .. })
        {
            return Err("production automatic storage cannot use sparse local-test files");
        }
        for service in self.setup.services.iter().filter(|service| service.enabled) {
            let ports = &service.listeners;
            if [
                ports.listen_port,
                ports.http_port,
                ports.rpc_port,
                ports.s3_port,
                ports.dataset_port,
                ports.health_port,
            ]
            .into_iter()
            .flatten()
            .any(|port| matches!(port, 2222 | 9090 | 9095))
            {
                return Err("service listener conflicts with node management or bootstrap transport");
            }
            let complete = match service.kind {
                SetupServiceKind::PaxosKv | SetupServiceKind::Chunkdb | SetupServiceKind::ChunkKv => {
                    ports.http_port.is_some() && ports.rpc_port.is_some()
                }
                SetupServiceKind::Diskdb | SetupServiceKind::Diskio => ports.rpc_port.is_some(),
                SetupServiceKind::AccessServer => {
                    ports.s3_port.is_some()
                        && ports.http_port.is_some()
                        && ports.dataset_port.is_some()
                        && ports.health_port.is_some()
                }
            };
            if !complete {
                return Err("accepted service listeners must be explicit");
            }
        }
        self.public_access.validate()?;
        self.setup.validate()
    }
}

fn valid_cluster(cluster: &str) -> bool {
    !cluster.is_empty()
        && cluster.len() <= 48
        && cluster.as_bytes()[0].is_ascii_alphanumeric()
        && cluster
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

impl NodeDeploymentOverrides {
    /// Resolve explicit overrides over file inputs, retaining omitted accepted values.
    ///
    /// # Errors
    /// Rejects changed accepted inputs and invalid fresh or persisted policies.
    pub fn resolve(
        &self,
        file: &Self,
        accepted: Option<&NodeDeploymentPolicy>,
    ) -> Result<NodeDeploymentPolicy, &'static str> {
        if [self.version, file.version]
            .into_iter()
            .flatten()
            .any(|version| version != 1)
        {
            return Err("unsupported node deployment policy version");
        }
        if let Some(accepted) = accepted {
            accepted.validate()?;
        }
        let mut policy = accepted.cloned().unwrap_or_default();
        if accepted.is_none() && self.mode.or(file.mode) == Some(DeploymentMode::Production) {
            policy.mode = DeploymentMode::Production;
            policy.public_access = PublicAccessUris::default();
            policy.setup.storage = NodeSetupChoices::default().storage;
        }
        for inputs in [file, self] {
            inputs.apply(&mut policy);
        }
        policy.validate()?;
        if accepted.is_some_and(|accepted| *accepted != policy) {
            return Err("deployment inputs differ from accepted node policy");
        }
        Ok(policy)
    }

    fn apply(&self, policy: &mut NodeDeploymentPolicy) {
        self.public_access.apply(&mut policy.public_access);
        if let Some(version) = self.version {
            policy.version = version;
        }
        if let Some(mode) = self.mode {
            policy.mode = mode;
        }
        if let Some(cluster) = &self.cluster {
            policy.cluster.clone_from(cluster);
        }
        if let Some(voters) = self.initial_voters {
            policy.initial_voters = voters;
        }
        if let Some(storage) = &self.storage {
            policy.setup.storage.clone_from(storage);
        }
        if let Some(services) = &self.services {
            policy.setup.services.clone_from(services);
        }
    }
}
