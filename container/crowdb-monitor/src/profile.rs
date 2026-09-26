// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod validation;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PROFILE_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("failed to read deployment profile: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to decode deployment profile: {0}")]
    Decode(#[from] toml::de::Error),
    #[error("invalid deployment profile: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentProfile {
    pub version: u32,
    pub name: String,
    pub display_name: String,
    pub placement_mode: String,
    pub s3_tenant: String,
    pub iceberg_catalog: String,
    pub paths: PathProfile,
    pub logs: LogProfile,
    #[serde(default)]
    pub nodes: Vec<NodeProfile>,
    #[serde(default)]
    pub groups: Vec<GroupProfile>,
    #[serde(default)]
    pub disks: Vec<DiskProfile>,
    #[serde(default)]
    pub public_endpoints: Vec<PublicEndpoint>,
    #[serde(default)]
    pub services: Vec<ServiceProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathProfile {
    pub install_root: PathBuf,
    pub bin_root: PathBuf,
    pub template_root: PathBuf,
    pub data_root: PathBuf,
    pub run_root: PathBuf,
    pub log_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogProfile {
    pub max_file_bytes: u64,
    pub max_files: u16,
    pub mirror_warnings_to_stderr: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeProfile {
    pub node_id: u64,
    pub rack_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GroupRole {
    System,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupProfile {
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    pub role: GroupRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskProfile {
    pub disk_id: String,
    pub disk_group_id: u64,
    pub node_id: u64,
    pub path: PathBuf,
    pub capacity_bytes: u64,
    pub zone_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicEndpoint {
    pub id: String,
    pub bind: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeKind {
    Http,
    Tcp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeProfile {
    pub kind: ProbeKind,
    pub target: String,
    pub timeout_ms: u64,
    pub failure_threshold: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartProfile {
    pub max_attempts: u32,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
    #[serde(default = "default_stable_after_ms")]
    pub stable_after_ms: u64,
}

const fn default_stable_after_ms() -> u64 {
    60_000
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceProfile {
    pub id: String,
    pub program: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub config_template: Option<PathBuf>,
    pub probe: ProbeProfile,
    pub restart: RestartProfile,
}

impl DeploymentProfile {
    /// # Errors
    /// Returns an error when the profile cannot be read, parsed, or validated.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ProfileError> {
        let body = std::fs::read_to_string(path)?;
        Self::parse(&body)
    }

    /// # Errors
    /// Returns an error when the profile cannot be parsed or validated.
    pub fn parse(body: &str) -> Result<Self, ProfileError> {
        let profile: Self = toml::from_str(body)?;
        profile.validate()?;
        Ok(profile)
    }

    /// # Errors
    /// Returns an error when the profile violates deployment constraints.
    pub fn validate(&self) -> Result<(), ProfileError> {
        validation::validate(self)
    }

    /// # Errors
    /// Returns an error when the profile or its service dependencies are invalid.
    pub fn services_in_start_order(&self) -> Result<Vec<&ServiceProfile>, ProfileError> {
        validation::services_in_start_order(self)
    }
}
