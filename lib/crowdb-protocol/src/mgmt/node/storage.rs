// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Node-local media observations, never disk membership authority.

use serde::{Deserialize, Serialize};

pub const DEFAULT_FILE_CAPACITY_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub const DEFAULT_FILE_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;

/// Preparation is provisional; only Group 0 can admit the resulting disk.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareDiskFile {
    pub operation_id: String,
    pub cluster_id: String,
    pub node_id: u64,
    pub capacity_bytes: u64,
    pub sparse: bool,
    pub reserve_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<FileDiskAdmission>,
}

/// Physical placement and existing allocation geometry chosen at acceptance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDiskAdmission {
    pub rack_id: u64,
    pub disk_group_id: u64,
    pub unit_size_bytes: u32,
    pub zone_size_bytes: u64,
    pub media_type: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FilePreparationPhase {
    Accepted,
    Creating,
    Prepared,
    Publishing,
    #[serde(rename = "waiting-for-diskio")]
    WaitingForDiskIO,
    Registered,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FilePreparation {
    pub version: u32,
    pub request: PrepareDiskFile,
    pub path: String,
    pub phase: FilePreparationPhase,
    pub backing_identity: Option<String>,
    /// Once complete, retries validate only; they never resize or recreate.
    pub preparation_complete: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FilePreparationPage {
    pub items: Vec<FilePreparation>,
    pub next: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiskBackend {
    File,
    BlockDevice,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskInput {
    pub backend: DiskBackend,
    pub path: String,
    /// Identity observed by the launcher before mapping the host resource.
    #[serde(default)]
    pub backing_identity: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiskObservation {
    pub path: String,
    pub backend: Option<DiskBackend>,
    pub capacity_bytes: Option<u64>,
    pub allocated_bytes: Option<u64>,
    pub backing_identity: Option<String>,
    pub supplied: bool,
    pub error: Option<String>,
    /// Reference into the inventory's deduplicated filesystem observations.
    #[serde(default)]
    pub backing_filesystem: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackingFilesystem {
    pub identity: String,
    pub capacity_bytes: Option<u64>,
    /// Space available to this process, excluding filesystem-reserved blocks.
    pub available_bytes: Option<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiskInventory {
    pub observed_at_ms: u64,
    pub disks: Vec<DiskObservation>,
    #[serde(default)]
    pub filesystems: Vec<BackingFilesystem>,
}
