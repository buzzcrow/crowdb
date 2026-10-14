// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Node-local media observations, never disk membership authority.

use serde::{Deserialize, Serialize};

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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiskInventory {
    pub observed_at_ms: u64,
    pub disks: Vec<DiskObservation>,
}
