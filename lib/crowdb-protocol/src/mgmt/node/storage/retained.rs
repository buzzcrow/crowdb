// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated checks of an existing retained medium; never provisioning.
use super::DiskBackend;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedDiskCondition {
    Removed { operation_id: String },
    Offline { revision: String },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckRetainedDisk {
    pub version: u32,
    pub cluster_id: String,
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub disk_id: String,
    pub condition: RetainedDiskCondition,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMedium {
    pub version: u32,
    pub disk_id: String,
    pub backend: DiskBackend,
    pub backing_identity: String,
    pub capacity_bytes: u64,
    pub observed_at_ms: u64,
}
