// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact-revision service confirmation of administrative exclusion.

use crate::{
    common::DiskId,
    key::{DiskKey, TextKey},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiskExclusionReporter {
    Diskio,
    Diskdb,
}

impl DiskExclusionReporter {
    #[must_use]
    pub fn service(self) -> &'static str {
        match self {
            Self::Diskio => "diskio",
            Self::Diskdb => "diskdb",
        }
    }
}

/// A service confirms that it has stopped accepting disk work and drained all
/// admitted work. A heartbeat, timeout or absent process is not this evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskExclusionRecord {
    pub version: u32,
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub disk_id: DiskId,
    pub disk_revision: u64,
    pub reporter: DiskExclusionReporter,
    pub instance_id: u64,
    pub owner_revision: Option<u64>,
}

impl DiskExclusionRecord {
    #[must_use]
    pub fn disk_key(&self) -> DiskKey {
        DiskKey {
            rack_id: self.rack_id,
            node_id: self.node_id,
            disk_group_id: self.disk_group_id,
            disk_id: self.disk_id,
        }
    }

    #[must_use]
    pub fn key(&self) -> String {
        Self::key_for(&self.disk_key(), self.reporter, self.instance_id)
    }

    #[must_use]
    pub fn key_for(disk: &DiskKey, reporter: DiskExclusionReporter, instance_id: u64) -> String {
        format!(
            "{}/{}/{instance_id}",
            disk.to_path().replacen("/hw/disk/", "/hw/disk-exclusion/", 1),
            reporter.service()
        )
    }

    /// # Errors
    /// Rejects unsupported or incomplete confirmation records.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1 || self.disk_revision == 0 {
            return Err("unsupported or incomplete disk exclusion confirmation");
        }
        if match self.reporter {
            DiskExclusionReporter::Diskio => self.owner_revision.is_some(),
            DiskExclusionReporter::Diskdb => self.owner_revision.map_or(true, |revision| revision == 0),
        } {
            return Err("invalid disk exclusion owner generation");
        }
        Ok(())
    }
}
