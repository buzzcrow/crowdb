// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable supplied-medium provisioning, distinct from retained-disk recovery.

use crate::disk_storage::DiskStorageRecord;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareSuppliedDisk {
    pub cluster_id: String,
    pub record: DiskStorageRecord,
    pub initialize: bool,
}

impl PrepareSuppliedDisk {
    /// # Errors
    /// Rejects missing consent and unsupported geometry. The monitor checks UUIDs.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.record.validate()?;
        let disk = &self.record;
        let zone = disk.geometry.zone_size_units * u64::from(disk.geometry.unit_size_bytes);
        if self.cluster_id.is_empty()
            || self.cluster_id.len() > 256
            || self.cluster_id.contains('\0')
            || disk.geometry.device_path.len() > 4096
            || !disk.geometry.device_path.starts_with('/')
            || disk.geometry.device_path.contains('\0')
            || !self.initialize
            || disk.version != 2
            || disk.removed
            || disk.geometry.status != crate::common::HwStatus::Init as i32
            || disk.geometry.unit_size_bytes != 1024 * 1024
            || !(64 * 1024 * 1024..=32 * 1024 * 1024 * 1024).contains(&zone)
        {
            return Err("invalid supplied medium authorization or geometry");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SuppliedPreparationPhase {
    Accepted,
    Verifying,
    Publishing,
    WaitingForDiskio,
    Registered,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuppliedPreparation {
    pub version: u32,
    pub request: PrepareSuppliedDisk,
    pub phase: SuppliedPreparationPhase,
    pub verified_bytes: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuppliedPreparationPage {
    pub items: Vec<SuppliedPreparation>,
    pub next: Option<String>,
}
