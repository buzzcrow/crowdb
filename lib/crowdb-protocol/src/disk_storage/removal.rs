// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable reconciliation receipt for one exact Offline removal.

use crate::{disk_storage::DiskStorageRecord, key::DiskKey, DiskIdExt};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskRemovalRecord {
    pub version: u32,
    pub operation_id: String,
    pub offline_revision: u64,
    pub disk: DiskStorageRecord,
}

impl DiskRemovalRecord {
    #[must_use]
    pub fn new(disk: DiskStorageRecord, offline_revision: u64) -> Self {
        Self {
            version: 1,
            operation_id: format!("remove/{}/{}", disk.disk_id.to_display_string(), offline_revision),
            offline_revision,
            disk,
        }
    }

    #[must_use]
    pub fn key_for(disk: &DiskKey) -> String {
        DiskStorageRecord::key_for(disk).replacen("/hw/disk-storage/", "/hw/disk-removal/", 1)
    }

    /// # Errors
    /// Rejects unsupported receipts and changed removal identity.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.disk.validate()?;
        if self.version != 1
            || self.offline_revision == 0
            || !self.disk.removed
            || Self::new(self.disk.clone(), self.offline_revision).operation_id != self.operation_id
        {
            return Err("invalid disk removal receipt");
        }
        Ok(())
    }
}
