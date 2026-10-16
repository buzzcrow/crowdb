// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit restoration of one retained removal, without new disk initialization.

use super::{DiskRemovalRecord, DiskStorageRecord};
use crate::{key::DiskKey, DiskIdExt};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskRejoinRecord {
    pub version: u32,
    pub operation_id: String,
    pub source: DiskRemovalRecord,
}
impl DiskRejoinRecord {
    #[must_use]
    pub fn new(source: DiskRemovalRecord) -> Self {
        Self {
            version: 1,
            operation_id: format!(
                "rejoin/{}/{}",
                source.disk.disk_id.to_display_string(),
                source.offline_revision
            ),
            source,
        }
    }
    #[must_use]
    pub fn key_for(key: &DiskKey) -> String {
        DiskStorageRecord::key_for(key).replacen("/hw/disk-storage/", "/hw/disk-rejoin/", 1)
    }
    #[must_use]
    pub fn restored(&self) -> DiskStorageRecord {
        let mut record = self.source.disk.clone();
        record.removed = false;
        record
    }
    /// # Errors
    /// Rejects changed source geometry, removal identity and operation identity.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.source.validate()?;
        if self.version != 1 || Self::new(self.source.clone()).operation_id != self.operation_id {
            return Err("invalid disk rejoin receipt");
        }
        Ok(())
    }
}
