// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable authorization to validate one exact Offline admission.
use super::DiskStorageRecord;
use crate::{key::DiskKey, DiskIdExt};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskOnlineState {
    Pending,
    Failed,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskOnlineRecord {
    pub version: u32,
    pub operation_id: String,
    pub offline_revision: u64,
    pub disk: DiskStorageRecord,
    pub state: DiskOnlineState,
    pub error: Option<String>,
}

impl DiskOnlineRecord {
    #[must_use]
    pub fn new(disk: DiskStorageRecord, offline_revision: u64) -> Self {
        Self {
            version: 1,
            operation_id: format!("online/{}/{}", disk.disk_id.to_display_string(), offline_revision),
            offline_revision,
            disk,
            state: DiskOnlineState::Pending,
            error: None,
        }
    }

    #[must_use]
    pub fn key_for(key: &DiskKey) -> String {
        DiskStorageRecord::key_for(key).replacen("/hw/disk-storage/", "/hw/disk-online/", 1)
    }

    /// # Errors
    /// Rejects unsupported schemas, changed identity and inconsistent outcomes.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.disk.validate()?;
        if self.version != 1
            || self.offline_revision == 0
            || self.disk.removed
            || self.operation_id != Self::new(self.disk.clone(), self.offline_revision).operation_id
            || (self.state == DiskOnlineState::Failed) != self.error.is_some()
            || self
                .error
                .as_ref()
                .is_some_and(|error| error.is_empty() || error.len() > 2048)
        {
            return Err("invalid disk Online operation");
        }
        Ok(())
    }
}
