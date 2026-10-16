// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Global disk identity survives removal from physical membership.
use crate::{
    common::DiskId,
    key::{DiskGroupKey, DiskKey, TextKey},
    DiskIdExt,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskIdReservation {
    pub version: u32,
    pub placement: String,
}

/// Numeric `DiskGroup` IDs are also used without their hardware parent path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskGroupIdReservation {
    pub version: u32,
    pub placement: String,
}
impl DiskGroupIdReservation {
    #[must_use]
    pub fn new(key: &DiskGroupKey) -> Self {
        Self {
            version: 1,
            placement: key.to_path(),
        }
    }
    #[must_use]
    pub fn key_for(id: u64) -> String {
        format!("/hw/disk-group-id/{id}")
    }
    /// # Errors
    /// Rejects unsupported schemas and noncanonical parent paths.
    pub fn validate(&self) -> Result<DiskGroupKey, &'static str> {
        let key = DiskGroupKey::from_path(&self.placement).map_err(|_| "invalid disk group ID placement")?;
        if self.version != 1 || key.to_path() != self.placement {
            return Err("invalid disk group ID reservation");
        }
        Ok(key)
    }
}
impl DiskIdReservation {
    #[must_use]
    pub fn new(key: &DiskKey) -> Self {
        Self {
            version: 1,
            placement: key.to_path(),
        }
    }
    #[must_use]
    pub fn key_for(id: &DiskId) -> String {
        format!("/hw/disk-id/{}", id.to_display_string())
    }
    /// # Errors
    /// Rejects unsupported schemas and noncanonical physical identities.
    pub fn validate(&self) -> Result<DiskKey, &'static str> {
        let key = DiskKey::from_path(&self.placement).map_err(|_| "invalid disk ID reservation placement")?;
        if self.version != 1 || key.to_path() != self.placement {
            return Err("invalid disk ID reservation");
        }
        Ok(key)
    }
}
