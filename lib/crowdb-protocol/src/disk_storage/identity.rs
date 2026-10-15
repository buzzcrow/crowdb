// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Global disk identity survives removal from physical membership.
use crate::{
    common::DiskId,
    key::{DiskKey, TextKey},
    DiskIdExt,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskIdReservation {
    pub version: u32,
    pub placement: String,
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
