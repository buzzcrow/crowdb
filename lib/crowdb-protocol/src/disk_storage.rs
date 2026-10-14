// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned Group-0 backing identity, separate from the existing disk geometry.

use serde::{Deserialize, Serialize};

use crate::{common::DiskId, diskdb::rpc::DiskValue, mgmt::node::storage::DiskBackend, DiskIdExt};

/// Retained backing information is not evidence of current I/O availability.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskStorageRecord {
    pub version: u32,
    pub operation_id: String,
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub disk_id: DiskId,
    pub backend: DiskBackend,
    pub backing_identity: String,
    /// None means the medium class has not been established.
    pub media_type: Option<i32>,
    pub sparse: bool,
    pub geometry: DiskValue,
    pub removed: bool,
}

impl DiskStorageRecord {
    /// One retained identity survives removal from active disk membership.
    #[must_use]
    pub fn key(&self) -> String {
        format!(
            "{}{}/{}",
            Self::node_prefix(self.rack_id, self.node_id),
            self.disk_group_id,
            self.disk_id.to_display_string()
        )
    }

    /// Limit duplicate backing checks to one physical node.
    #[must_use]
    pub fn node_prefix(rack_id: u64, node_id: u64) -> String {
        format!("/hw/disk-storage/{rack_id}/{node_id}/")
    }

    /// Reject unsupported schemas and ambiguous backing/geometry before use.
    ///
    /// # Errors
    /// Returns the invalid field category without interpreting another version.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1 {
            return Err("unsupported disk storage version");
        }
        if self.operation_id.is_empty() || self.backing_identity.is_empty() {
            return Err("missing disk storage identity");
        }
        if self.backend == DiskBackend::BlockDevice && self.sparse {
            return Err("block device cannot be sparse");
        }
        if self
            .media_type
            .is_some_and(|media| crate::diskdb::rpc::DiskType::try_from(media).is_err())
        {
            return Err("invalid disk storage media type");
        }
        let geometry = &self.geometry;
        if crate::diskdb::rpc::DiskType::try_from(geometry.disk_type).is_err()
            || crate::common::HwStatus::try_from(geometry.status).is_err()
            || self.media_type.is_some_and(|media| media != geometry.disk_type)
            || geometry.device_path.is_empty()
            || geometry.unit_size_bytes == 0
            || geometry.zone_size_units == 0
            || geometry.zone_count == 0
            || geometry.capacity_units / geometry.zone_size_units != u64::from(geometry.zone_count)
            || geometry
                .capacity_units
                .checked_mul(u64::from(geometry.unit_size_bytes))
                .is_none()
        {
            return Err("invalid disk storage geometry");
        }
        Ok(())
    }
}
