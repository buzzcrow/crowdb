// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned Group-0 backing identity, separate from the existing disk geometry.

mod exclusion;
mod identity;
mod online;
mod rejoin;
mod removal;
pub use exclusion::{DiskExclusionRecord, DiskExclusionReporter};
pub use identity::{DiskGroupIdReservation, DiskIdReservation};
pub use online::{DiskOnlineRecord, DiskOnlineState};
pub use rejoin::DiskRejoinRecord;
pub use removal::DiskRemovalRecord;

use serde::{Deserialize, Serialize};

use crate::{
    common::DiskId,
    diskdb::rpc::DiskValue,
    key::{DiskKey, TextKey},
    mgmt::node::storage::DiskBackend,
};

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
    /// Verified physical resource, separate from logical usable geometry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_backing: Option<PhysicalBacking>,
    /// None means the medium class has not been established.
    pub media_type: Option<i32>,
    pub sparse: bool,
    pub geometry: DiskValue,
    pub removed: bool,
}

/// Launcher-established resource identity; aliases share this resource claim.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalBacking {
    pub physical_host_id: String,
    pub resource_identity: String,
    pub capacity_bytes: u64,
}

impl PhysicalBacking {
    /// # Errors
    /// Rejects unbounded/ambiguous identities and unsupported physical capacities.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.physical_host_id.is_empty()
            || self.physical_host_id.len() > 256
            || self.physical_host_id.contains('\0')
            || !canonical_backing_identity(&self.resource_identity)
            || self.capacity_bytes == 0
            || self.capacity_bytes > i64::MAX as u64
        {
            return Err("invalid physical backing resource");
        }
        Ok(())
    }
}

fn canonical_backing_identity(identity: &str) -> bool {
    if identity.len() > 64 {
        return false;
    }
    let parts: Vec<_> = identity.split(':').collect();
    let expected = match parts.first().copied() {
        Some("file") => 3,
        Some("device") => 2,
        _ => return false,
    };
    parts.len() == expected
        && parts[1..]
            .iter()
            .all(|part| part.parse::<u64>().is_ok_and(|value| value.to_string() == *part))
}

impl DiskStorageRecord {
    /// Physical bytes include any tail excluded from whole-zone allocation.
    #[must_use]
    pub fn physical_capacity_bytes(&self) -> Option<u64> {
        self.physical_backing
            .as_ref()
            .map(|backing| backing.capacity_bytes)
            .or_else(|| {
                self.geometry
                    .capacity_units
                    .checked_mul(u64::from(self.geometry.unit_size_bytes))
            })
    }

    /// One retained identity survives removal from active disk membership.
    #[must_use]
    pub fn key(&self) -> String {
        Self::key_for(&DiskKey {
            rack_id: self.rack_id,
            node_id: self.node_id,
            disk_group_id: self.disk_group_id,
            disk_id: self.disk_id,
        })
    }

    /// Use the existing disk key's canonical identity encoding.
    #[must_use]
    pub fn key_for(key: &DiskKey) -> String {
        key.to_path().replacen("/hw/disk/", "/hw/disk-storage/", 1)
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
        if !matches!(self.version, 1 | 2) {
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
        match (self.version, &self.physical_backing) {
            (1, None) => {}
            (2, Some(backing)) => {
                backing.validate()?;
                let logical = self
                    .geometry
                    .capacity_units
                    .checked_mul(u64::from(self.geometry.unit_size_bytes))
                    .ok_or("logical disk capacity overflows")?;
                if backing.capacity_bytes < logical
                    || !canonical_backing_identity(&self.backing_identity)
                    || (self.backend == DiskBackend::File && !self.backing_identity.starts_with("file:"))
                    || (self.backend == DiskBackend::BlockDevice
                        && !self.backing_identity.starts_with("device:"))
                {
                    return Err("physical backing differs from disk geometry or backend");
                }
            }
            _ => return Err("disk storage version and physical backing differ"),
        }
        let geometry = &self.geometry;
        if crate::diskdb::rpc::DiskType::try_from(geometry.disk_type).is_err()
            || crate::common::HwStatus::try_from(geometry.status).is_err()
            || self.media_type.is_some_and(|media| media != geometry.disk_type)
            || geometry.device_path.is_empty()
            || geometry.unit_size_bytes == 0
            || geometry.zone_size_units == 0
            || geometry.zone_size_units % 64 != 0
            || geometry.zone_count == 0
            || geometry.capacity_units % geometry.zone_size_units != 0
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
