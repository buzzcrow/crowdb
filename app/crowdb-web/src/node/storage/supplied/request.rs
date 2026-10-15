// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Derive usable geometry without resizing a supplied physical medium.

use super::{Failure, JoinCandidate};
use crate::error::err_400;
use crowdb_console_shared::deployment::registry::NodeRecord;
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::{DiskStorageRecord, PhysicalBacking},
    diskdb::rpc::DiskValue,
    mgmt::node::storage::{DiskBackend, DiskObservation, PrepareSuppliedDisk},
};

pub(super) fn prepare(
    cluster: &str,
    owner: &NodeRecord,
    operation: uuid::Uuid,
    body: &JoinCandidate,
    observed: &DiskObservation,
) -> Result<PrepareSuppliedDisk, Failure> {
    let medium = &body.medium;
    let unit = 1024 * 1024;
    let zone = (medium.capacity_bytes.min(32 * 1024 * 1024 * 1024) / (64 * unit)) * (64 * unit);
    if zone == 0 || medium.capacity_bytes > (1_u64 << 53) - 1 {
        return Err(err_400("Unsupported supplied capacity"));
    }
    let zones = medium.capacity_bytes / zone;
    let (high, low) = operation.as_u64_pair();
    let request = PrepareSuppliedDisk {
        cluster_id: cluster.to_owned(),
        initialize: medium.initialize,
        record: DiskStorageRecord {
            version: 2,
            operation_id: body.operation_id.clone(),
            rack_id: owner.rack_id,
            node_id: owner.node_id,
            disk_group_id: body.disk_group_id,
            disk_id: DiskId { high, low },
            backend: medium.backend,
            backing_identity: medium.backing_identity.clone(),
            media_type: medium.media_type,
            sparse: medium.backend == DiskBackend::File
                && observed
                    .allocated_bytes
                    .is_some_and(|bytes| bytes < medium.capacity_bytes),
            removed: false,
            physical_backing: Some(PhysicalBacking {
                physical_host_id: owner.physical_host_id.clone(),
                resource_identity: medium
                    .resource_identity
                    .clone()
                    .ok_or_else(|| err_400("Missing resource identity"))?,
                capacity_bytes: medium.capacity_bytes,
            }),
            geometry: DiskValue {
                disk_type: medium.media_type.unwrap_or_default(),
                capacity_units: (zone / unit) * zones,
                zone_size_units: zone / unit,
                unit_size_bytes: 1024 * 1024,
                zone_count: u32::try_from(zones).map_err(|error| err_400(error.to_string()))?,
                status: HwStatus::Init as i32,
                device_path: medium.path.clone(),
            },
        },
    };
    request.validate().map_err(err_400)?;
    Ok(request)
}
