// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A `DiskGroup` has one media class and allocation unit, across retained removals.

use crowdb_protocol::{
    disk_storage::DiskStorageRecord,
    diskdb::rpc::{DiskType, DiskValue},
    key::{DiskKey, TextKey},
};

use super::{mutation, storage_observation::bounded_inventory, HardwareClient};
use crate::{Error, Result};

impl HardwareClient {
    /// Validate storage class before preparing bytes. Admission repeats this
    /// check under its Rack revision fence, so a concurrent first disk cannot
    /// publish an incompatible class. Backend does not define media class.
    ///
    /// # Errors
    /// Rejects missing parents, inconsistent receipts or mixed allocation units
    /// and media classes. Unknown media is a distinct class, including in an
    /// otherwise empty group with retained removed disks.
    pub async fn validate_disk_group_storage(
        &self,
        rack_id: u64,
        node_id: u64,
        group_id: u64,
        media: Option<i32>,
        unit_size: u32,
    ) -> Result<()> {
        if unit_size == 0 || media.is_some_and(|value| DiskType::try_from(value).is_err()) {
            return Err(Error::Server("invalid disk storage class".into()));
        }
        let node = self
            .get_node(rack_id, node_id)
            .await?
            .ok_or_else(|| Error::Server("disk admission node does not exist".into()))?;
        if !node.disk_group_ids.contains(&group_id)
            || self.get_disk_group(rack_id, node_id, group_id).await?.is_none()
        {
            return Err(Error::Server(
                "disk admission requires an existing DiskGroup on this node".into(),
            ));
        }
        let prefix = format!("/hw/disk-storage/{rack_id}/{node_id}/{group_id}/");
        let mut retained = std::collections::HashMap::new();
        for (key, bytes) in bounded_inventory(&self.kv, &prefix).await? {
            let path = String::from_utf8_lossy(&key);
            let record: DiskStorageRecord = mutation::decode(&path, &bytes)?;
            record.validate().map_err(|error| Error::Server(error.into()))?;
            if record.key().as_bytes() != key.as_ref() {
                return Err(Error::Server(
                    "disk storage placement differs from its key".into(),
                ));
            }
            compatible(
                media,
                unit_size,
                record.media_type,
                record.geometry.unit_size_bytes,
            )?;
            retained.insert(record.disk_id, record);
        }
        let prefix = DiskKey::text_prefix_for_disk_group(rack_id, node_id, group_id);
        for (key, bytes) in bounded_inventory(&self.kv, &prefix).await? {
            let path = String::from_utf8_lossy(&key);
            let key = DiskKey::from_path(&path).map_err(|error| Error::SysdataKeyParse(error.to_string()))?;
            let disk: DiskValue = mutation::decode(&path, &bytes)?;
            let existing_media = if let Some(record) = retained.get(&key.disk_id) {
                let mut geometry = record.geometry.clone();
                geometry.status = disk.status;
                if record.removed || geometry != disk {
                    return Err(Error::Server(
                        "disk membership differs from retained backing geometry".into(),
                    ));
                }
                record.media_type
            } else {
                Some(disk.disk_type)
            };
            compatible(media, unit_size, existing_media, disk.unit_size_bytes)?;
        }
        Ok(())
    }
}

fn compatible(media: Option<i32>, unit: u32, existing_media: Option<i32>, existing_unit: u32) -> Result<()> {
    if media != existing_media || unit != existing_unit {
        return Err(Error::Server(
            "DiskGroup media class or allocation unit is incompatible with this disk".into(),
        ));
    }
    Ok(())
}
