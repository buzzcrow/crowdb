// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unknown physical media cannot bypass retained removal through legacy cleanup.
use super::{mutation, storage_observation::bounded_inventory, HardwareClient};
use crate::{Error, Result};
use crowdb_protocol::{
    diskdb::rpc::DiskValue,
    key::{DiskGroupKey, DiskKey, NodeKey, RackKey, TextKey},
};

impl HardwareClient {
    pub(super) async fn reject_legacy_media_deletion(&self, target: &str) -> Result<()> {
        let prefix = if let Ok(key) = DiskKey::from_path(target) {
            key.to_path()
        } else if let Ok(key) = DiskGroupKey::from_path(target) {
            format!("/hw/disk/{}/{}/{}/", key.rack_id, key.node_id, key.disk_group_id)
        } else if let Ok(key) = NodeKey::from_path(target) {
            format!("/hw/disk/{}/{}/", key.rack_id, key.node_id)
        } else if let Ok(key) = RackKey::from_path(target) {
            format!("/hw/disk/{}/", key.rack_id)
        } else {
            return Ok(());
        };
        for (path, bytes) in bounded_inventory(&self.kv, &prefix).await? {
            let disk: DiskValue = mutation::decode(&String::from_utf8_lossy(&path), &bytes)?;
            if !disk.device_path.is_empty() {
                return Err(Error::Server("legacy physical disk backing is unverified; direct deletion and parent cleanup cannot bypass Offline/Remove".into()));
            }
        }
        Ok(())
    }
}
