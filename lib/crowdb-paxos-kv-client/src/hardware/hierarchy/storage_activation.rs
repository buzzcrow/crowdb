// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Initial activation cannot override administrative exclusion or changed media.

use crowdb_protocol::{
    common::HwStatus,
    disk_storage::DiskStorageRecord,
    diskdb::rpc::DiskValue,
    key::{DiskKey, TextKey},
};

use super::{mutation, HardwareClient};
use crate::{Error, Result};

impl HardwareClient {
    /// Activate initial admission after the caller verifies the exact medium via `DiskIO`.
    ///
    /// # Errors
    /// Rejects changed receipts/geometry, missing membership and any status other
    /// than Init or already Up. This never authorizes Offline recovery.
    pub async fn activate_disk_storage(&self, record: &DiskStorageRecord) -> Result<()> {
        let key = DiskKey {
            rack_id: record.rack_id,
            node_id: record.node_id,
            disk_group_id: record.disk_group_id,
            disk_id: record.disk_id,
        };
        if record.removed || self.disk_storage(&key).await?.as_ref() != Some(record) {
            return Err(Error::Server("disk activation backing receipt changed".into()));
        }
        self.reserve_disk_resource(record).await?;
        mutation::update::<DiskValue, _>(&self.kv, record.rack_id, &key.to_path(), |value| {
            if !matches!(
                HwStatus::try_from(value.status),
                Ok(HwStatus::Init | HwStatus::Up)
            ) {
                return Err(Error::Server(
                    "disk activation cannot override current health or Offline exclusion".into(),
                ));
            }
            let mut geometry = value.clone();
            geometry.status = record.geometry.status;
            if geometry != record.geometry {
                return Err(Error::Server("disk activation geometry changed".into()));
            }
            value.status = HwStatus::Up as i32;
            Ok(())
        })
        .await
    }
}
