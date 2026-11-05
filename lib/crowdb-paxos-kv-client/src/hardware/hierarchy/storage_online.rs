// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Accepted Online requests survive UI closure and require owner validation.
mod completion;
use super::{get_json, mutation, HardwareClient};
use crate::{Error, Result};
use crowdb_protocol::{
    common::HwStatus,
    disk_storage::{DiskOnlineRecord, DiskOnlineState},
    key::{DiskKey, RackKey, TextKey},
};

impl HardwareClient {
    /// Observe the latest accepted operation without claiming it completed.
    ///
    /// # Errors
    /// Rejects corrupt operations or placement inconsistent with their key.
    pub async fn disk_online_operation(&self, key: &DiskKey) -> Result<Option<DiskOnlineRecord>> {
        let result: Option<DiskOnlineRecord> = get_json(&self.kv, &DiskOnlineRecord::key_for(key)).await?;
        if let Some(record) = &result {
            record.validate().map_err(|error| Error::Server(error.into()))?;
            if record.disk.key() != crowdb_protocol::disk_storage::DiskStorageRecord::key_for(key) {
                return Err(Error::Server("disk Online placement differs from its key".into()));
            }
        }
        Ok(result)
    }

    /// Accept validation after the caller verifies the original physical medium.
    /// This never changes Offline status; the current owner performs validation.
    ///
    /// # Errors
    /// Rejects changed identity, geometry or revision. Same-operation retries
    /// reconcile Pending/Complete and explicitly retry Failed validation.
    pub async fn request_disk_online(
        &self,
        key: &DiskKey,
        offline_revision: u64,
    ) -> Result<DiskOnlineRecord> {
        let rack_path = RackKey { rack_id: key.rack_id }.to_path();
        for _ in 0..10 {
            let (rack, revision) = self.storage_rack(key.rack_id).await?;
            let retained = self
                .disk_storage(key)
                .await?
                .filter(|disk| !disk.removed)
                .ok_or_else(|| Error::Server("disk Online requires retained backing".into()))?;
            let expected = DiskOnlineRecord::new(retained, offline_revision);
            expected.validate().map_err(|error| Error::Server(error.into()))?;
            let (disk, revision_of_disk) = self
                .get_disk_with_revision(key)
                .await?
                .ok_or_else(|| Error::Server("disk Online membership is absent".into()))?;
            let mut geometry = disk.clone();
            geometry.status = expected.disk.geometry.status;
            if let Some(current) = self.disk_online_operation(key).await? {
                if current.operation_id == expected.operation_id
                    && current.disk == expected.disk
                    && geometry == expected.disk.geometry
                    && ((current.state == DiskOnlineState::Complete && disk.status == HwStatus::Up as i32)
                        || (current.state == DiskOnlineState::Pending
                            && current.offline_revision == offline_revision
                            && disk.status == HwStatus::Offline as i32
                            && revision_of_disk == offline_revision))
                {
                    return Ok(current);
                }
            }
            if revision_of_disk != offline_revision
                || disk.status != HwStatus::Offline as i32
                || geometry != expected.disk.geometry
            {
                return Err(Error::Server(
                    "disk Online Offline revision or geometry changed".into(),
                ));
            }
            self.storage_parent(&expected.disk, &rack).await?;
            let operations = [
                mutation::put(&rack_path, &rack)?,
                mutation::put(&DiskOnlineRecord::key_for(key), &expected)?,
            ];
            match self
                .kv
                .batch_write_cas(0, 0, &operations, rack_path.as_bytes(), revision)
                .await
            {
                Ok(_) => return Ok(expected),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }
}
