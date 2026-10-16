// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Physical observations may close an exact membership, never reopen it.

use crowdb_protocol::{
    common::HwStatus,
    disk_storage::DiskStorageRecord,
    key::{DiskKey, RackKey, TextKey},
};

use super::{mutation, HardwareClient};
use crate::{BatchOp, Error, Result};

impl HardwareClient {
    /// Quarantine a failed physical observation at its captured membership.
    ///
    /// # Errors
    /// Rejects changed placement, backing, membership or hierarchy. A concurrent
    /// lifecycle operation requires a fresh physical observation, never a retry
    /// against a newly read revision.
    pub async fn quarantine_disk_storage(
        &self,
        record: &DiskStorageRecord,
        expected_revision: u64,
    ) -> Result<()> {
        record.validate().map_err(|error| Error::Server(error.into()))?;
        let key = DiskKey {
            rack_id: record.rack_id,
            node_id: record.node_id,
            disk_group_id: record.disk_group_id,
            disk_id: record.disk_id,
        };
        let rack_path = RackKey {
            rack_id: record.rack_id,
        }
        .to_path();
        let (rack, rack_revision) = mutation::read(&self.kv, &rack_path)
            .await?
            .ok_or_else(|| Error::Server("physical observation rack is absent".into()))?;
        if record.removed || self.disk_storage(&key).await?.as_ref() != Some(record) {
            return Err(Error::Server("physical observation backing changed".into()));
        }
        let (mut disk, revision) = self
            .get_disk_with_revision(&key)
            .await?
            .ok_or_else(|| Error::Server("physical observation membership is absent".into()))?;
        let mut geometry = disk.clone();
        geometry.status = record.geometry.status;
        if expected_revision == 0 || revision != expected_revision || geometry != record.geometry {
            return Err(Error::Server("physical observation membership changed".into()));
        }
        if disk.status == HwStatus::Offline as i32 {
            return Ok(());
        }
        disk.status = HwStatus::Offline as i32;
        self.kv
            .batch_write_cas(
                0,
                0,
                &[
                    mutation::put(&key.to_path(), &disk)?,
                    BatchOp::Put {
                        key: rack_path.as_bytes().to_vec().into(),
                        value: rack,
                    },
                ],
                rack_path.as_bytes(),
                rack_revision,
            )
            .await?;
        Ok(())
    }
}
