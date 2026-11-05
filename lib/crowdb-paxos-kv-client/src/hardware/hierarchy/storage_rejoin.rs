// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Restore original Offline membership under the shared hierarchy fence.

use super::{get_json, mutation, HardwareClient};
use crate::{Error, Result};
use crowdb_protocol::{
    common::HwStatus,
    disk_storage::DiskRejoinRecord,
    key::{DiskGroupKey, DiskKey, RackKey, TextKey},
};

impl HardwareClient {
    /// Read the retained rejoin source; its presence forbids blank initialization.
    ///
    /// # Errors
    /// Rejects unsupported schemas and records stored at another disk's key.
    pub async fn disk_rejoin_receipt(&self, key: &DiskKey) -> Result<Option<DiskRejoinRecord>> {
        let receipt: Option<DiskRejoinRecord> = get_json(&self.kv, &DiskRejoinRecord::key_for(key)).await?;
        if let Some(receipt) = &receipt {
            receipt.validate().map_err(|error| Error::Server(error.into()))?;
            if receipt.source.disk.key() != crowdb_protocol::disk_storage::DiskStorageRecord::key_for(key) {
                return Err(Error::Server(
                    "disk rejoin source differs from lookup placement".into(),
                ));
            }
        }
        Ok(receipt)
    }

    /// Restore the exact retained medium Offline at its original placement.
    /// The caller must first verify that node-local media still matches the
    /// retained identity and geometry. This never clears allocations or opens I/O.
    ///
    /// # Errors
    /// Rejects changed removal identity, geometry, membership and compatibility.
    /// Unknown commits reconcile the same retained source without changing status.
    pub async fn rejoin_disk_storage(
        &self,
        key: &DiskKey,
        removal_operation: &str,
    ) -> Result<DiskRejoinRecord> {
        for _ in 0..10 {
            let (rack, revision) = self.storage_rack(key.rack_id).await?;
            let source = self
                .disk_removal_receipt(key)
                .await?
                .filter(|receipt| receipt.operation_id == removal_operation)
                .ok_or_else(|| Error::Server("disk rejoin removal operation changed".into()))?;
            let outcome = DiskRejoinRecord::new(source);
            let restored = outcome.restored();
            self.reserve_disk_resource(&restored).await?;
            let retained = self
                .disk_storage(key)
                .await?
                .ok_or_else(|| Error::Server("disk rejoin backing is absent".into()))?;
            let mut group = self.storage_parent(&restored, &rack).await?;
            let current = self.get_disk_with_revision(key).await?;
            if !retained.removed {
                let mut current = current
                    .ok_or_else(|| Error::Server("rejoined disk membership is absent".into()))?
                    .0;
                current.status = restored.geometry.status;
                if retained == restored
                    && current == restored.geometry
                    && group.disk_ids.contains(&key.disk_id)
                    && self.disk_rejoin_receipt(key).await?.as_ref() == Some(&outcome)
                {
                    return Ok(outcome);
                }
                return Err(Error::Server(
                    "disk rejoin conflicts with restored membership".into(),
                ));
            }
            if retained != outcome.source.disk || current.is_some() || group.disk_ids.contains(&key.disk_id) {
                return Err(Error::Server(
                    "disk rejoin retained source or membership changed".into(),
                ));
            }
            self.validate_disk_group_storage(
                key.rack_id,
                key.node_id,
                key.disk_group_id,
                restored.media_type,
                restored.geometry.unit_size_bytes,
            )
            .await?;
            self.check_storage_duplicates(&restored, Some(&outcome.source.disk))
                .await?;
            let mut membership = restored.geometry.clone();
            membership.status = HwStatus::Offline as i32;
            group.disk_ids.push(key.disk_id);
            group.disk_ids.sort_unstable_by_key(|id| (id.high, id.low));
            let rack_path = RackKey { rack_id: key.rack_id }.to_path();
            let group_path = DiskGroupKey {
                rack_id: key.rack_id,
                node_id: key.node_id,
                disk_group_id: key.disk_group_id,
            }
            .to_path();
            let operations = [
                mutation::put(&rack_path, &rack)?,
                mutation::put(&group_path, &group)?,
                mutation::put(&key.to_path(), &membership)?,
                mutation::put(&restored.key(), &restored)?,
                mutation::put(&DiskRejoinRecord::key_for(key), &outcome)?,
            ];
            match self
                .kv
                .batch_write_cas(0, 0, &operations, rack_path.as_bytes(), revision)
                .await
            {
                Ok(_) => return Ok(outcome),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }
}
