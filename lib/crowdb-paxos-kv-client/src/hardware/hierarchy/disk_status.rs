// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional disk status writes preserve concurrent lifecycle changes.

use crowdb_protocol::{
    common::{DiskId, HwStatus, OwnerMapValue},
    diskdb::rpc::DiskValue,
    key::{DiskKey, OwnerMapKey, RackKey, TextKey},
};

use super::{mutation, HardwareClient, G0_GROUP, G0_STORE};
use crate::{Error, GetOutcome, ReadMode, Result};

impl HardwareClient {
    /// Read a disk and its authoritative revision for exclusion observations.
    ///
    /// # Errors
    /// Propagates Group-0 read and record decoding errors.
    pub async fn get_disk_with_revision(&self, key: &DiskKey) -> Result<Option<(DiskValue, u64)>> {
        let path = key.to_path();
        match self
            .kv
            .get(G0_STORE, G0_GROUP, path.as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::Found { value, revision, .. } => {
                let value = serde_json::from_slice(&value).map_err(|error| Error::SysdataDecode {
                    key: path,
                    reason: error.to_string(),
                })?;
                Ok(Some((value, revision)))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }

    /// Change an existing disk's status at the observed revision.
    ///
    /// # Errors
    /// Missing disks fail without creating metadata. Concurrent changes and
    /// unknown outcomes propagate; callers must reconcile before retrying.
    pub async fn set_disk_status(
        &self,
        rack_id: u64,
        node_id: u64,
        dg_id: u64,
        disk_id: &DiskId,
        status: HwStatus,
    ) -> Result<()> {
        let key = DiskKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
            disk_id: *disk_id,
        };
        mutation::update::<DiskValue, _>(&self.kv, rack_id, &key.to_path(), |value| {
            if value.status == HwStatus::Offline as i32 && status != HwStatus::Offline {
                return Err(Error::Server(
                    "Offline disks require validated recovery before status changes".into(),
                ));
            }
            value.status = status as i32;
            Ok(())
        })
        .await
    }
}

impl HardwareClient {
    /// Quarantine a failed load under both ownership and membership fencing.
    /// Advancing the owner revision invalidates the loader's captured generation.
    ///
    /// # Errors
    /// Rejects changed owners, absent disks, concurrent hierarchy mutation and
    /// unknown outcomes. Legacy unparented fixtures retain owner-CAS semantics.
    pub async fn mark_disk_offline_if_owned(
        &self,
        key: &DiskKey,
        instance_id: u64,
        owner_revision: u64,
    ) -> Result<()> {
        let rack_path = RackKey { rack_id: key.rack_id }.to_path();
        let rack = mutation::read(&self.kv, &rack_path).await?;
        let owner_path = OwnerMapKey {
            rack_id: key.rack_id,
            node_id: key.node_id,
            disk_group_id: key.disk_group_id,
        }
        .to_path();
        let (owner, generation) = mutation::read(&self.kv, &owner_path)
            .await?
            .ok_or_else(|| Error::Server("DiskGroup owner disappeared".into()))?;
        let current: OwnerMapValue = mutation::decode(&owner_path, &owner)?;
        if current.instance_id != instance_id || generation != owner_revision {
            return Err(Error::Server(
                "DiskGroup ownership changed; stale DiskDB writes are fenced".into(),
            ));
        }
        let path = key.to_path();
        let (bytes, _) = mutation::read(&self.kv, &path)
            .await?
            .ok_or_else(|| Error::Server("disk membership disappeared".into()))?;
        let mut value: DiskValue = mutation::decode(&path, &bytes)?;
        value.status = HwStatus::Offline as i32;
        let mut operations = vec![
            mutation::put(&path, &value)?,
            crate::BatchOp::Put {
                key: owner_path.as_bytes().to_vec().into(),
                value: owner,
            },
        ];
        let (condition, revision) = if let Some((bytes, revision)) = rack {
            operations.push(crate::BatchOp::Put {
                key: rack_path.as_bytes().to_vec().into(),
                value: bytes,
            });
            (rack_path.as_str(), revision)
        } else {
            (owner_path.as_str(), generation)
        };
        self.kv
            .batch_write_cas(0, 0, &operations, condition.as_bytes(), revision)
            .await?;
        Ok(())
    }
}

impl HardwareClient {
    /// Close one retained disk at the revision the operator inspected.
    ///
    /// # Errors
    /// Rejects stale active membership, removed disks and changed geometry.
    /// Unknown writes reconcile the same Offline state without reopening it.
    pub async fn offline_disk_storage(&self, key: &DiskKey, expected_revision: u64) -> Result<u64> {
        if expected_revision == 0 {
            return Err(Error::Server("disk revision is required".into()));
        }
        let rack_path = RackKey { rack_id: key.rack_id }.to_path();
        for _ in 0..10 {
            let (rack, revision) = mutation::read(&self.kv, &rack_path)
                .await?
                .ok_or_else(|| Error::Server("disk exclusion rack is absent".into()))?;
            let receipt = self
                .disk_storage(key)
                .await?
                .filter(|record| !record.removed)
                .ok_or_else(|| Error::Server("disk exclusion requires registered retained backing".into()))?;
            let (mut disk, current_revision) = self
                .get_disk_with_revision(key)
                .await?
                .ok_or_else(|| Error::Server("disk membership is absent".into()))?;
            let mut geometry = disk.clone();
            geometry.status = receipt.geometry.status;
            if geometry != receipt.geometry {
                return Err(Error::Server("disk backing geometry changed".into()));
            }
            if disk.status == HwStatus::Offline as i32 {
                return Ok(current_revision);
            }
            if current_revision != expected_revision {
                return Err(Error::Server(
                    "disk revision changed; refresh before marking Offline".into(),
                ));
            }
            disk.status = HwStatus::Offline as i32;
            let operations = [
                mutation::put(&key.to_path(), &disk)?,
                crate::BatchOp::Put {
                    key: rack_path.as_bytes().to_vec().into(),
                    value: rack,
                },
            ];
            match self
                .kv
                .batch_write_cas(0, 0, &operations, rack_path.as_bytes(), revision)
                .await
            {
                Ok(result) => return Ok(result.revision),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }
}
