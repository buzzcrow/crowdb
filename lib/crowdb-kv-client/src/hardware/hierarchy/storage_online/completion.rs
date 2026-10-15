// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish validation outcome under exact membership and owner generations.
use super::{
    mutation, DiskKey, DiskOnlineRecord, DiskOnlineState, Error, HardwareClient, RackKey, Result, TextKey,
};
use crowdb_protocol::{
    common::{HwStatus, OwnerMapValue},
    key::OwnerMapKey,
};

impl HardwareClient {
    /// Record strict read-only validation failure, or atomically activate its
    /// exact Offline admission. Caller must hold a closed/drained local gate and
    /// the current data-group ownership fence for the whole validation.
    ///
    /// # Errors
    /// Rejects changed operations, owner generations, backing or Offline revision.
    /// Success additionally requires current owner and scoped `DiskIO` exclusions.
    pub async fn finish_disk_online(
        &self,
        key: &DiskKey,
        accepted: &DiskOnlineRecord,
        instance: u64,
        owner_generation: u64,
        failure: Option<&str>,
    ) -> Result<DiskOnlineRecord> {
        accepted.validate().map_err(|error| Error::Server(error.into()))?;
        if failure.is_none() {
            self.reserve_disk_resource(&accepted.disk).await?;
        }
        let rack_path = RackKey { rack_id: key.rack_id }.to_path();
        for _ in 0..10 {
            let (rack, revision) = self.storage_rack(key.rack_id).await?;
            let current = self
                .disk_online_operation(key)
                .await?
                .ok_or_else(|| Error::Server("disk Online operation disappeared".into()))?;
            if current.operation_id != accepted.operation_id || current.disk != accepted.disk {
                return Err(Error::Server("disk Online operation changed".into()));
            }
            if current.state != DiskOnlineState::Pending {
                return Ok(current);
            }
            let owner_path = OwnerMapKey {
                rack_id: key.rack_id,
                node_id: key.node_id,
                disk_group_id: key.disk_group_id,
            }
            .to_path();
            let (bytes, generation) = mutation::read(&self.kv, &owner_path)
                .await?
                .ok_or_else(|| Error::Server("disk Online owner disappeared".into()))?;
            let owner: OwnerMapValue = mutation::decode(&owner_path, &bytes)?;
            let (mut disk, disk_revision) = self
                .get_disk_with_revision(key)
                .await?
                .ok_or_else(|| Error::Server("disk Online membership disappeared".into()))?;
            if owner.instance_id != instance
                || generation != owner_generation
                || disk_revision != accepted.offline_revision
                || disk.status != HwStatus::Offline as i32
                || self.disk_storage(key).await?.as_ref() != Some(&accepted.disk)
            {
                return Err(Error::Server(
                    "disk Online owner, backing or revision changed".into(),
                ));
            }
            let mut outcome = current;
            outcome.state = if failure.is_some() {
                DiskOnlineState::Failed
            } else {
                DiskOnlineState::Complete
            };
            outcome.error = failure.map(|error| error.chars().take(512).collect());
            outcome.validate().map_err(|error| Error::Server(error.into()))?;
            let mut operations = vec![
                mutation::put(&rack_path, &rack)?,
                mutation::put(&DiskOnlineRecord::key_for(key), &outcome)?,
            ];
            if failure.is_none() {
                let readiness = self.removal_conditions(key).await?;
                if readiness.disk_revision != accepted.offline_revision || !readiness.conditions.is_empty() {
                    return Err(Error::Server(format!(
                        "disk Online waits for exclusion: {}",
                        readiness.conditions.join("; ")
                    )));
                }
                disk.status = HwStatus::Up as i32;
                operations.push(mutation::put(&key.to_path(), &disk)?);
            }
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
