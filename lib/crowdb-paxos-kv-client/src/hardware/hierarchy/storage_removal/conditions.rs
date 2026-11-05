// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read current parent, owner and configured I/O participants without TTL inference.

use super::{get_json, mutation, DiskRemovalReadiness, Error, HardwareClient, Result};
use crowdb_protocol::{
    common::{HwStatus, InstanceValue, RackValue},
    disk_storage::DiskExclusionReporter,
    key::{DiskKey, InstanceKey, OwnerMapKey, RackKey, TextKey},
};

use super::super::storage_observation::bounded_inventory;

impl HardwareClient {
    pub(in crate::hardware::hierarchy) async fn removal_conditions(
        &self,
        key: &DiskKey,
    ) -> Result<DiskRemovalReadiness> {
        let retained = self
            .disk_storage(key)
            .await?
            .ok_or_else(|| Error::Server("disk removal requires retained backing identity".into()))?;
        if retained.removed {
            return Err(Error::Server(
                "disk is already removed; reconcile its receipt".into(),
            ));
        }
        self.removal_parent(key).await?;
        let (disk, revision) = self
            .get_disk_with_revision(key)
            .await?
            .ok_or_else(|| Error::Server("disk membership is absent".into()))?;
        let mut geometry = disk.clone();
        geometry.status = retained.geometry.status;
        if geometry != retained.geometry {
            return Err(Error::Server("disk removal backing geometry changed".into()));
        }
        let mut result = DiskRemovalReadiness {
            disk_revision: revision,
            conditions: Vec::new(),
        };
        if disk.status != HwStatus::Offline as i32 {
            result
                .conditions
                .push("Mark the disk Offline before removal".into());
        }
        self.removal_owner(key, &mut result).await?;
        self.removal_diskio(key, &mut result).await?;
        Ok(result)
    }

    async fn removal_parent(&self, key: &DiskKey) -> Result<()> {
        let rack: RackValue = get_json(&self.kv, &RackKey { rack_id: key.rack_id }.to_path())
            .await?
            .ok_or_else(|| Error::Server("disk removal rack is absent".into()))?;
        let node = self
            .get_node(key.rack_id, key.node_id)
            .await?
            .ok_or_else(|| Error::Server("disk removal node is absent".into()))?;
        let group = self
            .get_disk_group(key.rack_id, key.node_id, key.disk_group_id)
            .await?
            .ok_or_else(|| Error::Server("disk removal group is absent".into()))?
            .value;
        if !rack.node_ids.contains(&key.node_id)
            || !node.disk_group_ids.contains(&key.disk_group_id)
            || !group.disk_ids.contains(&key.disk_id)
        {
            return Err(Error::Server(
                "disk removal parent membership is inconsistent".into(),
            ));
        }
        Ok(())
    }

    async fn removal_owner(&self, key: &DiskKey, result: &mut DiskRemovalReadiness) -> Result<()> {
        let owner = self
            .get_owner(key.rack_id, key.node_id, key.disk_group_id)
            .await?;
        let Some(owner) = owner else {
            if self
                .get_bind(key.rack_id, key.node_id, key.disk_group_id)
                .await?
                .is_some()
            {
                result
                    .conditions
                    .push("DiskDB owner is unavailable; exclusion is unconfirmed".into());
            }
            return Ok(());
        };
        let owner_path = OwnerMapKey {
            rack_id: key.rack_id,
            node_id: key.node_id,
            disk_group_id: key.disk_group_id,
        }
        .to_path();
        let generation = mutation::read(&self.kv, &owner_path)
            .await?
            .map(|(_, revision)| revision);
        let proof = self
            .disk_exclusion(key, DiskExclusionReporter::Diskdb, owner.instance_id)
            .await?;
        if !proof.is_some_and(|proof| {
            proof.disk_revision == result.disk_revision && proof.owner_revision == generation
        }) {
            result.conditions.push(format!(
                "DiskDB {} has not confirmed this Offline revision and owner generation",
                owner.instance_id
            ));
        }
        Ok(())
    }

    async fn removal_diskio(&self, key: &DiskKey, result: &mut DiskRemovalReadiness) -> Result<()> {
        let mut participants = 0;
        for (path, bytes) in
            bounded_inventory(&self.kv, &InstanceKey::text_prefix_for_service("diskio")).await?
        {
            let path = String::from_utf8_lossy(&path);
            let instance: InstanceValue = mutation::decode(&path, &bytes)?;
            let identity =
                InstanceKey::from_path(&path).map_err(|error| Error::SysdataKeyParse(error.to_string()))?;
            if identity.instance_id != instance.instance_id {
                return Err(Error::Server(
                    "DiskIO registry identity differs from its key".into(),
                ));
            }
            let extra = instance.extra.and_then(|extra| extra.diskdb);
            let Some(extra) = extra else {
                result.conditions.push(format!(
                    "DiskIO {} physical scope is unknown",
                    instance.instance_id
                ));
                continue;
            };
            if extra.rack_id != Some(key.rack_id) || extra.node_id != Some(key.node_id) {
                if extra.rack_id.is_none() || extra.node_id.is_none() {
                    result.conditions.push(format!(
                        "DiskIO {} physical scope is unknown",
                        instance.instance_id
                    ));
                }
                continue;
            }
            let Some(scope) = extra.diskio_scope else {
                result.conditions.push(format!(
                    "DiskIO {} configured scope is unknown",
                    instance.instance_id
                ));
                continue;
            };
            if scope.version != 1 || scope.rack_id != key.rack_id || scope.node_id != key.node_id {
                return Err(Error::Server("DiskIO configured scope is inconsistent".into()));
            }
            if scope.disk_group_id != 0 && scope.disk_group_id != key.disk_group_id {
                continue;
            }
            participants += 1;
            let proof = self
                .disk_exclusion(key, DiskExclusionReporter::Diskio, instance.instance_id)
                .await?;
            if !proof.is_some_and(|proof| proof.disk_revision == result.disk_revision) {
                result.conditions.push(format!(
                    "DiskIO {} has not confirmed this Offline revision",
                    instance.instance_id
                ));
            }
        }
        if participants == 0 {
            result
                .conditions
                .push("No scoped DiskIO exclusion confirmation is available".into());
        }
        Ok(())
    }
}
