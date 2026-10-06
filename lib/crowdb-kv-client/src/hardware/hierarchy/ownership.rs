// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! Revision-checked initial owner publication and renewal.
use super::{get_json, scan_prefix, HardwareClient, G0_GROUP, G0_STORE};
use crate::{BatchOp, Error, GetOutcome, Result};
use crowdb_protocol::common_type::{DiskGroupId, NodeId, RackId};
use crowdb_protocol::diskdb::rpc::DiskGroupValue;
use crowdb_protocol::key::{DiskGroupKey, OwnerMapKey, TextKey};
use crowdb_protocol::sysdata::DiskdbOwnerEntry;
// ── ownership map ───────────────────────────────────────────────

impl HardwareClient {
    /// Atomically persist a disk-group record and its initial
    /// owner in one group-0 batch.
    pub async fn add_disk_group_with_owner(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_group: &DiskGroupValue,
        instance_id: u64,
        lease_expiry_ms: u64,
    ) -> Result<()> {
        let revision = self.owner_revision(rack_id, node_id, dg_id, instance_id).await?;
        let group_key = DiskGroupKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        }
        .to_path();
        let owner_key = OwnerMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        }
        .to_path();
        let owner = crowdb_protocol::common::OwnerMapValue {
            instance_id,
            lease_expiry_ms,
        };
        let group_value = serde_json::to_vec(disk_group).map_err(|error| Error::SysdataDecode {
            key: group_key.clone(),
            reason: error.to_string(),
        })?;
        let owner_value = serde_json::to_vec(&owner).map_err(|error| Error::SysdataDecode {
            key: owner_key.clone(),
            reason: error.to_string(),
        })?;
        self.kv
            .batch_write_cas(
                G0_STORE,
                G0_GROUP,
                &[
                    BatchOp::Put {
                        key: bytes::Bytes::from(group_key),
                        value: bytes::Bytes::from(group_value),
                    },
                    BatchOp::Put {
                        key: bytes::Bytes::from(owner_key.clone()),
                        value: bytes::Bytes::from(owner_value),
                    },
                ],
                owner_key.as_bytes(),
                revision,
            )
            .await
            .map(|_| ())
    }

    /// Create an ownership-map entry or renew its lease for the same
    /// instance. Replacing an existing owner is rejected.
    pub async fn set_owner(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        instance_id: u64,
        lease_expiry_ms: u64,
    ) -> Result<()> {
        let revision = self.owner_revision(rack_id, node_id, dg_id, instance_id).await?;
        let key = OwnerMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        let value = crowdb_protocol::common::OwnerMapValue {
            instance_id,
            lease_expiry_ms,
        };
        let value = serde_json::to_vec(&value).map_err(|error| Error::Server(error.to_string()))?;
        self.kv
            .put_cas(G0_STORE, G0_GROUP, key.to_path().as_bytes(), &value, revision)
            .await
            .map(|_| ())
    }

    /// Read the ownership-map entry for a disk-group.
    pub async fn get_owner(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Option<DiskdbOwnerEntry>> {
        let key = OwnerMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        let value = get_json::<crowdb_protocol::common::OwnerMapValue>(&self.kv, &key.to_path()).await?;
        Ok(value.map(|v| DiskdbOwnerEntry {
            rack_id,
            node_id,
            dg_id,
            instance_id: v.instance_id,
            lease_expiry_ms: v.lease_expiry_ms,
        }))
    }

    /// List all ownership-map entries (prefix scan `/hw/dg_owner/`).
    pub async fn list_owners(&self) -> Result<Vec<DiskdbOwnerEntry>> {
        let entries = scan_prefix::<crowdb_protocol::common::OwnerMapValue>(
            &self.kv,
            &<OwnerMapKey as TextKey>::prefix_all(),
        )
        .await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = OwnerMapKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push(DiskdbOwnerEntry {
                rack_id: k.rack_id,
                node_id: k.node_id,
                dg_id: k.disk_group_id,
                instance_id: value.instance_id,
                lease_expiry_ms: value.lease_expiry_ms,
            });
        }
        Ok(out)
    }

    /// Remove the ownership-map entry for a disk-group.
    pub async fn remove_owner(&self, rack_id: RackId, node_id: NodeId, dg_id: DiskGroupId) -> Result<()> {
        let key = OwnerMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }
}

impl HardwareClient {
    async fn owner_revision(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        instance_id: u64,
    ) -> Result<u64> {
        let key = OwnerMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        }
        .to_path();
        match self
            .kv
            .get(
                G0_STORE,
                G0_GROUP,
                key.as_bytes(),
                crate::ReadMode::Linearizable,
                None,
            )
            .await?
        {
            GetOutcome::NotFound => Ok(0),
            GetOutcome::Found { value, revision } => {
                let current: crowdb_protocol::common::OwnerMapValue =
                    serde_json::from_slice(&value).map_err(|error| Error::Server(error.to_string()))?;
                if current.instance_id != instance_id {
                    return Err(Error::OwnerConflict {
                        disk_group_id: dg_id,
                        current: current.instance_id,
                        requested: instance_id,
                    });
                }
                Ok(revision)
            }
        }
    }
}
