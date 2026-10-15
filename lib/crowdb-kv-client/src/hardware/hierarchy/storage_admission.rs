// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Atomic backing identity and physical disk membership publication.

use crowdb_protocol::{
    common::{HwStatus, RackValue},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::{DiskGroupValue, DiskValue},
    key::{DiskGroupKey, DiskKey, RackKey, TextKey},
};

use super::{get_json, HardwareClient};
use crate::{BatchOp, Error, GetOutcome, ReadMode, Result};

impl HardwareClient {
    /// Publish one prepared medium together with its immutable backing receipt.
    /// Callers must verify node-local preparation before supplying the record.
    ///
    /// # Errors
    /// Rejects missing parents, duplicate resources, changed intents and removed
    /// identities. Unknown write outcomes reconcile the same immutable record.
    pub async fn admit_disk_storage(&self, record: &DiskStorageRecord) -> Result<()> {
        record.validate().map_err(|error| Error::Server(error.into()))?;
        if record.removed || record.geometry.status != HwStatus::Init as i32 {
            return Err(Error::Server(
                "new disk admission requires Init membership".into(),
            ));
        }
        for _ in 0..10 {
            let (rack, revision) = self.storage_rack(record.rack_id).await?;
            let mut group = self.storage_parent(record, &rack).await?;
            if self.storage_replay(record, &group).await? {
                return Ok(());
            }
            self.validate_disk_group_storage(
                record.rack_id,
                record.node_id,
                record.disk_group_id,
                record.media_type,
                record.geometry.unit_size_bytes,
            )
            .await?;
            self.check_storage_duplicates(record, None).await?;
            group.disk_ids.push(record.disk_id);
            group.disk_ids.sort_unstable_by_key(|id| (id.high, id.low));
            let rack_path = RackKey {
                rack_id: record.rack_id,
            }
            .to_path();
            let group_path = group_key(record).to_path();
            let operations = [
                put(&rack_path, &rack)?,
                put(&group_path, &group)?,
                put(&disk_key(record).to_path(), &record.geometry)?,
                put(&record.key(), record)?,
            ];
            match self
                .kv
                .batch_write_cas(0, 0, &operations, rack_path.as_bytes(), revision)
                .await
            {
                Ok(_) => return Ok(()),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }

    /// Read retained backing metadata, including explicitly removed identities.
    ///
    /// # Errors
    /// Propagates authority, decoding and unsupported-schema errors.
    pub async fn disk_storage(&self, key: &DiskKey) -> Result<Option<DiskStorageRecord>> {
        let path = DiskStorageRecord::key_for(key);
        let value: Option<DiskStorageRecord> = get_json(&self.kv, &path).await?;
        if let Some(record) = &value {
            record.validate().map_err(|error| Error::Server(error.into()))?;
            if record.key() != path {
                return Err(Error::Server(
                    "disk storage placement differs from its key".into(),
                ));
            }
        }
        Ok(value)
    }

    pub(super) async fn storage_rack(&self, rack_id: u64) -> Result<(RackValue, u64)> {
        let path = RackKey { rack_id }.to_path();
        match self
            .kv
            .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::Found { value, revision } => Ok((decode(&path, &value)?, revision)),
            GetOutcome::NotFound => Err(Error::Server("disk admission rack does not exist".into())),
        }
    }

    pub(super) async fn storage_parent(
        &self,
        record: &DiskStorageRecord,
        rack: &RackValue,
    ) -> Result<DiskGroupValue> {
        let node = self
            .get_node(record.rack_id, record.node_id)
            .await?
            .ok_or_else(|| Error::Server("disk admission node does not exist".into()))?;
        if !rack.node_ids.contains(&record.node_id) || !node.disk_group_ids.contains(&record.disk_group_id) {
            return Err(Error::Server(
                "disk admission parent membership is inconsistent".into(),
            ));
        }
        get_json(&self.kv, &group_key(record).to_path())
            .await?
            .ok_or_else(|| Error::Server("disk admission group does not exist".into()))
    }

    async fn storage_replay(&self, record: &DiskStorageRecord, group: &DiskGroupValue) -> Result<bool> {
        let actual = self.get_disk_with_revision(&disk_key(record)).await?;
        let retained = self.disk_storage(&disk_key(record)).await?;
        match (actual, retained) {
            (None, None) if !group.disk_ids.contains(&record.disk_id) => Ok(false),
            (Some((mut disk, _)), Some(retained)) => {
                disk.status = record.geometry.status;
                if retained == *record && disk == record.geometry && group.disk_ids.contains(&record.disk_id)
                {
                    Ok(true)
                } else {
                    Err(Error::Server(
                        "disk admission conflicts with retained identity or geometry".into(),
                    ))
                }
            }
            _ => Err(Error::Server(
                "disk admission requires explicit recovery of existing identity".into(),
            )),
        }
    }

    pub(super) async fn check_storage_duplicates(
        &self,
        record: &DiskStorageRecord,
        retained_source: Option<&DiskStorageRecord>,
    ) -> Result<()> {
        let prefix = DiskStorageRecord::node_prefix(record.rack_id, record.node_id);
        let rows = super::storage_observation::bounded_inventory(&self.kv, &prefix).await?;
        for (key, value) in rows {
            let existing: DiskStorageRecord = decode(&String::from_utf8_lossy(&key), &value)?;
            existing.validate().map_err(|error| Error::Server(error.into()))?;
            if existing.key().as_bytes() != key.as_ref() {
                return Err(Error::Server(
                    "disk storage placement differs from its key".into(),
                ));
            }
            if retained_source == Some(&existing) && existing.key() == record.key() {
                continue;
            }
            if existing.backing_identity == record.backing_identity
                || existing.geometry.device_path == record.geometry.device_path
                || existing.disk_id == record.disk_id
            {
                return Err(Error::Server(
                    "disk resource is already admitted or retained as removed".into(),
                ));
            }
        }
        let prefix = format!("/hw/disk/{}/{}/", record.rack_id, record.node_id);
        let rows = super::storage_observation::bounded_inventory(&self.kv, &prefix).await?;
        for (key, value) in rows {
            let disk: DiskValue = decode(&String::from_utf8_lossy(&key), &value)?;
            let key = DiskKey::from_path(&String::from_utf8_lossy(&key))
                .map_err(|error| Error::SysdataKeyParse(error.to_string()))?;
            if disk.device_path == record.geometry.device_path || key.disk_id == record.disk_id {
                return Err(Error::Server(
                    "disk path already belongs to an existing disk".into(),
                ));
            }
        }
        Ok(())
    }
}

fn group_key(record: &DiskStorageRecord) -> DiskGroupKey {
    DiskGroupKey {
        rack_id: record.rack_id,
        node_id: record.node_id,
        disk_group_id: record.disk_group_id,
    }
}

fn disk_key(record: &DiskStorageRecord) -> DiskKey {
    DiskKey {
        rack_id: record.rack_id,
        node_id: record.node_id,
        disk_group_id: record.disk_group_id,
        disk_id: record.disk_id,
    }
}

fn put<T: serde::Serialize>(key: &str, value: &T) -> Result<BatchOp> {
    let value = serde_json::to_vec(value).map_err(|error| Error::SysdataDecode {
        key: key.into(),
        reason: error.to_string(),
    })?;
    Ok(BatchOp::Put {
        key: key.as_bytes().to_vec().into(),
        value: value.into(),
    })
}

fn decode<T: serde::de::DeserializeOwned>(key: &str, value: &[u8]) -> Result<T> {
    serde_json::from_slice(value).map_err(|error| Error::SysdataDecode {
        key: key.into(),
        reason: error.to_string(),
    })
}
