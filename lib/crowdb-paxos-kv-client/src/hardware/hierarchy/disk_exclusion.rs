// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 accepts exclusion evidence only for exact current authority.

use super::{get_json, mutation, HardwareClient};
use crate::{BatchOp, Error, Result};
use crowdb_protocol::{
    common::{HwStatus, InstanceValue, OwnerMapValue},
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter},
    key::{DiskKey, InstanceKey, OwnerMapKey, RackKey, TextKey},
};

impl HardwareClient {
    /// Persist a service's confirmed exclusion after local admission closes and
    /// admitted work drains. Callers must not infer this from liveness or status.
    ///
    /// # Errors
    /// Rejects another disk revision, non-Offline membership, changed owner,
    /// mismatched service location, and unavailable authority.
    pub async fn acknowledge_disk_exclusion(&self, record: &DiskExclusionRecord) -> Result<()> {
        record.validate().map_err(|error| Error::Server(error.into()))?;
        let rack_path = RackKey {
            rack_id: record.rack_id,
        }
        .to_path();
        for _ in 0..10 {
            let (rack, revision) = mutation::read(&self.kv, &rack_path)
                .await?
                .ok_or_else(|| Error::Server("disk exclusion rack is absent".into()))?;
            let (disk, disk_revision) = self
                .get_disk_with_revision(&record.disk_key())
                .await?
                .ok_or_else(|| Error::Server("disk exclusion membership is absent".into()))?;
            if disk.status != HwStatus::Offline as i32 || disk_revision != record.disk_revision {
                return Err(Error::Server(
                    "disk exclusion requires the exact Offline revision".into(),
                ));
            }
            self.validate_exclusion_reporter(record).await?;
            let path = record.key();
            let existing: Option<DiskExclusionRecord> = get_json(&self.kv, &path).await?;
            if existing.as_ref() == Some(record) {
                return Ok(());
            }
            let operations = [
                mutation::put(&path, record)?,
                BatchOp::Put {
                    key: rack_path.as_bytes().to_vec().into(),
                    value: rack,
                },
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

    /// # Errors
    /// Propagates invalid stored evidence and authoritative read failures.
    pub async fn disk_exclusion(
        &self,
        disk: &DiskKey,
        reporter: DiskExclusionReporter,
        instance_id: u64,
    ) -> Result<Option<DiskExclusionRecord>> {
        let path = DiskExclusionRecord::key_for(disk, reporter, instance_id);
        let record: Option<DiskExclusionRecord> = get_json(&self.kv, &path).await?;
        if let Some(record) = &record {
            record.validate().map_err(|error| Error::Server(error.into()))?;
            if record.key() != path {
                return Err(Error::Server(
                    "disk exclusion identity differs from its key".into(),
                ));
            }
        }
        Ok(record)
    }

    async fn validate_exclusion_reporter(&self, record: &DiskExclusionRecord) -> Result<()> {
        match record.reporter {
            DiskExclusionReporter::Diskdb => {
                let path = OwnerMapKey {
                    rack_id: record.rack_id,
                    node_id: record.node_id,
                    disk_group_id: record.disk_group_id,
                }
                .to_path();
                let (bytes, revision) = mutation::read(&self.kv, &path)
                    .await?
                    .ok_or_else(|| Error::Server("disk exclusion owner is absent".into()))?;
                let owner: OwnerMapValue = mutation::decode(&path, &bytes)?;
                if owner.instance_id != record.instance_id || Some(revision) != record.owner_revision {
                    return Err(Error::Server("disk exclusion owner generation changed".into()));
                }
            }
            DiskExclusionReporter::Diskio => {
                let path = InstanceKey {
                    service: "diskio".into(),
                    instance_id: record.instance_id,
                }
                .to_path();
                let instance: InstanceValue = get_json(&self.kv, &path)
                    .await?
                    .ok_or_else(|| Error::Server("disk exclusion service is absent".into()))?;
                let location = instance
                    .extra
                    .and_then(|extra| extra.diskdb)
                    .ok_or_else(|| Error::Server("disk exclusion service location is unknown".into()))?;
                let covers_group = match &location.diskio_scope {
                    Some(scope) => {
                        scope.version == 1
                            && scope.rack_id == record.rack_id
                            && scope.node_id == record.node_id
                            && (scope.disk_group_id == 0 || scope.disk_group_id == record.disk_group_id)
                    }
                    None => location.owned_dg_ids.contains(&record.disk_group_id),
                };
                if location.rack_id != Some(record.rack_id)
                    || location.node_id != Some(record.node_id)
                    || !covers_group
                {
                    return Err(Error::Server(
                        "disk exclusion service location does not match".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}
