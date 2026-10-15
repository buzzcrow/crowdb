// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Administrative removal retains backing interpretation and every data record.

use super::{get_json, mutation, HardwareClient};
use crate::{BatchOp, Error, Result};
mod conditions;

use crowdb_protocol::{
    disk_storage::DiskRemovalRecord,
    key::{DiskGroupKey, DiskKey, RackKey, TextKey},
};

#[derive(Clone, Debug)]
pub struct DiskRemovalReadiness {
    pub disk_revision: u64,
    pub conditions: Vec<String>,
}

impl HardwareClient {
    /// Read the last committed removal for reconciliation, including after restart.
    ///
    /// # Errors
    /// Rejects corrupt receipts and a placement inconsistent with the lookup key.
    pub async fn disk_removal_receipt(&self, key: &DiskKey) -> Result<Option<DiskRemovalRecord>> {
        let record: Option<DiskRemovalRecord> = get_json(&self.kv, &DiskRemovalRecord::key_for(key)).await?;
        if let Some(record) = &record {
            record.validate().map_err(|error| Error::Server(error.into()))?;
            if crowdb_protocol::disk_storage::DiskStorageRecord::key_for(key) != record.disk.key() {
                return Err(Error::Server(
                    "disk removal placement differs from its key".into(),
                ));
            }
        }
        Ok(record)
    }

    /// Observe all current exclusion requirements without modifying membership.
    ///
    /// # Errors
    /// Rejects inconsistent authority or an unstable hierarchy observation.
    pub async fn disk_removal_readiness(&self, key: &DiskKey) -> Result<DiskRemovalReadiness> {
        let rack = RackKey { rack_id: key.rack_id }.to_path();
        for _ in 0..3 {
            let before = mutation::read(&self.kv, &rack)
                .await?
                .ok_or_else(|| Error::Server("disk removal rack is absent".into()))?;
            let result = self.removal_conditions(key).await?;
            if mutation::read(&self.kv, &rack)
                .await?
                .map(|(_, revision)| revision)
                == Some(before.1)
            {
                return Ok(result);
            }
        }
        Err(Error::OutcomeUnknown)
    }

    /// Remove the exact confirmed Offline membership. No allocation metadata,
    /// file bytes, binding, owner or interpretation receipt is deleted.
    ///
    /// # Errors
    /// Rejects Online/unconfirmed/stale requests. Unknown results reconcile the
    /// same disk/revision receipt; later incarnations cannot replay old removal.
    pub async fn remove_disk_storage(
        &self,
        key: &DiskKey,
        expected_revision: u64,
    ) -> Result<DiskRemovalRecord> {
        if expected_revision == 0 {
            return Err(Error::Server("disk removal revision is required".into()));
        }
        let rack_path = RackKey { rack_id: key.rack_id }.to_path();
        let group_path = DiskGroupKey {
            rack_id: key.rack_id,
            node_id: key.node_id,
            disk_group_id: key.disk_group_id,
        }
        .to_path();
        for _ in 0..10 {
            let (rack, revision) = mutation::read(&self.kv, &rack_path)
                .await?
                .ok_or_else(|| Error::Server("disk removal rack is absent".into()))?;
            let mut retained = self
                .disk_storage(key)
                .await?
                .ok_or_else(|| Error::Server("disk removal requires retained backing identity".into()))?;
            if retained.removed {
                return self.removal_replay(key, expected_revision, &retained).await;
            }
            let readiness = self.removal_conditions(key).await?;
            if readiness.disk_revision != expected_revision {
                return Err(Error::Server(
                    "disk removal revision changed; refresh before retrying".into(),
                ));
            }
            if !readiness.conditions.is_empty() {
                return Err(Error::Server(readiness.conditions.join("; ")));
            }
            let mut group = self
                .get_disk_group(key.rack_id, key.node_id, key.disk_group_id)
                .await?
                .ok_or_else(|| Error::Server("disk removal group is absent".into()))?
                .value;
            if !group.disk_ids.contains(&key.disk_id) {
                return Err(Error::Server("disk removal membership is inconsistent".into()));
            }
            group.disk_ids.retain(|id| *id != key.disk_id);
            retained.removed = true;
            let outcome = DiskRemovalRecord::new(retained.clone(), expected_revision);
            let operations = [
                BatchOp::Put {
                    key: rack_path.as_bytes().to_vec().into(),
                    value: rack,
                },
                mutation::put(&group_path, &group)?,
                mutation::put(&retained.key(), &retained)?,
                mutation::put(&DiskRemovalRecord::key_for(key), &outcome)?,
                BatchOp::Delete {
                    key: key.to_path().into_bytes().into(),
                },
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

    async fn removal_replay(
        &self,
        key: &DiskKey,
        expected: u64,
        retained: &crowdb_protocol::disk_storage::DiskStorageRecord,
    ) -> Result<DiskRemovalRecord> {
        let receipt: DiskRemovalRecord = get_json(&self.kv, &DiskRemovalRecord::key_for(key))
            .await?
            .ok_or_else(|| Error::Server("removed disk has no reconciliation receipt".into()))?;
        receipt.validate().map_err(|error| Error::Server(error.into()))?;
        if receipt.offline_revision != expected
            || receipt.disk != *retained
            || self.get_disk_with_revision(key).await?.is_some()
        {
            return Err(Error::Server(
                "removal outcome differs from the requested disk revision".into(),
            ));
        }
        Ok(receipt)
    }
}
