// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional disk status writes preserve concurrent lifecycle changes.

use crowdb_protocol::{
    common::{DiskId, HwStatus},
    diskdb::rpc::DiskValue,
    key::{DiskKey, TextKey},
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
