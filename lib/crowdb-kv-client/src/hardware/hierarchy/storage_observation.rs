// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Complete retained-medium observations for the node's `DiskIO` reconciler.

use crowdb_protocol::{
    common::DiskId,
    disk_storage::DiskStorageRecord,
    diskdb::rpc::DiskValue,
    key::{DiskKey, RackKey, TextKey},
};
use serde::Serialize;

use super::{mutation, HardwareClient};
use crate::{Error, Result};

pub(super) async fn bounded_inventory(
    kv: &crate::CrowdbKvClient,
    prefix: &str,
) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
    let mut rows = Vec::new();
    let mut after = Vec::new();
    loop {
        let page = kv
            .scan(
                0,
                0,
                prefix.as_bytes(),
                &after,
                &[],
                256,
                crate::ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        let last = page.items.last().map(|(key, _)| key.to_vec());
        rows.extend(page.items);
        if rows.len() > 4096 {
            return Err(Error::Server("disk inventory exceeds admission bound".into()));
        }
        if !page.truncated {
            return Ok(rows);
        }
        let next = last
            .filter(|key| *key > after)
            .ok_or_else(|| Error::Server("disk inventory cursor did not advance".into()))?;
        after = next;
    }
}

#[derive(Debug, Serialize)]
pub struct DiskIoObservation {
    pub disk_group_id: u64,
    #[serde(serialize_with = "identity")]
    pub disk_id: DiskId,
    #[serde(serialize_with = "revision")]
    pub revision: u64,
    pub value: DiskValue,
    pub storage: Option<DiskStorageRecord>,
}

fn identity<S: serde::Serializer>(id: &DiskId, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    serde_json::json!({"high": id.high.to_string(), "low": id.low.to_string()}).serialize(serializer)
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with requires a field reference"
)]
fn revision<S: serde::Serializer>(value: &u64, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

impl HardwareClient {
    /// Observe membership, exact revisions and retained backing together.
    ///
    /// # Errors
    /// Rejects incomplete, changing, oversized or inconsistent observations;
    /// callers must retain previous state instead of interpreting failure as empty.
    pub async fn disk_io_inventory(
        &self,
        rack_id: u64,
        node_id: u64,
        group: Option<u64>,
    ) -> Result<Vec<DiskIoObservation>> {
        let rack_path = RackKey { rack_id }.to_path();
        let rack = mutation::read(&self.kv, &rack_path)
            .await?
            .ok_or_else(|| Error::Server("DiskIO observation rack missing".into()))?;
        let groups = self.list_disk_groups_on_node(rack_id, node_id).await?;
        let mut entries = Vec::new();
        for entry in groups
            .into_iter()
            .filter(|entry| group.map_or(true, |id| entry.dg_id == id))
        {
            for (disk_id, _) in self.list_disks_in_group(rack_id, node_id, entry.dg_id).await? {
                if entries.len() >= 4096 {
                    return Err(Error::Server("DiskIO observation exceeds disk bound".into()));
                }
                let key = DiskKey {
                    rack_id,
                    node_id,
                    disk_group_id: entry.dg_id,
                    disk_id,
                };
                let (value, revision) = self
                    .get_disk_with_revision(&key)
                    .await?
                    .ok_or_else(|| Error::Server("DiskIO observation membership changed".into()))?;
                let storage = self.disk_storage(&key).await?;
                if let Some(record) = &storage {
                    let mut geometry = value.clone();
                    geometry.status = record.geometry.status;
                    if record.removed || geometry != record.geometry {
                        return Err(Error::Server(
                            "DiskIO observation retained geometry conflicts".into(),
                        ));
                    }
                }
                entries.push(DiskIoObservation {
                    disk_group_id: entry.dg_id,
                    disk_id,
                    revision,
                    value,
                    storage,
                });
            }
        }
        if mutation::read(&self.kv, &rack_path).await?.as_ref() != Some(&rack) {
            return Err(Error::Server("DiskIO observation hierarchy changed".into()));
        }
        Ok(entries)
    }
}
