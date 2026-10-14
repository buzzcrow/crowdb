// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Legacy deletion cannot bypass retained-media lifecycle records.

use crowdb_protocol::key::{RackKey, TextKey};

use super::{mutation, HardwareClient};
use crate::{BatchOp, Error, ReadMode, Result};

impl HardwareClient {
    /// Check whether a scope contains retained backing records, including Removed.
    ///
    /// # Errors
    /// Propagates authority errors. A group requires its containing node.
    pub async fn has_retained_disk_storage(
        &self,
        rack_id: u64,
        node_id: Option<u64>,
        group_id: Option<u64>,
    ) -> Result<bool> {
        let prefix = storage_prefix(rack_id, node_id, group_id)?;
        self.storage_prefix_exists(&prefix).await
    }

    pub(super) async fn storage_prefix_exists(&self, prefix: &str) -> Result<bool> {
        let rows = self
            .kv
            .scan(
                0,
                0,
                prefix.as_bytes(),
                &[],
                &[],
                1,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        Ok(!rows.items.is_empty() || rows.truncated)
    }

    pub(super) async fn legacy_delete(
        &self,
        rack_id: u64,
        target: &str,
        retained_prefix: &str,
    ) -> Result<()> {
        let rack_path = RackKey { rack_id }.to_path();
        let rack = mutation::read(&self.kv, &rack_path).await?;
        if self.storage_prefix_exists(retained_prefix).await? {
            return Err(Error::Server(
                "retained disks require the Offline/Remove lifecycle; parent deletion is blocked".into(),
            ));
        }
        if mutation::read(&self.kv, target).await?.is_none() {
            return Ok(());
        }
        let (rack_bytes, revision) =
            rack.ok_or_else(|| Error::Server("hardware rack does not exist".into()))?;
        let mut operations = vec![BatchOp::Delete {
            key: target.as_bytes().to_vec().into(),
        }];
        if target != rack_path {
            operations.push(BatchOp::Put {
                key: rack_path.as_bytes().to_vec().into(),
                value: rack_bytes,
            });
        }
        self.kv
            .batch_write_cas(0, 0, &operations, rack_path.as_bytes(), revision)
            .await?;
        Ok(())
    }
}

fn storage_prefix(rack: u64, node: Option<u64>, group: Option<u64>) -> Result<String> {
    match (node, group) {
        (None, None) => Ok(format!("/hw/disk-storage/{rack}/")),
        (Some(node), None) => Ok(format!("/hw/disk-storage/{rack}/{node}/")),
        (Some(node), Some(group)) => Ok(format!("/hw/disk-storage/{rack}/{node}/{group}/")),
        (None, Some(_)) => Err(Error::Server("disk storage group scope requires a node".into())),
    }
}
