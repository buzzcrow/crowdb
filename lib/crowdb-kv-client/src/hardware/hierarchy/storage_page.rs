// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded retained backing pages, including disks absent from membership.

use super::{mutation, HardwareClient};
use crate::{Error, ReadMode, Result};
use crowdb_protocol::{
    disk_storage::DiskStorageRecord,
    key::{RackKey, TextKey},
};

impl HardwareClient {
    /// List one physical node's retained backing receipts without loading usage.
    /// The cursor advances over all receipts, including registered disks.
    ///
    /// # Errors
    /// Rejects foreign cursors, unsupported receipts and changing authority.
    pub async fn disk_storage_page(
        &self,
        rack_id: u64,
        node_id: u64,
        after: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<DiskStorageRecord>, Option<String>)> {
        let prefix = DiskStorageRecord::node_prefix(rack_id, node_id);
        let after = after.unwrap_or_default();
        if !(1..=64).contains(&limit)
            || (!after.is_empty() && (!after.starts_with(&prefix) || after.len() > 256))
        {
            return Err(Error::Server(
                "invalid retained disk page limit or node cursor".into(),
            ));
        }
        let rack_path = RackKey { rack_id }.to_path();
        let rack = mutation::read(&self.kv, &rack_path)
            .await?
            .ok_or_else(|| Error::Server("retained disk rack is absent".into()))?;
        let page = self
            .kv
            .scan(
                0,
                0,
                prefix.as_bytes(),
                after.as_bytes(),
                &[],
                limit,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        let next = if page.truncated {
            let key = page
                .items
                .last()
                .ok_or_else(|| Error::Server("retained disk page did not advance".into()))?;
            let key = String::from_utf8(key.0.to_vec()).map_err(|error| Error::Server(error.to_string()))?;
            if key.as_str() <= after {
                return Err(Error::Server("retained disk cursor did not advance".into()));
            }
            Some(key)
        } else {
            None
        };
        let mut records = Vec::with_capacity(page.items.len());
        for (key, bytes) in page.items {
            let path = String::from_utf8(key.to_vec()).map_err(|error| Error::Server(error.to_string()))?;
            let record: DiskStorageRecord = mutation::decode(&path, &bytes)?;
            record.validate().map_err(|error| Error::Server(error.into()))?;
            if record.key() != path || record.rack_id != rack_id || record.node_id != node_id {
                return Err(Error::Server(
                    "retained disk receipt differs from node placement".into(),
                ));
            }
            records.push(record);
        }
        if mutation::read(&self.kv, &rack_path).await?.as_ref() != Some(&rack) {
            return Err(Error::OutcomeUnknown);
        }
        Ok((records, next))
    }
}
