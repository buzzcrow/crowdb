// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded legacy evidence must rule out an existing same-host resource.

use super::{authority::Hosts, HardwareClient};
use crate::{Error, ReadMode, Result};
use crowdb_protocol::{
    disk_storage::{DiskStorageRecord, PhysicalBacking},
    diskdb::rpc::DiskValue,
    key::{DiskKey, TextKey},
    mgmt::node::storage::DiskBackend,
};

pub(super) async fn verify(hw: &HardwareClient, record: &DiskStorageRecord, hosts: &Hosts) -> Result<()> {
    let mut count = 0;
    for prefix in ["/hw/disk-storage/", "/hw/disk/"] {
        let mut after = Vec::new();
        loop {
            let page = hw
                .kv
                .scan(
                    0,
                    0,
                    prefix.as_bytes(),
                    &after,
                    &[],
                    128,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await?;
            if page.timed_out || page.items.len() > 128 || (page.truncated && page.items.is_empty()) {
                return Err(Error::Server(
                    "physical resource legacy verification is incomplete".into(),
                ));
            }
            for (key, value) in &page.items {
                count += 1;
                if count > 65536
                    || value.len() > 65536
                    || !key.starts_with(prefix.as_bytes())
                    || key.as_ref() <= after.as_slice()
                {
                    return Err(Error::Server(
                        "physical resource legacy verification exceeds bound or does not advance".into(),
                    ));
                }
                let path = std::str::from_utf8(key).map_err(|error| Error::Server(error.to_string()))?;
                if prefix == "/hw/disk-storage/" {
                    retained(path, value, record, hosts)?;
                } else {
                    membership(hw, path, value, record, hosts).await?;
                }
                after = key.to_vec();
            }
            if !page.truncated {
                break;
            }
        }
    }
    Ok(())
}

fn retained(path: &str, bytes: &[u8], record: &DiskStorageRecord, hosts: &Hosts) -> Result<()> {
    let existing: DiskStorageRecord =
        serde_json::from_slice(bytes).map_err(|error| Error::Server(error.to_string()))?;
    existing.validate().map_err(|error| Error::Server(error.into()))?;
    if existing.key() != path {
        return Err(Error::Server(
            "legacy backing placement differs from its key".into(),
        ));
    }
    if existing.key() == record.key() {
        let mut normalized = existing;
        normalized.removed = record.removed;
        normalized.geometry.status = record.geometry.status;
        if normalized != *record {
            return Err(Error::Server(
                "retained resource identity or geometry changed".into(),
            ));
        }
        return Ok(());
    }
    let backing = record
        .physical_backing
        .as_ref()
        .ok_or_else(|| Error::Server("physical backing missing".into()))?;
    if let Some(actual) = &existing.physical_backing {
        if actual.physical_host_id == backing.physical_host_id
            && actual.resource_identity == backing.resource_identity
        {
            return Err(Error::Server(
                "physical resource already belongs to retained storage".into(),
            ));
        }
    } else if host(hosts, existing.node_id)? == backing.physical_host_id {
        if existing.backend != DiskBackend::File {
            return Err(Error::Server(
                "legacy raw backing lineage is unknown; explicit migration is required".into(),
            ));
        }
        let actual = PhysicalBacking {
            physical_host_id: backing.physical_host_id.clone(),
            resource_identity: existing.backing_identity.clone(),
            capacity_bytes: existing
                .physical_capacity_bytes()
                .ok_or_else(|| Error::Server("legacy capacity overflows".into()))?,
        };
        actual.validate().map_err(|error| Error::Server(error.into()))?;
        if actual.resource_identity == backing.resource_identity {
            return Err(Error::Server(
                "physical resource already belongs to legacy retained storage".into(),
            ));
        }
    }
    Ok(())
}

async fn membership(
    hw: &HardwareClient,
    path: &str,
    bytes: &[u8],
    record: &DiskStorageRecord,
    hosts: &Hosts,
) -> Result<()> {
    let key = DiskKey::from_path(path).map_err(|error| Error::Server(error.to_string()))?;
    if key.to_path() != path {
        return Err(Error::Server("legacy disk placement is not canonical".into()));
    }
    let disk: DiskValue = serde_json::from_slice(bytes).map_err(|error| Error::Server(error.to_string()))?;
    if disk.device_path.is_empty() {
        return Ok(());
    }
    let backing = record
        .physical_backing
        .as_ref()
        .ok_or_else(|| Error::Server("physical backing missing".into()))?;
    if host(hosts, key.node_id)? == backing.physical_host_id && hw.disk_storage(&key).await?.is_none() {
        return Err(Error::Server(
            "legacy physical membership has no verified backing receipt; explicit migration is required"
                .into(),
        ));
    }
    Ok(())
}

fn host(hosts: &Hosts, node: u64) -> Result<&str> {
    hosts.get(&node).map(String::as_str).ok_or_else(|| {
        Error::Server("legacy physical node host is unknown; explicit migration is required".into())
    })
}
