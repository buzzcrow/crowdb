// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{Error, Result};
use crowdb_paxos_kv_client::PaxosKvClient;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

/// Confirm that loaded `DiskDB` group counts cover the registered online disks.
/// # Errors
/// Rejects unavailable registry/hardware reads and incomplete storage publication.
pub async fn diskdb_storage_ready(kv: Arc<PaxosKvClient>) -> Result<()> {
    let registry = crowdb_paxos_kv_client::ServiceRegistryClient::from_shared(kv.clone());
    let hardware = crowdb_paxos_kv_client::HardwareClient::from_shared(kv);
    let (instances, disks) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(registry.read_all_diskdb_instances(), hardware.list_all_disks())
    })
    .await
    .map_err(|_| Error::Config("DiskDB readiness probe timed out".into()))?;
    let instances = instances?;
    let mut expected = BTreeMap::<u64, u32>::new();
    for disk in disks? {
        if matches!(
            crowdb_protocol::common::HwStatus::try_from(disk.value.status),
            Ok(crowdb_protocol::common::HwStatus::Init | crowdb_protocol::common::HwStatus::Offline)
        ) {
            continue;
        }
        *expected.entry(disk.disk_group_id).or_default() += 1;
    }
    for (group_id, disk_count) in expected {
        let owners: Vec<_> = instances
            .iter()
            .filter_map(|(_, value)| {
                let extra = value.extra.as_ref()?.diskdb.as_ref()?;
                (!value.rpc_endpoint.is_empty() && extra.owned_dg_ids.contains(&group_id)).then_some(extra)
            })
            .collect();
        if owners.len() != 1 {
            return Err(Error::Config(format!(
                "disk group {group_id} has no unique published DiskDB route"
            )));
        }
        if !owners[0].group_usages.iter().any(|usage| {
            usage.disk_group_id == group_id && usage.usage_known && usage.disk_count >= disk_count
        }) {
            return Err(Error::Config(format!(
                "disk group {group_id} has not loaded its {disk_count} registered disks"
            )));
        }
    }
    Ok(())
}
