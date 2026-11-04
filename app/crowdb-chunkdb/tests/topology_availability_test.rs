// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/topology_availability.rs"]
mod availability;
mod common;

use crowdb_chunkdb::{
    selector::{MirrorPlacement, PlacementConstraints},
    topology::build_snapshot,
};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::{
    common::{HwStatus, InstanceValue},
    key::InstanceKey,
};

#[tokio::test]
async fn expired_physical_io_is_excluded_without_changing_hardware_and_returns_after_heartbeat() {
    let cluster = common::cluster::KvCluster::start_pinned().await;
    let hardware = cluster.make_hardware_client();
    common::cluster::seed_hardware(&hardware).await;
    let registry = ServiceRegistryClient::from_shared(cluster.make_crowdb_client());
    let groups = hardware.list_disk_groups().await.unwrap();
    assert!(groups.len() >= 3);
    for group in &groups {
        availability::heartbeat(&registry, group).await;
    }
    let before = build_snapshot(&hardware).await.unwrap();
    assert_eq!(before.healthy_disk_groups().len(), groups.len());
    let expired = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + crowdb_kv_client::SERVICE_INSTANCE_TTL_MS
        + 1;
    assert!(before.healthy_disk_groups_at_for_tests(expired).is_empty());
    let stopped = &groups[0];
    let key = InstanceKey {
        service: "diskio".into(),
        instance_id: stopped.dg_id,
    }
    .to_path();
    let kv = cluster.make_crowdb_client();
    let value = kv
        .get(
            0,
            0,
            key.as_bytes(),
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap();
    let crowdb_kv_client::GetOutcome::Found { value, .. } = value else {
        panic!("missing IO registration")
    };
    let mut observation: InstanceValue = serde_json::from_slice(&value).unwrap();
    observation.last_heartbeat_ms = 0;
    kv.put(
        0,
        0,
        key.as_bytes(),
        &serde_json::to_vec(&observation).unwrap(),
        None,
    )
    .await
    .unwrap();
    let unavailable = build_snapshot(&hardware).await.unwrap();
    assert!(unavailable
        .healthy_disk_groups()
        .iter()
        .all(|group| group.dg_id != stopped.dg_id));
    let plan = MirrorPlacement::select(&unavailable, 2, &PlacementConstraints::default()).unwrap();
    assert!(plan
        .entries
        .iter()
        .all(|entry| entry.disk_group_id != stopped.dg_id));
    assert_eq!(before.healthy_disk_groups().len(), groups.len());
    assert!(hardware
        .list_disk_groups()
        .await
        .unwrap()
        .iter()
        .all(|group| group.value.status == HwStatus::Up as i32));
    availability::heartbeat(&registry, stopped).await;
    assert_eq!(
        build_snapshot(&hardware)
            .await
            .unwrap()
            .healthy_disk_groups()
            .len(),
        groups.len()
    );
}
