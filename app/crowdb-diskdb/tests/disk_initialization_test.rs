// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use std::{sync::Arc, time::Duration};

use common::metadata::{wait_for_disks_ready, TestMetadata};
use crowdb_diskdb::{
    ddb_config::KeepAliveConfig, liveness::keepalive::KeepAlive,
    model::disk_group_container::DdbDiskGroupContainer,
};
use crowdb_protocol::{
    common::{DiskId, HwStatus, NodeValue, RackValue},
    diskdb::rpc::{DiskGroupValue, DiskValue},
};

#[tokio::test]
async fn provisional_init_waits_for_activation_and_only_then_loads_allocatable_zones() {
    let metadata = TestMetadata::start().await;
    let hw = metadata.make_hardware_client();
    let id = DiskId { high: 4, low: 5 };
    hw.add_rack(
        1,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![2],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_node(
        1,
        2,
        &NodeValue {
            status: HwStatus::Up as i32,
            disk_group_ids: vec![3],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_disk_group(
        1,
        2,
        3,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: vec![id],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_disk(
        1,
        2,
        3,
        &id,
        &DiskValue {
            status: HwStatus::Init as i32,
            disk_type: 0,
            capacity_units: 128,
            zone_size_units: 128,
            unit_size_bytes: 1024 * 1024,
            zone_count: 1,
            device_path: String::new(),
        },
    )
    .await
    .unwrap();
    hw.set_owner(1, 2, 3, 999, u64::MAX).await.unwrap();
    hw.set_bind(1, 2, 3, 0, 1).await.unwrap();
    let container = Arc::new(DdbDiskGroupContainer::new(999));
    let keepalive = KeepAlive::new(
        metadata.make_hardware_client(),
        metadata.make_service_registry_client(),
        Arc::clone(&container),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(metadata.make_ddb_kv_client());
    for _ in 0..2 {
        let outcome = keepalive.tick().await;
        assert_eq!(outcome.disks_added, 0);
        assert!(container.get_disk_group(3).unwrap().disk_snapshot().is_empty());
        assert_eq!(
            hw.get_disk(1, 2, 3, &id).await.unwrap().unwrap().status,
            HwStatus::Init as i32
        );
    }
    hw.set_disk_status(1, 2, 3, &id, HwStatus::Up).await.unwrap();
    assert_eq!(keepalive.tick().await.disks_added, 1);
    wait_for_disks_ready(&container, 3, 1, 1).await;
    let group = container.get_disk_group(3).unwrap();
    assert!(group.disk_snapshot()[0].allocatable());
}
