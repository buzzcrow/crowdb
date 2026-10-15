// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Startup loading ownership and publication tests.

use std::sync::Arc;

use crowdb_diskdb::model::disk_group::DdbDiskGroup;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;

#[test]
fn recovered_group_replaces_only_the_retained_owner_generation() {
    let container = DdbDiskGroupContainer::new(7);
    let initial = Arc::new(DdbDiskGroup::new(100, 10, 1));
    initial.set_bind((1, 1));
    container.replace_disk_group(&initial);

    let newer = Arc::new(DdbDiskGroup::new(100, 10, 1));
    newer.set_bind((1, 2));
    container.replace_disk_group(&newer);

    let stale_load = Arc::new(DdbDiskGroup::new(100, 10, 1));
    assert!(!container.replace_disk_group_if_current(&initial, (1, 1), &stale_load));
    let retained = container.get_disk_group(100).expect("retained group");
    assert!(Arc::ptr_eq(&retained, &newer));

    let current_load = Arc::new(DdbDiskGroup::new(100, 10, 1));
    current_load.set_bind((1, 2));
    assert!(container.replace_disk_group_if_current(&newer, (1, 2), &current_load));
    let retained = container.get_disk_group(100).expect("loaded group");
    assert!(Arc::ptr_eq(&retained, &current_load));
}

#[tokio::test]
async fn startup_reconstruction_keeps_the_original_administrative_write_gate() {
    use crowdb_diskdb::{ddb_kv_client::DdbKvClient, model::disk::DdbDisk, recovery::ZoneLoader};
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
    use crowdb_protocol::{
        common::{DiskId, HwStatus},
        diskdb::rpc::DiskValue,
    };
    let id = DiskId { high: 4, low: 5 };
    let value = DiskValue {
        status: HwStatus::Up as i32,
        zone_count: 1,
        capacity_units: 128,
        zone_size_units: 128,
        unit_size_bytes: 4096,
        ..Default::default()
    };
    let group = DdbDiskGroup::new(3, 2, 1);
    let disk = Arc::new(DdbDisk::new(id, 3, 2, 1, value.clone()));
    group.add_disk(disk.clone());
    let scoped = DdbKvClient::new(CrowdbKvClient::new(ClientConfig::new(vec![]))).for_group(&group);
    let loader = ZoneLoader::new(Arc::new(scoped), 1);
    // The loader captured an Up value before exclusion, but it must neither
    // manufacture an empty baseline nor publish a new writable incarnation.
    disk.exclude();
    let reconstructed = loader
        .load_disk_group(3, 2, 1, (0, 1), &[(id, value)], 4)
        .await
        .unwrap();
    let loaded_disks = reconstructed.disk_snapshot();
    let restored = &loaded_disks[0];
    assert!(Arc::ptr_eq(&restored.write_gate, &disk.write_gate));
    assert_eq!(restored.effective_status(), HwStatus::Offline);
    assert!(!restored.allocatable());
    assert!(restored.zones.load().is_empty());
}
