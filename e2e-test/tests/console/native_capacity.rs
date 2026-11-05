// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::native_context::TestNativeContext;
use crowdb_paxos_kv_client::HardwareClient;
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::DiskValue,
    mgmt::node::storage::DiskBackend,
};
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

pub(super) async fn prepare(context: &TestNativeContext) {
    super::native_storage::start_diskdb(context, [128 * 1024; 3]).await;
    let profile = crate::common::control::request(json!({"action":"native-storage-profile"})).await;
    let capacity = profile["spare_disk_gib"].as_u64().unwrap() * 1024u64.pow(3);
    assert_eq!(capacity, 4 * 1024u64.pow(4));
    let loaded_by = tokio::time::Instant::now() + Duration::from_secs(3);
    context
        .api(
            Method::POST,
            "/api/nodes/1/disk-groups",
            json!({"id":581,"name":"second storage","store_id":1,"group_id":1}),
            201,
        )
        .await;
    let backing = &profile["spare_backing"];
    assert_eq!(backing["path"], "/opt/crowdb/data/disks/capacity-spare.img");
    assert_eq!(backing["capacity_bytes"], capacity);
    let hardware = HardwareClient::from_shared(context.kv.clone());
    let rack_id = hardware
        .list_nodes()
        .await
        .unwrap()
        .into_iter()
        .find(|(_, node, _)| *node == 1)
        .unwrap()
        .0;
    tokio::time::timeout_at(loaded_by, async {
        loop {
            let group = hardware.get_disk_group(rack_id, 1, 581).await.unwrap().unwrap();
            if group.value.status == HwStatus::Up as i32 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the actual DiskDB initializes the new group before its media is admitted");
    // This is a fresh, exclusively owned truncated fixture, matching the normal
    // native fixture's trusted legacy bootstrap rather than a public bypass.
    let record = DiskStorageRecord {
        version: 1,
        physical_backing: None,
        operation_id: "native-capacity-spare".into(),
        rack_id,
        node_id: 1,
        disk_group_id: 581,
        disk_id: DiskId { high: 0, low: 0x581 },
        backend: DiskBackend::File,
        backing_identity: backing["backing_identity"].as_str().unwrap().into(),
        media_type: Some(0),
        sparse: true,
        removed: false,
        geometry: DiskValue {
            disk_type: 0,
            capacity_units: capacity / (128 * 1024),
            zone_size_units: 32 * 1024u64.pow(3) / (128 * 1024),
            unit_size_bytes: 128 * 1024,
            zone_count: 128,
            status: HwStatus::Init as i32,
            device_path: backing["path"].as_str().unwrap().into(),
        },
    };
    record.validate().unwrap();
    hardware.admit_disk_storage(&record).await.unwrap();
    hardware.activate_disk_storage(&record).await.unwrap();
    tokio::time::timeout_at(loaded_by, async {
        loop {
            let response = context
                .http
                .get(format!("{}/api/diskdb/usage?dg=581", context.base))
                .send()
                .await
                .unwrap();
            if !response.status().is_success() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let usage: Value = response.json().await.unwrap();
            let loaded = usage["disk_groups"].as_array().unwrap().iter().any(|group| {
                group["disk_group_id"] == 581
                    && group["disks"].as_array().unwrap().iter().any(|disk| {
                        disk["disk_id"].as_str().unwrap().replace('-', "")
                            == "00000000000000000000000000000581"
                    })
            });
            if loaded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("second actual DiskDB group is observed within the original bound");
}
