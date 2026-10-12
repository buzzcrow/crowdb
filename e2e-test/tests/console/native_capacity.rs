// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::native_context::TestNativeContext;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

pub(super) async fn prepare(context: &TestNativeContext) {
    super::native_storage::start_diskdb(context, [128 * 1024; 3]).await;
    let profile = crate::common::control::request(json!({"action":"native-storage-profile"})).await;
    let capacity = profile["spare_disk_gib"].as_u64().unwrap() * 1024u64.pow(3);
    assert_eq!(capacity, 4 * 1024u64.pow(4));
    context
        .api(
            Method::POST,
            "/api/nodes/1/disk-groups",
            json!({"id":581,"name":"second storage","store_id":1,"group_id":1}),
            201,
        )
        .await;
    context
        .api(
            Method::POST,
            "/api/nodes/1/disk-groups/581/disks",
            json!({"disk_id":"00000000000000000000000000000581","disk_type":"Hdd",
                "capacity_bytes":capacity,"zone_size_bytes":32 * 1024u64.pow(3),
                "unit_size_bytes":128 * 1024,"device_path":"/opt/crowdb/data/capacity-spare.img"}),
            201,
        )
        .await;
    tokio::time::timeout(Duration::from_secs(3), async {
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
