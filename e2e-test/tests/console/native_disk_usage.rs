// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A restarted owner reports unknown occupancy until retained validation loads it.
use super::native_context::TestNativeContext;
use crowdb_paxos_kv_client::{GetOutcome, HardwareClient, ReadMode};
use crowdb_protocol::{
    key::{DiskKey, OwnerMapKey, TextKey},
    DiskIdExt,
};
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

pub(super) async fn restart_offline_owner(context: &TestNativeContext, key: &DiskKey) {
    let hardware = HardwareClient::from_shared(context.kv.clone());
    let owner = hardware
        .get_owner(key.rack_id, key.node_id, key.disk_group_id)
        .await
        .unwrap()
        .unwrap();
    let generation = owner_revision(context, key).await;
    let servers = context.api(Method::GET, "/api/servers", Value::Null, 200).await;
    let id = format!("diskdb-{}", owner.instance_id);
    let server = servers
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == id)
        .unwrap();
    let node = server["node_id"].as_u64().unwrap();
    let restarted = context
        .api(
            Method::POST,
            &format!("/api/nodes/{node}/diskdb/restart"),
            json!({}),
            200,
        )
        .await;
    assert_ne!(restarted["pid"], server["pid"]);
    let wanted = key.disk_id.to_display_string().replace('-', "");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response = context.http.get(format!("{}/api/diskdb/usage?dg={}", context.base, key.disk_group_id)).send().await.unwrap();
            if response.status().is_success() {
                let usage: Value = response.json().await.unwrap();
                for group in usage["disk_groups"].as_array().unwrap() {
                    if let Some(disk) = group["disks"].as_array().unwrap().iter().find(|disk| disk["disk_id"].as_str().unwrap().replace('-', "") == wanted) {
                        assert_eq!(disk["usage_known"], false, "unloaded retained usage: {disk}");
                        assert_eq!(group["usage_known"], false);
                        assert!(owner_revision(context, key).await > generation);
                        assert_eq!(hardware.get_owner(key.rack_id, key.node_id, key.disk_group_id).await.unwrap().unwrap().instance_id, owner.instance_id);
                        println!("retained Offline owner restart: {}", json!({"disk_id":wanted,"usage_known":false,"same_instance_id":owner.instance_id,"owner_generation_advanced":true}));
                        return;
                    }
                }
            } else {
                eprintln!("restarted owner usage pending: {} {}", response.status(), response.text().await.unwrap());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await.expect("restarted same owner reports retained Offline occupancy as unknown");
}

async fn owner_revision(context: &TestNativeContext, key: &DiskKey) -> u64 {
    let path = OwnerMapKey {
        rack_id: key.rack_id,
        node_id: key.node_id,
        disk_group_id: key.disk_group_id,
    }
    .to_path();
    let GetOutcome::Found { revision, .. } = context
        .kv
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("owner disappeared");
    };
    revision
}
