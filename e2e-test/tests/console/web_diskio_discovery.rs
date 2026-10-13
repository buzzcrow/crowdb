// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::web_legacy::{bootstrap, get};
use crate::common::management::request;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use reqwest::Method;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

async fn owned_groups(registry: &ServiceRegistryClient) -> Option<Vec<u64>> {
    let instances = registry.read_all_diskio_instances().await.unwrap();
    if instances.is_empty() {
        return None;
    }
    assert_eq!(instances.len(), 1);
    Some(
        instances[0]
            .1
            .extra
            .as_ref()
            .unwrap()
            .diskdb
            .as_ref()
            .unwrap()
            .owned_dg_ids
            .clone(),
    )
}

#[tokio::test]
#[ignore = "requires normal KV and DiskIO deployment in an owned release Web node"]
async fn diskio_discovers_disk_groups_created_after_web_deployment() {
    let address = crate::common::management::addresses(1).remove(0);
    let http = crate::common::management::client();
    let base = format!("http://{address}:9090");
    assert_eq!(get(&http, &base, "/api/group0-readiness").await["ready"], false);
    let (http, base) = bootstrap().await;
    assert_eq!(get(&http, &base, "/api/group0-readiness").await["ready"], true);
    request(
        &http,
        &base,
        Method::POST,
        "/api/stores/0/groups",
        json!({"group_id":1,"replica_id":10,"nodes":[1]}),
        201,
    )
    .await;
    let mut body = get(&http, &base, "/api/deployment-defaults?node_id=1").await["diskio"].clone();
    body["rpc_port"] = json!(7200);
    body["kind"] = json!("diskio");
    request(
        &http,
        &base,
        Method::POST,
        "/api/nodes/1/services/deploy",
        body,
        201,
    )
    .await;
    let kv = CrowdbKvClient::new(ClientConfig::new(vec![format!("http://{address}:7000")]));
    kv.seed_leader(0, 0, format!("{address}:7001"));
    let registry = ServiceRegistryClient::from_shared(Arc::new(kv));
    let initial = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(owned) = owned_groups(&registry).await {
                break owned;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("node-local DiskIO registers after deployment");
    assert!(initial.is_empty(), "{initial:?}");
    for group in [11, 12] {
        request(
            &http,
            &base,
            Method::POST,
            "/api/nodes/1/disk-groups",
            json!({"id":group,"store_id":0,"group_id":1}),
            201,
        )
        .await;
        request(
            &http,
            &base,
            Method::POST,
            &format!("/api/nodes/1/disk-groups/{group}/disks"),
            json!({"disk_id":format!("{group:032x}"),"disk_type":"Hdd","capacity_bytes":1_073_741_824_u64,
                "zone_size_bytes":1_073_741_824_u64,"unit_size_bytes":1_048_576,"device_path":""}),
            201,
        )
        .await;
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if owned_groups(&registry).await == Some(vec![11, 12]) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("node-local DiskIO discovers both late groups");
    assert_eq!(
        get(&http, &base, "/api/chunk-storage-readiness").await["ready"],
        true
    );
    assert_eq!(
        get(&http, &base, "/api/nodes/1/disk-groups")
            .await
            .as_array()
            .unwrap()
            .len(),
        2
    );
}
