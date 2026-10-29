// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Normal three-node storage prerequisites shared by public Console clients.

use super::native_context::TestNativeContext;
use crowdb_kv_client::ServiceRegistryClient;
use reqwest::Method;
use serde_json::{json, Value};
use std::{collections::BTreeSet, time::Duration};

mod media;

pub(super) async fn start(context: &TestNativeContext) {
    start_with_geometry(context, None).await;
}

pub(super) async fn start_with_geometry(context: &TestNativeContext, geometry: Option<u64>) {
    start_through(context, geometry, "access-server").await;
}

pub(super) async fn start_prerequisites(context: &TestNativeContext) {
    start_through(context, None, "chunkdb").await;
}

pub(super) async fn start_chunk_kv(context: &TestNativeContext) {
    start_through(context, None, "chunk-kv").await;
}

async fn start_through(context: &TestNativeContext, geometry: Option<u64>, last: &str) {
    start_diskdb(context, [128 * 1024; 3]).await;
    for (kind, http, rpc) in [
        ("diskio", None, Some(7200)),
        ("chunkdb", Some(7300), Some(7301)),
        ("chunk-kv", Some(7400), Some(7401)),
        ("access-server", Some(9000), None),
    ] {
        if kind == "chunkdb" {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if context
                        .api(Method::GET, "/api/chunk-storage-readiness", Value::Null, 200)
                        .await["ready"]
                        == true
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("live DiskIO ownership before ChunkDB");
        }
        for node in 1..=3 {
            deploy(context, node, kind, http, rpc, geometry).await;
            match kind {
                "chunkdb" => ready(context, node, 7300, "/ready").await,
                "chunk-kv" => ready(context, node, 7400, "/ready").await,
                "access-server" => ready(context, node, 9002, "/_crowdb/health/ready").await,
                _ => {}
            }
        }
        if kind == last {
            break;
        }
    }
}

pub(super) async fn start_diskdb(context: &TestNativeContext, units: [u64; 3]) {
    groups(context).await;
    // Initial ownership claims and disk admission share the Rack CAS guard.
    wait_diskdb(context).await;
    disks(context, units).await;
    wait_diskdb(context).await;
}

async fn groups(context: &TestNativeContext) {
    verify_manual_metadata(context).await;
    context
        .api(
            Method::POST,
            "/api/stores",
            json!({"store_id":1,"nodes":[1,2,3]}),
            201,
        )
        .await;
    for (store, group, replica) in [(1, 1, 10), (0, 2, 20)] {
        context
            .api(
                Method::POST,
                &format!("/api/stores/{store}/groups"),
                json!({"group_id":group,"replica_id":replica,"nodes":[1,2,3]}),
                201,
            )
            .await;
    }
    disk_group(context, 1).await;
    for node in 1..=3 {
        context
            .api(
                Method::POST,
                &format!("/api/nodes/{node}/diskdb/deploy"),
                json!({"rpc_port":7100,"keepalive_interval_secs":1}),
                201,
            )
            .await;
    }
    for node in 2..=3 {
        disk_group(context, node).await;
    }
}

async fn verify_manual_metadata(context: &TestNativeContext) {
    context
        .api(Method::GET, "/api/stores/0/groups/1", Value::Null, 404)
        .await;
    assert!(matches!(
        context
            .kv
            .get(
                0,
                0,
                crowdb_console_shared::deployment::preparation::DEFAULT_PREPARATION_KEY,
                crowdb_kv_client::ReadMode::Linearizable,
                None
            )
            .await
            .unwrap(),
        crowdb_kv_client::GetOutcome::NotFound,
    ));
}

async fn disk_group(context: &TestNativeContext, node: u64) {
    context
        .api(
            Method::POST,
            &format!("/api/nodes/{node}/disk-groups"),
            json!({"id":node,"store_id":u64::from(node == 1),
                "group_id":if node == 1 { 1 } else { 2 },"name":"storage"}),
            201,
        )
        .await;
}

pub(super) async fn disks(context: &TestNativeContext, units: [u64; 3]) {
    let profile = crate::common::control::request(json!({"action":"native-storage-profile"})).await;
    media::admit(context, units, &profile).await;
}

async fn wait_diskdb(context: &TestNativeContext) {
    for node in 1..=3 {
        ready(context, node, 7101, "/ready").await;
    }
    let registry = ServiceRegistryClient::from_shared(context.kv.clone());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let instances = registry.read_all_diskdb_instances().await.unwrap();
            let groups: BTreeSet<_> = instances
                .iter()
                .filter(|(_, value)| {
                    context
                        .addresses
                        .iter()
                        .any(|address| value.rpc_endpoint == format!("{address}:7102"))
                })
                .flat_map(|(_, value)| {
                    value
                        .extra
                        .as_ref()
                        .and_then(|extra| extra.diskdb.as_ref())
                        .map(|extra| extra.owned_dg_ids.clone())
                        .unwrap_or_default()
                })
                .collect();
            if groups == BTreeSet::from([1, 2, 3])
                && crowdb_console_shared::deployment::services::diskdb_storage_ready(context.kv.clone())
                    .await
                    .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("actual loaded DiskDB groups are published before consumers");
}

async fn deploy(
    context: &TestNativeContext,
    node: usize,
    kind: &str,
    http: Option<u16>,
    rpc: Option<u16>,
    geometry: Option<u64>,
) {
    let mut body = context
        .api(
            Method::GET,
            &format!("/api/deployment-defaults?node_id={node}"),
            Value::Null,
            200,
        )
        .await[kind]
        .clone();
    body["kind"] = json!(kind);
    body["instance_id"] = json!(node.to_string());
    body["local_deployment"] = json!(false);
    if let Some(port) = http {
        body["http_port"] = json!(port);
    }
    if let Some(port) = rpc {
        body["rpc_port"] = json!(port);
    }
    match kind {
        "diskio" => body["disk_group_id"] = json!(node),
        "chunk-kv" => {
            body["metadata_store_id"] = json!(0);
            body["bootstrap_group_id"] = json!(2);
            if let Some(bytes) = geometry {
                body["stream_chunk_capacity_bytes"] = json!(bytes);
            }
        }
        "access-server" => {
            body["s3_port"] = json!(9001);
            body["health_port"] = json!(9002);
        }
        _ => {}
    }
    context
        .api(
            Method::POST,
            &format!("/api/nodes/{node}/services/deploy"),
            body,
            201,
        )
        .await;
}

async fn ready(context: &TestNativeContext, node: usize, port: u16, path: &str) {
    let endpoint = format!("http://{}:{port}{path}", context.addresses[node - 1]);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if context
                .http
                .get(&endpoint)
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("usable producer at {endpoint}: {error}"));
}
