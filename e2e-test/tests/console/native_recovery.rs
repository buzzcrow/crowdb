// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{native_context::TestNativeContext, native_s3::s3_request};
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

const KINDS: [&str; 6] = [
    "paxos-kv",
    "diskdb",
    "chunkdb",
    "diskio",
    "chunk-kv",
    "access-server",
];

async fn pid(node: usize, service: &str, expected: Option<u64>) -> u64 {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status =
                crate::common::control::request(json!({"action":"observe-native-services","node":node}))
                    .await;
            if let Some(pid) = status["services"][service]["pid"].as_u64().filter(|pid| *pid > 1) {
                if expected.map_or(true, |expected| expected == pid) {
                    return pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("Node {node} service {service} PID observation: {error}"))
}

async fn alive(node: usize, pid: u64) -> bool {
    crate::common::control::request(json!({"action":"observe-native-process","node":node,"pid":pid})).await
        ["alive"]
        .as_bool()
        .unwrap()
}

fn monitor_id(kind: &str, id: &str) -> String {
    if kind == "paxos-kv" {
        "kv".into()
    } else {
        id.into()
    }
}

async fn inventory(context: &TestNativeContext) -> Value {
    let servers = context.api(Method::GET, "/api/servers", Value::Null, 200).await;
    assert_eq!(servers.as_array().unwrap().len(), 18);
    for node in 1..=3 {
        for kind in KINDS {
            let entries: Vec<_> = servers
                .as_array()
                .unwrap()
                .iter()
                .filter(|entry| entry["node_id"] == node && entry["service_type"] == kind)
                .collect();
            assert_eq!(entries.len(), 1, "Node {node} {kind}");
            let observed = pid(
                node - 1,
                &monitor_id(kind, entries[0]["id"].as_str().unwrap()),
                None,
            )
            .await;
            assert!(alive(node - 1, observed).await);
        }
    }
    servers
}

async fn restart(context: &TestNativeContext, kind: &str, node: u64) {
    let index = usize::try_from(node.checked_sub(1).unwrap()).unwrap();
    let before = context.api(Method::GET, "/api/servers", Value::Null, 200).await;
    let entry = before
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["node_id"] == node && entry["service_type"] == kind)
        .unwrap();
    let id = entry["id"].as_str().unwrap();
    let service = monitor_id(kind, id);
    let old = pid(index, &service, None).await;
    let path = match kind {
        "paxos-kv" => format!("/api/nodes/{node}/server/restart"),
        "diskdb" => format!("/api/nodes/{node}/diskdb/restart"),
        _ => format!("/api/services/{id}/restart"),
    };
    let reply = context.api(Method::POST, &path, json!({}), 200).await;
    let new = reply["pid"].as_u64().unwrap();
    assert_ne!(old, new);
    assert_eq!(pid(index, &service, Some(new)).await, new);
    assert!(!alive(index, old).await);
    assert!(alive(index, new).await);
    let after = context.api(Method::GET, "/api/servers", Value::Null, 200).await;
    assert_eq!(after.as_array().unwrap().len(), 18);
    assert_eq!(
        after
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["id"] == id)
            .count(),
        1
    );
    assert_eq!(
        s3_request(
            context,
            "GET",
            "/api/access/s3/native-locations/multipart.bin",
            vec![]
        )
        .await,
        b"replacement",
        "durable object survives {kind} restart"
    );
}

#[tokio::test]
#[ignore = "requires three formal nodes and all six typed services"]
async fn all_typed_restarts_preserve_objects_and_keep_one_live_process() {
    let context = TestNativeContext::connect().await;
    super::native_storage::start(&context).await;
    inventory(&context).await;
    let namespaces = context
        .api(Method::GET, "/api/access/iceberg/v1/namespaces", Value::Null, 200)
        .await;
    assert!(namespaces["namespaces"].is_array());
    assert!(
        String::from_utf8(s3_request(&context, "GET", "/api/access/s3/", vec![]).await)
            .unwrap()
            .contains("ListAllMyBucketsResult")
    );
    s3_request(&context, "PUT", "/api/access/s3/native-locations", vec![]).await;
    s3_request(
        &context,
        "PUT",
        "/api/access/s3/native-locations/multipart.bin",
        b"replacement".to_vec(),
    )
    .await;
    assert_eq!(
        s3_request(
            &context,
            "GET",
            "/api/access/s3/native-locations/multipart.bin",
            vec![]
        )
        .await,
        b"replacement"
    );
    let group = context
        .api(Method::GET, "/api/stores/0/groups/0", Value::Null, 200)
        .await;
    let leader = group["replicas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|replica| replica["role"] == "leader")
        .expect("live authority before restart")["node_id"]
        .as_u64()
        .unwrap();
    if leader != 1 {
        restart(&context, "paxos-kv", leader).await;
    }
    for kind in KINDS {
        restart(&context, kind, 1).await;
    }
    inventory(&context).await;
    super::native_s3::assert_native_locations(&context).await;
}
