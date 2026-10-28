// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

async fn request(http: &Client, base: &str, method: Method, path: &str, body: Value, status: u16) -> Value {
    let response = http
        .request(method, format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let actual = response.status().as_u16();
    let text = response.text().await.unwrap();
    assert_eq!(actual, status, "{path}: {text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test]
#[ignore = "requires owned release Web and packaged KV child"]
async fn deploy_restart_and_stop_retains_fixed_ports_and_workspace() {
    let http = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let addresses = crate::common::management::addresses(1);
    let base = format!("http://{}:9090", addresses[0]);
    request(&http, &base, Method::POST, "/api/racks", json!({"id":1}), 201).await;
    request(
        &http,
        &base,
        Method::POST,
        "/api/nodes",
        json!({"id":1,"rack_id":1,"host":"127.0.0.1","ssh_user":""}),
        201,
    )
    .await;
    let input = json!({"rest_port":7000,"rpc_port":7001});
    let deployed = request(
        &http,
        &base,
        Method::POST,
        "/api/nodes/1/server/deploy",
        input.clone(),
        201,
    )
    .await;
    assert_eq!(deployed["node_id"], 1);
    let first = deployed["pid"].as_u64().unwrap();
    assert!(first > 1);
    let root = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mounts: Value = serde_json::from_slice(&std::fs::read(root.join("mounts.json")).unwrap()).unwrap();
    let workspace =
        PathBuf::from(mounts["host_data"][0].as_str().unwrap()).join("persistent/console/default/N-1");
    for subdir in ["bin", "log", "waldata"] {
        assert!(workspace.join(subdir).is_dir());
    }
    assert!(workspace.join("bin").read_dir().unwrap().next().is_some());
    assert!(workspace
        .join(format!("log/crowdb-paxos-kv-server-{first}.out.log"))
        .is_file());
    let server = request(&http, &base, Method::GET, "/api/nodes/1/server", Value::Null, 200).await;
    assert_eq!(server["url"], "http://127.0.0.1:7000");
    assert_eq!(
        request(&http, &base, Method::GET, "/api/nodes/1", Value::Null, 200).await["has_server"],
        true
    );
    request(
        &http,
        &base,
        Method::POST,
        "/api/nodes/1/server/deploy",
        input,
        409,
    )
    .await;
    let restarted = request(
        &http,
        &base,
        Method::POST,
        "/api/nodes/1/server/restart",
        json!({}),
        200,
    )
    .await;
    let second = restarted["pid"].as_u64().unwrap();
    assert!(second > 1);
    assert_ne!(first, second);
    assert_eq!(restarted["mgmt_url"], "http://127.0.0.1:7000");
    assert_eq!(
        request(&http, &base, Method::GET, "/api/nodes/1/server", Value::Null, 200).await["pid"],
        second
    );
    request(
        &http,
        &base,
        Method::POST,
        "/api/nodes/1/server/stop",
        json!({}),
        200,
    )
    .await;
    let stopped = request(&http, &base, Method::GET, "/api/nodes/1/server", Value::Null, 200).await;
    assert!(stopped["pid"].is_null());
    assert!(workspace.is_dir());
}
