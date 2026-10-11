// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::Client;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

async fn get(http: &Client, base: &str, path: &str) -> Value {
    let response = http.get(format!("{base}{path}")).send().await.unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{path}: {status}: {text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test]
#[ignore = "requires owned release KV and Web containers"]
async fn structured_metrics_preserve_node_group_and_store_scopes() {
    let addresses = crate::common::management::addresses(1);
    let base = format!("http://{}:9090", addresses[0]);
    let http = Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
    assert_eq!(get(&http, &base, "/api/mode").await["mode"], "legacy");
    for (path, body, expected) in [
        ("/api/racks", json!({"id":1}), 201),
        (
            "/api/nodes",
            json!({"id":1,"rack_id":1,"host":"127.0.0.1","ssh_user":""}),
            201,
        ),
        (
            "/api/nodes/1/server/deploy",
            json!({"rest_port":7000,"rpc_port":7001,"election_profile":"test"}),
            201,
        ),
        ("/api/cluster/init", json!({"nodes":[1]}), 201),
    ] {
        let response = http
            .post(format!("{base}{path}"))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let text = response.text().await.unwrap();
        assert_eq!(status, expected, "{path}: {text}");
    }
    // Readiness observes topology; metrics assertions are executed only once.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let group = get(&http, &base, "/api/stores/0/groups/0").await;
        if group["replicas"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["role"] == "leader")
        {
            break;
        }
        assert!(Instant::now() < deadline, "system group has no leader: {group}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let node = get(&http, &base, "/api/nodes/1/metrics").await;
    assert!(node["window_secs"].as_f64().is_some());
    assert!(node["timestamp"].is_string());
    let metrics = node["metrics"].as_array().unwrap();
    assert!(!metrics.is_empty(), "{node}");
    for metric in metrics {
        assert!(metric["name"].is_string());
        assert!(metric["kind"].is_string());
        assert!(metric["fields"].as_array().is_some());
    }
    for (path, prefix) in [
        ("/api/stores/0/groups/0/metrics", "s.0.g.0."),
        ("/api/stores/0/metrics", "s.0."),
    ] {
        let response = get(&http, &base, path).await;
        let metrics = response["metrics"].as_array().unwrap();
        assert!(!metrics.is_empty(), "{path}: {response}");
        for metric in metrics {
            let name = metric["name"].as_str().unwrap();
            assert!(name.starts_with(prefix), "{path}: unexpected metric {name}");
        }
    }
}
