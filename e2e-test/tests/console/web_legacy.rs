// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::Client;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub(super) async fn get(http: &Client, base: &str, path: &str) -> Value {
    let response = http.get(format!("{base}{path}")).send().await.unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{path}: {status}: {text}");
    serde_json::from_str(&text).unwrap()
}

pub(super) async fn bootstrap() -> (Client, String) {
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
        if path == "/api/cluster/init" {
            let result: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(result["store_id"], 0);
            assert_eq!(result["group_id"], 0);
        }
    }
    // Observe readiness before executing each flow once.
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
    (http, base)
}
