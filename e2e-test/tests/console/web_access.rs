// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::time::Duration;

async fn call(http: &Client, base: &str, method: Method, path: &str, body: Value, status: u16) -> String {
    let response = http
        .request(method, format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let actual = response.status().as_u16();
    let text = response.text().await.unwrap();
    assert_eq!(actual, status, "{path}: {text}");
    text
}

#[tokio::test]
#[ignore = "fixture setup requires the owned storage authority"]
async fn prepare() {
    let hardware = crowdb_kv_client::HardwareClient::new(crate::common::hardware::kv_client());
    hardware
        .add_rack(
            1,
            &crowdb_protocol::common::RackValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                node_ids: vec![1],
                name: "native fixture".into(),
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(
            1,
            1,
            &crowdb_protocol::common::NodeValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                management_host: "127.0.0.1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires owned release Web and native storage containers"]
async fn native_access_deployment_provisions_private_credentials_and_reuses_them() {
    let addresses = crate::common::management::addresses(13);
    let base = format!("http://{}:9090", addresses[12]);
    let http = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let deployed: Value = serde_json::from_str(
        &call(
            &http,
            &base,
            Method::POST,
            "/api/nodes/1/services/deploy",
            json!({"kind":"access-server", "instance_id":"902",
        "http_port":7502, "s3_port":7500, "health_port":7501, "local_deployment":true}),
            201,
        )
        .await,
    )
    .unwrap();
    let before = crate::common::control::node("access-credentials", 12).await;
    assert_eq!(before["pid"], deployed["pid"]);
    assert!(before["size"].as_u64().unwrap() > 0);
    assert_eq!(before["mode"], "600");
    assert!(
        call(&http, &base, Method::GET, "/api/access/s3/", Value::Null, 200)
            .await
            .contains("ListAllMyBucketsResult")
    );
    call(
        &http,
        &base,
        Method::GET,
        "/api/access/iceberg/v1/namespaces",
        Value::Null,
        200,
    )
    .await;
    let restarted: Value = serde_json::from_str(
        &call(
            &http,
            &base,
            Method::POST,
            "/api/services/access-server-902/restart",
            Value::Null,
            200,
        )
        .await,
    )
    .unwrap();
    let after = crate::common::control::node("access-credentials", 12).await;
    assert_eq!(after["pid"], restarted["pid"]);
    assert_eq!(after["sha256"], before["sha256"]);
    assert_eq!(after["size"], before["size"]);
    assert_eq!(after["mode"], "600");
    call(&http, &base, Method::GET, "/api/access/s3/", Value::Null, 200).await;
    call(
        &http,
        &base,
        Method::DELETE,
        "/api/services/access-server-902",
        Value::Null,
        200,
    )
    .await;
    let stopped = crate::common::control::node("access-stopped", 12).await;
    assert_eq!(stopped["pid"], restarted["pid"]);
    assert_eq!(stopped["alive"], false);
}
