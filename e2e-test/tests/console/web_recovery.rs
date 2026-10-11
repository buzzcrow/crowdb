// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::Client;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

async fn request(http: &Client, path: &str, body: Option<Value>) -> Value {
    let addresses = crate::common::management::addresses(1);
    let url = format!("http://{}:9090{path}", addresses[0]);
    let response = if let Some(body) = body {
        http.post(url).json(&body).send().await.unwrap()
    } else {
        http.get(url).send().await.unwrap()
    };
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {text}");
    serde_json::from_str(&text).unwrap()
}

async fn restart() {
    crate::common::control::node("kill", 0).await;
    crate::common::control::node("start", 0).await;
}

#[tokio::test]
#[ignore = "requires an owned standalone release Web and KV deployment"]
async fn standalone_recovers_kv_before_and_after_group0_init() {
    let http = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    request(&http, "/api/racks", Some(json!({"id":1,"name":"rack"}))).await;
    request(
        &http,
        "/api/nodes",
        Some(json!({"id":1,"rack_id":1,"host":"127.0.0.1"})),
    )
    .await;
    let deployed = request(
        &http,
        "/api/nodes/1/server/deploy",
        Some(json!({"rest_port":7000,"rpc_port":7001})),
    )
    .await;
    assert!(deployed["pid"].as_u64().unwrap() > 1);
    assert_eq!(request(&http, "/api/stores", None).await, json!([]));
    restart().await;
    assert_eq!(request(&http, "/api/servers", None).await[0]["health"], "up");
    let root = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    crate::common::control::node("export-standalone-config", 0).await;
    let config: crowdb_console_shared::ConsoleConfig =
        serde_json::from_slice(&std::fs::read(root.join("standalone-config.json")).unwrap()).unwrap();
    crowdb_console_shared::bootstrap_intent::BootstrapIntent::capture(&config, &[1])
        .unwrap()
        .seal(&root.join("bootstrap-intent.toml"))
        .unwrap();
    crate::common::control::node("seal-bootstrap-intent", 0).await;
    restart().await;
    assert_eq!(
        crate::common::control::node("export-standalone-config", 0).await["intent_exists"],
        false
    );
    assert_eq!(request(&http, "/api/stores", None).await[0]["store_id"], 0);
    request(&http, "/api/stores", Some(json!({"store_id":7,"nodes":[1]}))).await;
    request(
        &http,
        "/api/stores/7/groups",
        Some(json!({"group_id":1,"replica_id":1,"nodes":[1]})),
    )
    .await;
    request(
        &http,
        "/api/stores/7/groups/1/kv/put",
        Some(json!({"key":"restart-key","value":"saved-value"})),
    )
    .await;
    restart().await;
    assert_eq!(request(&http, "/api/racks", None).await[0]["name"], "rack");
    assert_eq!(
        request(&http, "/api/stores/7/groups/1/kv/get?key=restart-key", None).await["value_utf8"],
        "saved-value"
    );
    crate::common::control::node("export-standalone-config", 0).await;
    let saved: Value =
        serde_json::from_slice(&std::fs::read(root.join("standalone-config.json")).unwrap()).unwrap();
    assert!(saved["server"][0]["pid"].as_u64().unwrap() > 1);
}
