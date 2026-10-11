// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::Client;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

fn data() -> PathBuf {
    let root = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mounts: Value = serde_json::from_slice(&std::fs::read(root.join("mounts.json")).unwrap()).unwrap();
    PathBuf::from(mounts["host_data"][0].as_str().unwrap())
}

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

#[tokio::test]
#[ignore = "requires an owned standalone release Web container"]
async fn empty_ui_persists_rack_and_node_across_restart() {
    let http = Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
    assert_eq!(request(&http, "/api/mode", None).await["mode"], "legacy");
    assert_eq!(request(&http, "/api/racks", None).await, json!([]));
    assert_eq!(request(&http, "/api/stores", None).await, json!([]));
    let addresses = crate::common::management::addresses(1);
    assert!(http
        .get(format!("http://{}:9090", addresses[0]))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    for path in ["/", "/some/spa/route"] {
        let response = http
            .get(format!("http://{}:9090{path}", addresses[0]))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert!(response.text().await.unwrap().contains("id=\"root\""));
    }
    assert_eq!(
        http.get(format!("http://{}:9090/healthz", addresses[0]))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "ok"
    );
    let racks = http
        .get(format!("http://{}:9090/api/racks", addresses[0]))
        .send()
        .await
        .unwrap();
    assert!(racks.status().is_success());
    assert!(racks.headers()[reqwest::header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("application/json"));
    let traversal = http
        .get(format!("http://{}:9090/../Cargo.toml", addresses[0]))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!traversal.contains("[package]") && !traversal.contains("[workspace]"));
    request(&http, "/api/racks", Some(json!({"id":1,"name":"saved-rack"}))).await;
    request(
        &http,
        "/api/nodes",
        Some(json!({"id":1,"rack_id":1,"host":"127.0.0.1"})),
    )
    .await;
    crate::common::control::node("kill", 0).await;
    crate::common::control::node("start", 0).await;
    assert_eq!(request(&http, "/api/racks", None).await[0]["name"], "saved-rack");
    assert_eq!(request(&http, "/api/nodes", None).await[0]["id"], 1);
    assert!(data().join("persistent/console/default/config.json").is_file());
}

#[tokio::test]
#[ignore = "requires an owned rejected standalone release Web container"]
async fn malformed_standalone_config_is_preserved_and_rejected() {
    let root = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let rejection: Value =
        serde_json::from_slice(&std::fs::read(root.join("standalone-rejection.json")).unwrap()).unwrap();
    assert_ne!(rejection["state"]["ExitCode"], 0);
    assert!(rejection["log"].as_str().unwrap().contains("config.json"));
    assert_eq!(
        std::fs::read_to_string(data().join("persistent/console/default/config.json")).unwrap(),
        "broken config"
    );
}
