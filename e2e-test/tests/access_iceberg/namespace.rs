// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::{control, management};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg frontends"]
async fn namespace_functional_crud_survives_native_storage_and_listener_restart() {
    let ips = management::addresses(14);
    let origins: Vec<_> = ips[12..].iter().map(|ip| format!("http://{ip}:7502")).collect();
    for origin in &origins {
        official_client(origin);
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let created = client
        .post(format!("{}/v1/namespaces", origins[0]))
        .bearer_auth("w".repeat(32))
        .header("content-type", "application/json")
        .body(r#"{"namespace":["persisted"],"properties":{"owner":"before-restart"}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    verify_retained_namespace(&client, &origins[1]).await;
    for index in [12, 13] {
        control::node("stop", index).await;
    }
    control::node("kill", 11).await;
    control::node("start", 11).await;
    for (index, origin) in (12..14).zip(&origins) {
        control::node("start", index).await;
        verify_retained_namespace(&client, origin).await;
        official_client(origin);
    }
}

fn official_client(origin: &str) {
    let path = super::namespace_script();
    let output = std::process::Command::new("pixi")
        .args(["run", "-e", "iceberg-e2e", "python", &path, origin])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "official Iceberg client failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
}

async fn verify_retained_namespace(client: &reqwest::Client, origin: &str) {
    let response = client
        .get(format!("{origin}/v1/namespaces/persisted"))
        .bearer_auth("r".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["namespace"], serde_json::json!(["persisted"]));
    assert_eq!(body["properties"], serde_json::json!({"owner":"before-restart"}));
}
