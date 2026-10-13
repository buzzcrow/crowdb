// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires an owned release Web container and host clients"]
async fn survives_peer_fixture_retirement() {
    let http = Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
    let addresses = crate::common::management::addresses(1);
    let base = format!("http://{}:9090", addresses[0]);
    let token = std::env::var("CROWDB_E2E_TOKEN").unwrap();
    let phase = std::env::var("CROWDB_E2E_PHASE").unwrap();
    if phase != "verify" {
        for (path, body) in [
            ("/api/racks", json!({"id":1,"name":token})),
            ("/api/nodes", json!({"id":1,"rack_id":1,"host":"127.0.0.1"})),
        ] {
            let response = http
                .post(format!("{base}{path}"))
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::CREATED,
                "{}",
                response.text().await.unwrap()
            );
        }
    }
    for (path, expected) in [
        ("/api/racks", json!([{"id":1,"name":token}])),
        ("/api/nodes", json!([{"id":1,"rack_id":1,"host":"127.0.0.1"}])),
    ] {
        let result: Value = http
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(result.as_array().unwrap().len(), 1);
        for (key, value) in expected[0].as_object().unwrap() {
            assert_eq!(result[0][key], *value);
        }
    }
    assert_eq!(
        http.get(format!("{base}/healthz"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap(),
        "ok"
    );
}
