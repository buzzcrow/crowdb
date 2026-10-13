// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires an owned release Web registry and packaged KV child"]
async fn bare_metal_web_loads_launch_policy_without_restoring_topology() {
    let root = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mounts: Value = serde_json::from_slice(&std::fs::read(root.join("mounts.json")).unwrap()).unwrap();
    let data = PathBuf::from(mounts["host_data"][0].as_str().unwrap());
    let identity_path = data.join("launches.runtime/701-kv.json");
    let addresses = crate::common::management::addresses(1);
    let base = format!("http://{}:9090", addresses[0]);
    let http = Client::builder().timeout(Duration::from_secs(3)).build().unwrap();
    let response = http.get(format!("{base}/api/launches")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let view: Value = response.json().await.unwrap();
    let first = view[0]["process"]["pid"].as_u64().expect("auto-start process");
    assert!(first > 1);
    let identity: Value = serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
    assert_eq!(identity["identity"]["pid"], first);
    let topology: Value = http
        .get(format!("http://{}:7000/topology", addresses[0]))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(topology["stores"].as_array().unwrap().is_empty());
    assert_eq!(
        http.get(format!("{base}/api/stores"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let restarted: Value = http
        .post(format!("{base}/api/launches/701/kv/restart"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let second = restarted["pid"].as_u64().unwrap();
    assert_ne!(second, first);
    let identity: Value = serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
    assert_eq!(identity["identity"]["pid"], second);
    assert_eq!(
        http.post(format!("{base}/api/launches/701/kv/stop"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let stopped: Value = http
        .get(format!("{base}/api/launches"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(stopped[0]["process"].is_null());
    assert!(!identity_path.exists());
    let path = data.join("launches.toml");
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(!body.contains("pid"));
    std::fs::write(&path, body.replace("auto_start = true", "auto_start = false")).unwrap();
    let updated: Value = http
        .get(format!("{base}/api/launches"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(updated[0]["auto_start"], false);
    assert!(!std::fs::read_to_string(path).unwrap().contains("pid"));
}
