// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, origin, request};
use reqwest::Method;
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires a bare release-container node and retained restart"]
async fn initialize_and_restore() {
    let base = origin(&addresses(1)[0]);
    let http = client();
    let write = std::env::var("CROWDB_E2E_PHASE").unwrap() == "write";
    request(&http, &base, Method::GET, "/health", Value::Null, 200).await;
    if write {
        let initial = request(&http, &base, Method::GET, "/stores", Value::Null, 200).await;
        assert!(initial["stores"].as_array().unwrap().is_empty());
        let created = request(&http, &base, Method::POST, "/system/init", json!({}), 201).await;
        assert_eq!(created["store_id"], 0);
        assert_eq!(created["group_id"], 0);
        assert_eq!(created["replica_id"], 1);
        assert!(created["listen_addr"].as_str().unwrap().ends_with(":7001"));
    }
    request(&http, &base, Method::POST, "/system/init", json!({}), 409).await;
    let stores = request(&http, &base, Method::GET, "/stores", Value::Null, 200).await;
    assert_eq!(stores["stores"].as_array().unwrap().len(), 1);
    assert_eq!(stores["stores"][0]["store_id"], 0);
    let groups = request(&http, &base, Method::GET, "/stores/0/groups", Value::Null, 200).await;
    assert_eq!(groups.as_array().unwrap().len(), 1);
    assert_eq!(groups[0]["group_id"], 0);
    request(&http, &base, Method::GET, "/health", Value::Null, 200).await;
    if !write {
        let added = request(&http, &base, Method::POST, "/stores", json!({"store_id": 7}), 201).await;
        assert!(added["listen_addr"].as_str().unwrap().ends_with(":7002"));
        let topology = request(&http, &base, Method::GET, "/topology", Value::Null, 200).await;
        let system = topology["stores"]
            .as_array()
            .unwrap()
            .iter()
            .find(|store| store["store_id"] == 0)
            .unwrap();
        assert!(system["listen_addr"].as_str().unwrap().ends_with(":7001"));
    }
}

#[tokio::test]
#[ignore = "requires a bare release-container node"]
async fn custom_replica() {
    let base = origin(&addresses(1)[0]);
    let created = request(
        &client(),
        &base,
        Method::POST,
        "/system/init",
        json!({"replica_id": 5, "start_election": true}),
        201,
    )
    .await;
    assert_eq!(created["store_id"], 0);
    assert_eq!(created["group_id"], 0);
    assert_eq!(created["replica_id"], 5);
}
