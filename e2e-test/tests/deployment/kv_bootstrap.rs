// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{client, request};
use reqwest::Method;
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires the selected release KV container"]
async fn monitor_creates_real_kv_groups_then_validates_after_restart() {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let bytes = std::fs::read(root.join("managed-node.json")).unwrap();
    let accepted: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(accepted["prepared"], true);
    assert_eq!(accepted["node_id"], 1);
    let write = std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write");
    let witness = root.join("initial-kv-node.json");
    if write {
        std::fs::write(&witness, &bytes).unwrap();
    } else {
        let initial: Value = serde_json::from_slice(&std::fs::read(witness).unwrap()).unwrap();
        assert_eq!(accepted, initial, "restart must preserve accepted initialization");
    }
    let origin = format!("http://{}:10000", crate::common::management::addresses(1)[0]);
    let http = client();
    let result = request(
        &http,
        &origin,
        Method::POST,
        "/system/prepare",
        json!({"replica_id": 1, "bootstrap": accepted["bootstrap"]}),
        200,
    )
    .await;
    assert_eq!(result["bootstrap"], accepted["bootstrap"]);
    if write {
        create_witness_group().await;
    }
    for group in [0, 1] {
        let ready = request(
            &http,
            &origin,
            Method::GET,
            &format!("/stores/0/groups/{group}/ready"),
            Value::Null,
            200,
        )
        .await;
        assert_eq!(ready["ready"], true);
        assert_eq!(ready["leader_id"], 1);
        assert_eq!(ready["voting_replicas"], 1);
    }
    let mut foreign = accepted["bootstrap"].clone();
    foreign["operation_id"] = json!(uuid::Uuid::new_v4().to_string());
    let conflict = http
        .post(format!("{origin}/system/prepare"))
        .json(&json!({"replica_id": 1, "bootstrap": foreign}))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), 409);
    let detail = conflict.text().await.unwrap();
    assert!(detail.contains("bootstrap"), "{detail}");
    let retained = request(
        &http,
        &origin,
        Method::POST,
        "/system/prepare",
        json!({"replica_id": 1, "bootstrap": accepted["bootstrap"]}),
        200,
    )
    .await;
    assert_eq!(retained["bootstrap"], accepted["bootstrap"]);
    super::witness_with_write(false, write).await;
}

pub(super) async fn create_witness_group() {
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
    use std::time::{Duration, Instant};

    let origin = format!("http://{}:10000", crate::common::management::addresses(1)[0]);
    let kv = CrowdbKvClient::new(ClientConfig::new(vec![origin.clone()]));
    assert!(matches!(
        kv.get(
            0,
            0,
            crowdb_console_shared::deployment::preparation::DEFAULT_PREPARATION_KEY,
            ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap(),
        GetOutcome::NotFound
    ));
    let http = client();
    // KV-only admission creates Group 0. This case explicitly requests its
    // ordinary witness group instead of depending on storage preparation.
    request(
        &http,
        &origin,
        Method::GET,
        "/stores/0/groups/1/ready",
        Value::Null,
        404,
    )
    .await;
    request(
        &http,
        &origin,
        Method::POST,
        "/stores/0/groups",
        json!({"group_id":1,"replica_id":1}),
        201,
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let ready = request(
            &http,
            &origin,
            Method::GET,
            "/stores/0/groups/1/ready",
            Value::Null,
            200,
        )
        .await;
        if ready["ready"] == true {
            assert_eq!(ready["leader_id"], 1);
            assert_eq!(ready["voting_replicas"], 1);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "explicit witness group did not become ready: {ready}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
