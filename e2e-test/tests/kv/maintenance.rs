// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, origin, request, wait_for_leader};
use crowdb_paxos_kv_client::{ClientConfig, GetOutcome, PaxosKvClient, ReadMode};
use reqwest::Method;
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires the release-container fixture"]
async fn synchronous_step_down_fence() {
    let base = origin(&addresses(1)[0]);
    let client = client();
    let topology = wait_for_leader(&client, &base).await;
    assert_eq!(topology["stores"][0]["groups"][0]["leader_id"], 1);
    let path = "/stores/0/groups/1/step-down?sync=true";
    let accepted = request(&client, &base, Method::POST, path, json!({"reason": "test"}), 200).await;
    assert_eq!(accepted["accepted"], true);
    assert_eq!(accepted["current_leader_id"], 1);
    let topology = request(&client, &base, Method::GET, "/topology", Value::Null, 200).await;
    assert_eq!(
        topology["stores"][0]["groups"][0]["local_replica"]["role"],
        "follower"
    );
    let rejected = request(&client, &base, Method::POST, path, json!({}), 200).await;
    assert_eq!(rejected["accepted"], false, "a follower must reject step-down");
}

async fn maintenance_errors(client: &reqwest::Client, base: &str) {
    for path in [
        "/stores/0/groups/99/step-down?sync=true",
        "/stores/0/groups/99/flush",
        "/stores/0/groups/99/wipe-user-data",
        "/stores/99/groups/1/wipe-user-data",
    ] {
        request(client, base, Method::POST, path, json!({}), 404).await;
    }
    let api = request(client, base, Method::GET, "/openapi.json", Value::Null, 200).await;
    assert!(api["paths"]["/stores/{sid}/groups/{gid}/wipe-user-data"].is_object());
}

#[tokio::test]
#[ignore = "requires the release-container fixture"]
async fn flush_and_wipe_preserve_group_and_resume_writes() {
    let base = origin(&addresses(1)[0]);
    let client = client();
    maintenance_errors(&client, &base).await;
    wait_for_leader(&client, &base).await;
    let kv = PaxosKvClient::new(ClientConfig::new(vec![base.clone()]));
    kv.put(0, 1, b"wipe-test-key", b"wipe-test-value", None)
        .await
        .unwrap();
    assert!(
        matches!(kv.get(0, 1, b"wipe-test-key", ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::Found { value, .. } if value.as_ref() == b"wipe-test-value")
    );
    let flushed = request(
        &client,
        &base,
        Method::POST,
        "/stores/0/groups/1/flush",
        Value::Null,
        200,
    )
    .await;
    assert_eq!(flushed["store_id"], 0);
    assert_eq!(flushed["group_id"], 1);
    assert_eq!(flushed["accepted"], true);
    let wiped = request(
        &client,
        &base,
        Method::POST,
        "/stores/0/groups/1/wipe-user-data",
        json!({}),
        200,
    )
    .await;
    assert_eq!(wiped["accepted"], true);
    let restored = wait_for_leader(&client, &base).await;
    assert_eq!(restored["stores"][0]["store_id"], 0);
    assert_eq!(restored["stores"][0]["groups"][0]["group_id"], 1);
    let kv = PaxosKvClient::new(ClientConfig::new(vec![base]));
    assert!(matches!(
        kv.get(0, 1, b"wipe-test-key", ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
    kv.put(0, 1, b"wipe-post-key", b"wipe-post-value", None)
        .await
        .unwrap();
    assert!(
        matches!(kv.get(0, 1, b"wipe-post-key", ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::Found { value, .. } if value.as_ref() == b"wipe-post-value")
    );
}
