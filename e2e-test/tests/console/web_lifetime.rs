// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::Client;
use serde_json::{json, Value};

async fn post(http: &Client, path: &str, body: Value) -> Value {
    let addresses = crate::common::management::addresses(1);
    let response = http
        .post(format!("http://{}:9090{path}", addresses[0]))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{path}: {status}: {text}");
    serde_json::from_str(&text).unwrap()
}

async fn terminate(forced: bool) {
    let http = Client::new();
    post(&http, "/api/racks", json!({"id":1})).await;
    post(
        &http,
        "/api/nodes",
        json!({"id":1,"rack_id":1,"host":"127.0.0.1","ssh_user":""}),
    )
    .await;
    let deployed = post(
        &http,
        "/api/nodes/1/server/deploy",
        json!({"rest_port":7000,"rpc_port":7001,"election_profile":"test"}),
    )
    .await;
    let recorded = crate::common::control::request(json!({
        "action":"record-disposable-child","node":0,"pid":deployed["pid"]}))
    .await;
    assert_eq!(recorded["recorded"], true);
    assert_eq!(recorded["child_alive"], true);
    assert_eq!(recorded["ephemeral"], true);
    let exited = crate::common::control::request(json!({
        "action":"terminate-disposable-web","node":0,"signal":if forced {"KILL"} else {"TERM"}}))
    .await;
    assert!(exited["seconds"].as_f64().unwrap() < 5.0);
    assert_eq!(exited["sentinel"], "preserve");
    let final_state = if forced {
        assert_ne!(exited["exit_code"], 0);
        assert_eq!(exited["child_alive"], true);
        assert_eq!(exited["ephemeral"], true);
        crate::common::control::node("cleanup-disposable-runtime", 0).await
    } else {
        assert_eq!(exited["exit_code"], 0);
        exited
    };
    assert_eq!(final_state["child_alive"], false);
    assert_eq!(final_state["ephemeral"], false);
    assert_eq!(final_state["sentinel"], "preserve");
}

#[tokio::test]
#[ignore = "requires disposable release Web inside an owned surviving PID namespace"]
async fn test_mode_termination_reaps_children_and_preserves_persistent_runtime() {
    terminate(false).await;
}

#[tokio::test]
#[ignore = "requires disposable release Web inside an owned surviving PID namespace"]
async fn test_mode_forced_exit_recovers_owned_children_without_removing_persistent_data() {
    terminate(true).await;
}
