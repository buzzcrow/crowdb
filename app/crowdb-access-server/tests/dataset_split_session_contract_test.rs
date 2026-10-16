// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_v2.rs"]
mod common;
#[path = "common/dataset.rs"]
mod dataset;
use common::response;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::test]
async fn split_plan_pins_revision_and_only_explicit_acknowledgement_advances_delivery() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let revision = published_split(&client, &root, &snapshot).await;
    let mut input = plan("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let prepared = prepare(&client, &root, &mut input).await;
    assert_eq!(prepared["snapshot"], snapshot);
    assert_eq!(prepared["split"]["revision"], revision);
    let id = input["id"].as_str().unwrap();
    let url = format!("{root}/read-plans/{id}");
    let initial = prepared["session"]["acknowledged"].clone();
    assert_eq!(
        response(client.get(&url), 200).await["session"]["acknowledged"],
        initial
    );
    let first = batch(&client, &url).await;
    assert!(!first["sample_ids"].as_array().unwrap().is_empty());
    assert_eq!(batch(&client, &url).await, first);
    assert_eq!(
        response(client.get(format!("{url}/progress")), 200).await["cursor"],
        initial
    );
    let ack = json!({"expected":initial, "next":first["next"]});
    let accepted = response(client.put(format!("{url}/progress")).json(&ack), 200).await;
    assert_eq!(accepted["cursor"], first["next"]);
    let second = batch(&client, &url).await;
    assert_ne!(first["sample_ids"], second["sample_ids"]);
    assert_eq!(response(client.delete(&url), 200).await["closed"], true);
    response(client.get(&url), 499).await;
    response(client.post(format!("{root}/read-plans")).json(&input), 499).await;
    drop(client);
    test.stop().await;
}
fn plan(id: &str) -> Value {
    json!({"id":id,"snapshot":"latest","selection":{"kind":"all"},"fields":["value"],"batch_size":3,"partition":{"worker":0,"workers":1,"rank":0,"world_size":1},"shuffle":null,"split":{"revision":null,"name":"train/a b"},"expected_page":"0"})
}
async fn prepare(client: &Client, root: &str, input: &mut Value) -> Value {
    for _ in 0..10 {
        let result = response(client.post(format!("{root}/read-plans")).json(input), 200).await;
        if result["complete"] == true {
            return result;
        }
        input["expected_page"] = result["next_page"].clone();
    }
    panic!("read preparation did not finish");
}
async fn published_split(client: &Client, root: &str, snapshot: &str) -> String {
    let op = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    response(
        client
            .post(format!("{root}/snapshots/{snapshot}/split-drafts"))
            .json(&common::begin(op, &Value::Null)),
        200,
    )
    .await;
    let draft = format!("{root}/split-drafts/{op}");
    let prepared = common::prepare(client, &draft, "1").await;
    common::publish(
        client,
        &draft,
        &prepared,
        "cccccccccccccccccccccccccccccccc",
        Value::Null,
    )
    .await;
    prepared["revision"].as_str().unwrap().into()
}
async fn batch(client: &Client, url: &str) -> Value {
    let response = client
        .post(format!("{url}/next"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
    assert_eq!(&bytes[..8], b"CDBDS001");
    assert_eq!(bytes[8], 1);
    let length = u32::from_be_bytes(bytes[9..13].try_into().unwrap()) as usize;
    serde_json::from_slice(&bytes[13..13 + length]).unwrap()
}

#[tokio::test]
async fn incomplete_read_can_be_inspected_and_cancelled_without_creating_delivery() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, _) = common::fixture(&client, &test.origin).await;
    let mut input = plan("dddddddddddddddddddddddddddddddd");
    input["split"] = Value::Null;
    let first = response(client.post(format!("{root}/read-plans")).json(&input), 200).await;
    assert_eq!(first["complete"], false);
    assert_eq!(first["session"], Value::Null);
    let url = format!("{root}/read-plans/{}", input["id"].as_str().unwrap());
    assert_eq!(response(client.get(&url), 200).await, first);
    response(client.post(format!("{url}/next")), 409).await;
    response(client.get(format!("{url}/progress")), 409).await;
    assert_eq!(response(client.delete(&url), 200).await["closed"], true);
    input["expected_page"] = first["next_page"].clone();
    response(client.post(format!("{root}/read-plans")).json(&input), 499).await;
    input["id"] = json!("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
    input.as_object_mut().unwrap().remove("split");
    response(client.post(format!("{root}/read-plans")).json(&input), 400).await;
    drop(client);
    test.stop().await;
}
