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
async fn split_read_continuation_replays_without_remote_progress_or_cleanup() {
    use std::sync::atomic::Ordering;
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let revision = published_split(&client, &root, &snapshot).await;
    let binding = response(client.post(format!("{root}/reads/bind")).json(&plan()), 200).await;
    assert_eq!(binding["snapshot"], snapshot);
    assert_eq!(binding["split"]["revision"], revision);
    let before = store.admission_reads.load(Ordering::Relaxed);
    let first = batch(&client, &root, &binding["cursor"]).await;
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - before, 1);
    assert!(!first["sample_ids"].as_array().unwrap().is_empty());
    let independent = Client::new();
    assert_eq!(batch(&independent, &root, &binding["cursor"]).await, first);
    let second = batch(&client, &root, &first["next"]).await;
    assert_ne!(first["sample_ids"], second["sample_ids"]);
    assert_eq!(batch(&client, &root, &binding["cursor"]).await, first);
    for suffix in ["", "/progress", "/next"] {
        let url = format!("{root}/read-plans/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa{suffix}");
        response(client.get(&url), 404).await;
        response(client.post(&url), 404).await;
        response(client.put(&url), 404).await;
        response(client.delete(&url), 404).await;
    }
    drop(independent);
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn unsplit_delivery_has_no_outer_duplicate_admission_reads() {
    use std::sync::atomic::Ordering;
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, _) = common::fixture(&client, &test.origin).await;
    let mut request = plan();
    request["split"] = Value::Null;
    let binding = response(client.post(format!("{root}/reads/bind")).json(&request), 200).await;
    let before = store.admission_reads.load(Ordering::Relaxed);
    let writes = store.mutations.load(Ordering::Relaxed);
    let page = batch(&client, &root, &binding["cursor"]).await;
    assert_eq!(page["sample_ids"].as_array().unwrap().len(), 3);
    // Selection and delivery share one request-local authority check.
    assert_eq!(store.admission_reads.load(Ordering::Relaxed) - before, 1);
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    drop(client);
    test.stop().await;
}
fn plan() -> Value {
    json!({"snapshot":"latest","selection":{"kind":"all"},"fields":["value"],"batch_size":3,
        "partition":{"worker":0,"workers":1,"rank":0,"world_size":1},"shuffle":null,
        "split":{"revision":null,"name":"train/a b"}})
}
async fn published_split(client: &Client, root: &str, snapshot: &str) -> String {
    let prepared = common::prepare(client, root, snapshot, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", false).await;
    common::publish(client, root, snapshot, &prepared, &Value::Null).await;
    prepared["revision"].as_str().unwrap().into()
}
async fn batch(client: &Client, url: &str, cursor: &Value) -> Value {
    let response = client
        .post(format!("{url}/reads/next"))
        .json(&json!({"cursor":cursor}))
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
async fn binding_rejects_unknown_split_and_malformed_cursor() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, _) = common::fixture(&client, &test.origin).await;
    response(client.post(format!("{root}/reads/bind")).json(&plan()), 404).await;
    response(
        client
            .post(format!("{root}/reads/next"))
            .json(&json!({"cursor":"invalid"})),
        400,
    )
    .await;
    let mut input = plan();
    input.as_object_mut().unwrap().remove("split");
    response(client.post(format!("{root}/reads/bind")).json(&input), 400).await;
    drop(client);
    test.stop().await;
}
