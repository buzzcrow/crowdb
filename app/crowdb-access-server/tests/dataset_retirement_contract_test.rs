// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset_composition.rs"]
mod composition;
#[path = "common/dataset.rs"]
mod dataset;
use common::response;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::test]
async fn retirement_respects_retention_and_finishes_split_and_composition_cleanup() {
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, old) = common::fixture(&client, &test.origin).await;
    let revision = common::prepare(&client, &root, &old, "16161616161616161616161616161616", false).await;
    let head = response(client.get(format!("{root}/head")), 200).await;
    let mut input = composition::input(&Value::Null);
    input["samples"] = json!([{"kind":"upsert","id":"YQ==","fields":[{"name":"value","value":{"kind":"inline","data":"Yg=="}}]}]);
    let prepared = composition::prepare(&client, &root, "17171717171717171717171717171717", &input).await;
    composition::publish(&client, &root, &prepared, &head["generation"]).await;
    let snapshot = format!("{root}/snapshots/{old}");
    response(client.post(format!("{snapshot}/retain")), 200).await;
    response(client.post(format!("{snapshot}/reclaim")), 409).await;
    response(client.post(format!("{snapshot}/release")), 200).await;
    let marked = response(client.post(format!("{snapshot}/reclaim")), 200).await;
    assert_eq!(marked["phase"], "grace_period");
    assert_eq!(marked["complete"], false);
    assert_eq!(marked["reclaim_after"], 1300);
    response(client.get(format!("{snapshot}/summary")), 404).await;
    test.clock.store(1300, std::sync::atomic::Ordering::Release);
    for step in 0..100 {
        let progress = response(client.post(format!("{snapshot}/reclaim")), 200).await;
        assert_eq!(progress["physical_cleanup_deferred"], true);
        if progress["complete"] == true {
            assert_eq!(progress["phase"], "complete");
            break;
        }
        assert!(step < 99, "Snapshot retirement did not terminate");
    }
    response(
        client.get(format!(
            "{snapshot}/split-revisions/{}",
            revision["revision"].as_str().unwrap()
        )),
        404,
    )
    .await;
    response(client.get(format!("{snapshot}/summary")), 404).await;
    assert_eq!(
        response(client.get(format!("{root}/head")), 200).await["latest"],
        prepared["snapshot"]
    );
    drop(client);
    test.stop().await;
}
