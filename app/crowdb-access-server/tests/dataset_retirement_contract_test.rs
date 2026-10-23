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
    let split_op = "16161616161616161616161616161616";
    let split = format!("{root}/split-drafts/{split_op}");
    response(
        client
            .post(format!("{root}/snapshots/{old}/split-drafts"))
            .json(&common::begin(split_op, &Value::Null)),
        200,
    )
    .await;
    common::prepare(&client, &split, "1").await;
    response(client.delete(&split).json(&json!({"expected_revision":"1"})), 200).await;
    for step in 0..100 {
        if response(client.post(format!("{split}/cleanup")), 200).await["complete"] == true {
            break;
        }
        assert!(step < 99, "Split cleanup did not terminate");
    }
    let operation = "17171717171717171717171717171717";
    let draft = format!("{root}/compositions/{operation}");
    response(
        client
            .post(format!("{root}/compositions"))
            .json(&composition::begin(operation, &Value::Null)),
        200,
    )
    .await;
    response(client.put(&draft).json(&json!({"edit":"18181818181818181818181818181818","expected_revision":"1","samples":[{"kind":"upsert","id":"YQ==","fields":[{"name":"value","value":{"kind":"inline","data":"Yg=="}}]}]})),200).await;
    let prepared = composition::prepare(&client, &draft, "2", "1").await;
    composition::publish(
        &client,
        &root,
        &draft,
        &prepared,
        "19191919191919191919191919191919",
    )
    .await;
    composition::cleanup(&client, &draft).await;
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
    response(client.get(format!("{snapshot}/summary")), 404).await;
    assert_eq!(
        response(client.get(format!("{root}/head")), 200).await["latest"],
        prepared["preparation"]["snapshot"]
    );
    drop(client);
    test.stop().await;
}
