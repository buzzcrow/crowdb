// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[allow(dead_code)]
#[path = "common/dataset_v2.rs"]
mod common;
#[path = "common/dataset.rs"]
mod dataset;
use common::response;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use std::sync::Arc;

#[tokio::test]
async fn lifecycle_observation_is_readonly_and_distinguishes_protection() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let path = format!("{root}/snapshots/{snapshot}");
    let head = response(client.get(format!("{root}/head")), 200).await;
    let observation = response(client.get(format!("{path}/reclaim")), 200).await;
    assert_eq!(observation["latest"], true);
    assert_eq!(observation["retained"], false);
    assert_eq!(observation["state"], "not_started");
    response(client.post(format!("{path}/retain")), 200).await;
    assert_eq!(
        response(client.get(format!("{path}/reclaim")), 200).await["retained"],
        true
    );
    response(client.post(format!("{path}/release")), 200).await;
    assert_eq!(
        response(client.get(format!("{path}/reclaim")), 200).await["retained"],
        false
    );
    assert_eq!(response(client.get(format!("{root}/head")), 200).await, head);
    response(
        client.get(format!(
            "{root}/snapshots/ffffffffffffffffffffffffffffffff/reclaim"
        )),
        404,
    )
    .await;
    test.stop().await;
}
