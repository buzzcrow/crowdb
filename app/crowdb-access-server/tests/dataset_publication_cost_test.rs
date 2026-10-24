// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;
use base64::{engine::general_purpose::STANDARD, Engine};
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::Value;
use std::sync::{atomic::Ordering, Arc};

fn counts(store: &TestStore) -> (u64, u64) {
    (
        store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}
async fn call(client: &Client, url: &str, input: &Value) -> Value {
    let response = client
        .post(url)
        .bearer_auth("secret")
        .json(input)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, 200, "{body}");
    body
}

#[tokio::test]
async fn inline_publication_and_identical_replay_have_bounded_metadata_cost() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    for samples in [1u8, 40] {
        let root = format!(
            "{}/api/dataset/v1/namespaces/dataset-ns/datasets/publication-cost-{samples}",
            test.origin
        );
        assert_eq!(
            client
                .put(&root)
                .bearer_auth("secret")
                .send()
                .await
                .unwrap()
                .status()
                .as_u16(),
            200
        );
        let mut input: Value = serde_json::from_str(include_str!(
            "../../../sdk/contract-fixtures/dataset-v1/publication.json"
        ))
        .unwrap();
        let template = input["manifest"]["samples"][0].clone();
        input["manifest"]["samples"] = (0..samples)
            .map(|id| {
                let mut sample = template.clone();
                sample["id"] = STANDARD.encode([id]).into();
                sample
            })
            .collect();
        let before = counts(&store);
        let result = call(&client, &format!("{root}/snapshots/publish"), &input).await;
        let after = counts(&store);
        assert_eq!(
            (after.0 - before.0, after.1 - before.1),
            (12, 11 + 3 * u64::from(samples))
        );
        println!(
            "cold publication: {} reads, {} mutations",
            after.0 - before.0,
            after.1 - before.1
        );
        assert_eq!(result["is_latest"], true);
        let before = after;
        assert_eq!(
            call(&client, &format!("{root}/snapshots/publish"), &input).await,
            result
        );
        let after = counts(&store);
        assert_eq!((after.0 - before.0, after.1 - before.1), (11, 0));
        println!(
            "identical replay: {} reads, {} mutations",
            after.0 - before.0,
            after.1 - before.1
        );
    }
    drop(client);
    test.stop().await;
}
