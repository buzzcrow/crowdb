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
use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
};

#[tokio::test]
async fn split_revision_flow_preserves_data_head_and_historical_members() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let base = format!("{root}/snapshots/{snapshot}");
    let head = response(client.get(format!("{root}/head")), 200).await;
    let prepared = common::prepare(
        &client,
        &root,
        &snapshot,
        "02020202020202020202020202020202",
        false,
    )
    .await;
    let revision = prepared["revision"].as_str().unwrap();
    assert_eq!(
        response(client.get(format!("{base}/split-revisions/{revision}")), 200).await,
        prepared
    );
    assert_eq!(
        response(client.get(format!("{base}/split-revisions")), 200).await["entries"][0]["revision"],
        revision
    );
    let before_publish = members(
        &client,
        &format!("{base}/split-revisions/{revision}/splits/train%2Fa%20b/members"),
    )
    .await;
    let current = common::publish(&client, &root, &snapshot, &prepared, &Value::Null).await;
    assert_eq!(
        common::publish(&client, &root, &snapshot, &prepared, &Value::Null).await,
        current
    );
    assert_eq!(response(client.get(format!("{root}/head")), 200).await, head);
    assert_eq!(
        response(client.get(format!("{base}/split-revisions/{revision}")), 200).await,
        prepared
    );
    let current = response(client.get(format!("{base}/split-head")), 200).await;
    assert_eq!(current["revision"], revision);
    let train = members(
        &client,
        &format!("{base}/split-revisions/{revision}/splits/train%2Fa%20b/members"),
    )
    .await;
    let test_ids = members(
        &client,
        &format!("{base}/split-revisions/{revision}/splits/test/members"),
    )
    .await;
    assert_eq!(train, before_publish);
    assert!(train.is_disjoint(&test_ids));
    assert_eq!(train.len() + test_ids.len(), 45);
    assert_eq!(
        response(client.get(format!("{base}/split-revisions")), 200).await["entries"][0]["revision"],
        revision
    );
    drop(client);
    test.stop().await;
}
async fn members(client: &Client, url: &str) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    let mut after = None;
    for _ in 0..100 {
        let mut request = client.get(url).query(&[("limit", "3")]);
        if let Some(after) = after {
            request = request.query(&[("after", after)]);
        }
        let page = response(request, 200).await;
        for id in page["entries"].as_array().unwrap() {
            assert!(result.insert(id.as_str().unwrap().to_owned()));
        }
        after = page["after"].as_str().map(str::to_owned);
        if after.is_none() {
            return result;
        }
    }
    panic!("membership did not terminate");
}

#[tokio::test]
async fn retired_split_workflow_writes_never_access_storage() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let before = (
        store.reads.load(Ordering::Relaxed),
        store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    );
    response(
        client
            .post(format!("{root}/snapshots/{snapshot}/split-drafts"))
            .json(&json!({})),
        501,
    )
    .await;
    let draft = format!("{root}/split-drafts/04040404040404040404040404040404");
    response(client.put(&draft).json(&json!({})), 501).await;
    for suffix in ["evaluate", "prepare", "publish"] {
        response(client.post(format!("{draft}/{suffix}")).json(&json!({})), 501).await;
    }
    assert_eq!(
        (
            store.reads.load(Ordering::Relaxed),
            store.bounded_reads.load(Ordering::Relaxed),
            store.mutations.load(Ordering::Relaxed)
        ),
        before
    );
    drop(client);
    test.stop().await;
}
