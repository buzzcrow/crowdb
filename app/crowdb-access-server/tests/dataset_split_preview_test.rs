// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset.rs"]
mod dataset;
use base64::{engine::general_purpose::STANDARD, Engine};
use common::response;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
};

#[tokio::test]
async fn split_preview_scans_empty_windows_binds_rules_and_never_mutates() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let url = format!("{root}/snapshots/{snapshot}/split-preview");
    let input = json!({"name":"late", "origin":{"kind":"evaluated"}, "definition":{"kind":"selection", "predicate":{"kind":"prefix", "value":STANDARD.encode([40])}}});
    let writes = store.mutations.load(Ordering::Relaxed);
    let first = response(client.post(&url).json(&input), 200).await;
    assert_eq!(first["entries"], json!([]));
    assert!(first["after"].is_string());
    let mut changed = input.clone();
    changed["definition"]["predicate"]["value"] = json!(STANDARD.encode([41]));
    response(
        client
            .post(&url)
            .query(&[("after", first["after"].as_str().unwrap())])
            .json(&changed),
        400,
    )
    .await;
    let second = response(
        client
            .post(&url)
            .query(&[("after", first["after"].as_str().unwrap())])
            .json(&input),
        200,
    )
    .await;
    assert_eq!(second["entries"], json!([]));
    let third = response(
        client
            .post(&url)
            .query(&[("after", second["after"].as_str().unwrap())])
            .json(&input),
        200,
    )
    .await;
    assert_eq!(third["entries"], json!([STANDARD.encode([40, 0, 255])]));
    assert!(third["after"].is_null());
    response(client.post(&url).query(&[("limit", "101")]).json(&input), 400).await;
    let mut invalid = common::definitions()[0].clone();
    invalid["definition"]["index"] = json!(99);
    response(client.post(&url).json(&invalid), 400).await;
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn preview_and_committed_weighted_membership_match() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let base = format!("{root}/snapshots/{snapshot}");
    let writes = store.mutations.load(Ordering::Relaxed);
    let split = common::definitions()[0].clone();
    let mut preview = BTreeSet::new();
    let mut after = Value::Null;
    loop {
        let mut request = client.post(format!("{base}/split-preview")).json(&split);
        if let Some(cursor) = after.as_str() {
            request = request.query(&[("after", cursor)]);
        }
        let page = response(request, 200).await;
        for id in page["entries"].as_array().unwrap() {
            preview.insert(id.as_str().unwrap().to_owned());
        }
        after = page["after"].clone();
        if after.is_null() {
            break;
        }
    }
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    let revision = "12121212121212121212121212121212";
    common::prepare(&client, &root, &snapshot, revision, false).await;
    let url = format!("{base}/split-revisions/{revision}/splits/train%2Fa%20b/members");
    let members = response(client.get(&url).query(&[("limit", "100")]), 200).await;
    let committed: BTreeSet<_> = members["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(preview, committed);
    assert!(!preview.is_empty());
    drop(client);
    test.stop().await;
}
