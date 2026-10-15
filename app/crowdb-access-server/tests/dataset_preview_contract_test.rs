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
use md5::{Digest, Md5};
use reqwest::Client;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn exact_matrix_and_inline_preview_do_not_create_read_sessions() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let base = format!("{root}/snapshots/{snapshot}");
    let ids = [STANDARD.encode([20, 0, 255]), STANDARD.encode([3, 0, 255])];
    let matrix = response(
        client
            .post(format!("{base}/matrix"))
            .json(&json!({"sample_ids":ids,"fields":["value"]})),
        200,
    )
    .await;
    assert_eq!(matrix["entries"][0]["id"], ids[0]);
    assert_eq!(matrix["entries"][1]["id"], ids[1]);
    assert_eq!(matrix["entries"][0]["fields"][0]["source_snapshot"], snapshot);
    let preview = response(
        client
            .get(format!("{base}/preview"))
            .query(&[("sample", ids[0].as_str()), ("field", "value")]),
        200,
    )
    .await;
    assert_eq!(preview["data"], STANDARD.encode([20]));
    assert_eq!(preview["length"], "1");
    assert_eq!(preview["truncated"], false);
    response(
        client.get(format!("{base}/preview")).query(&[
            ("sample", ids[0].as_str()),
            ("field", "value"),
            ("max_bytes", "65537"),
        ]),
        400,
    )
    .await;
    response(
        client
            .post(format!("{base}/matrix"))
            .json(&json!({"sample_ids":[ids[0],ids[0]],"fields":["value"]})),
        400,
    )
    .await;
    response(
        client
            .post(format!("{base}/matrix"))
            .json(&json!({"sample_ids":["bWlzc2luZw=="],"fields":["value"]})),
        404,
    )
    .await;
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn payload_preview_caps_bytes_across_parts_and_distinguishes_missing_tombstone() {
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let root = format!(
        "{}/api/dataset/v1/namespaces/dataset-ns/datasets/preview",
        test.origin
    );
    response(client.put(&root), 200).await;
    let data: Vec<u8> = (0..100_000).map(|i| u8::try_from(i % 251).unwrap()).collect();
    let upload = upload(&client, &root, &data).await;
    let publication = json!({"operation":"12121212121212121212121212121212","parent":null,"manifest":{"schema":[{"name":"value","required":true},{"name":"missing","required":false},{"name":"deleted","required":false}],"samples":[{"id":"c2FtcGxl","fields":{"value":{"kind":"payload","upload":upload},"deleted":{"kind":"tombstone"}}}]}});
    let published = response(
        client
            .post(format!("{root}/snapshots/publish"))
            .json(&publication),
        200,
    )
    .await;
    let url = format!(
        "{}/snapshots/{}/preview",
        root.replace("/v1/", "/v2/"),
        published["id"].as_str().unwrap()
    );
    let preview = response(
        client
            .get(&url)
            .query(&[("sample", "c2FtcGxl"), ("field", "value")]),
        200,
    )
    .await;
    assert_eq!(
        STANDARD.decode(preview["data"].as_str().unwrap()).unwrap(),
        data[..65536]
    );
    assert_eq!(preview["length"], "100000");
    assert_eq!(preview["truncated"], true);
    assert_eq!(preview["md5"], hex::encode(Md5::digest(&data)));
    for (field, state) in [("missing", "missing"), ("deleted", "tombstone")] {
        let p = response(
            client
                .get(&url)
                .query(&[("sample", "c2FtcGxl"), ("field", field)]),
            200,
        )
        .await;
        assert_eq!(p["state"], state);
        assert!(p["data"].is_null());
    }
    drop(client);
    test.stop().await;
}
async fn upload(client: &Client, root: &str, data: &[u8]) -> String {
    let id = "13131313131313131313131313131313";
    let url = format!("{root}/uploads");
    response(
        client.post(&url).json(
            &json!({"id":id,"length":data.len(),"part_size":32768,"md5":hex::encode(Md5::digest(data))}),
        ),
        200,
    )
    .await;
    for (index, part) in data.chunks(32768).enumerate() {
        response(
            client
                .put(format!("{url}/{id}/parts/{index}"))
                .header("content-type", "application/octet-stream")
                .body(part.to_vec()),
            200,
        )
        .await;
    }
    response(client.post(format!("{url}/{id}/complete")), 200).await;
    id.into()
}
