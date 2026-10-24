// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset.rs"]
mod dataset;

use common::response;
use dataset::{TestDatasetHttp, TestStore};
use md5::{Digest, Md5};
use reqwest::Client;
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn multipart_fields_use_one_upload_read_each_and_no_eof_query() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start_payload(store.clone()).await;
    let client = Client::new();
    let root = format!(
        "{}/api/dataset/v1/namespaces/dataset-ns/datasets/cost",
        test.origin
    );
    response(client.put(&root), 200).await;
    let upload = "11111111111111111111111111111111";
    let data = b"abcdefghij";
    response(
        client.post(format!("{root}/uploads")).json(&json!({
            "id":upload,"length":10,"part_size":4,"md5":hex::encode(Md5::digest(data))
        })),
        200,
    )
    .await;
    for (index, part) in data.chunks(4).enumerate() {
        response(
            client
                .put(format!("{root}/uploads/{upload}/parts/{index}"))
                .header("content-type", "application/octet-stream")
                .body(part.to_vec()),
            200,
        )
        .await;
    }
    response(client.post(format!("{root}/uploads/{upload}/complete")), 200).await;
    let published = response(
        client.post(format!("{root}/snapshots/publish")).json(&json!({
            "operation":"22222222222222222222222222222222", "parent":null,
            "manifest":{"schema":[{"name":"value","required":true}],"samples":[
                {"id":"YQ==","fields":{"value":{"kind":"payload","upload":upload}}},
                {"id":"Yg==","fields":{"value":{"kind":"payload","upload":upload}}}
            ]}
        })),
        200,
    )
    .await;
    let reads = store.upload_reads.load(Ordering::Relaxed);
    let writes = store.mutations.load(Ordering::Relaxed);
    let reply = client
        .post(format!("{root}/reads"))
        .bearer_auth("secret")
        .json(&json!({
            "snapshot":published["id"],"sample_ids":["YQ==","Yg=="],"fields":["value"]
        }))
        .send()
        .await
        .unwrap();
    let status = reply.status();
    let bytes = reply.bytes().await.unwrap();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
    assert_fields(&bytes, &[data.as_slice(), data.as_slice()]);
    assert_eq!(store.upload_reads.load(Ordering::Relaxed) - reads, 8);
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    drop(client);
    test.stop().await;
}

fn assert_fields(mut bytes: &[u8], expected: &[&[u8]]) {
    assert_eq!(&bytes[..8], b"CDBDS001");
    bytes = &bytes[8..];
    let mut field = Vec::new();
    let mut index = 0;
    let mut finished = false;
    while !bytes.is_empty() {
        assert!(!finished);
        let kind = bytes[0];
        let length = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
        let body = &bytes[5..5 + length];
        bytes = &bytes[5 + length..];
        match kind {
            1 => {
                let _: serde_json::Value = serde_json::from_slice(body).unwrap();
            }
            2 => {
                assert_eq!(&body[..16], Md5::digest(&body[16..]).as_slice());
                field.extend_from_slice(&body[16..]);
            }
            3 => {
                assert_eq!(field, expected[index]);
                index += 1;
                field.clear();
            }
            0 => {
                assert_eq!(index, expected.len());
                assert!(field.is_empty());
                finished = true;
            }
            _ => panic!("unexpected frame {kind}: {}", String::from_utf8_lossy(body)),
        }
    }
    assert!(finished);
}
