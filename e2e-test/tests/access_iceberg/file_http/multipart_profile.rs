// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::signed_file::TestFileClient;
use super::setup::{path, setup_with_bounds_and_file_limit};
use crowdb_access_iceberg::catalog::ClearBounds;
use futures::StreamExt;
use md5::{Digest, Md5};
use reqwest::{Client, Method};
use std::{fmt::Write as _, time::Instant};
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual 100 MiB multipart upload profile"]
async fn repeated_100_mib_multipart_upload_profile() {
    const BLOCK_BYTES: usize = 1024 * 1024;
    const PART_COUNT: usize = 13;
    let (_stack, _process, client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            request_ms: 120_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        128 * 1024 * 1024,
        128 * 1024 * 1024,
    )
    .await;
    let block = bytes::Bytes::from(vec![37; BLOCK_BYTES]);
    let digests = [8, 4].map(|repetitions| {
        let mut md5 = Md5::new();
        for _ in 0..repetitions {
            md5.update(&block);
        }
        <[u8; 16]>::from(md5.finalize())
    });
    let object = path(table, "data/repeated-100-mib-multipart.parquet");
    let upload_started = Instant::now();
    let created = client.send(Method::POST, &object, "uploads=", b"", false).await;
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    let created_body = created.text().await.unwrap();
    let upload = created_body
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let create_elapsed = upload_started.elapsed();
    let part_started = Instant::now();
    let parts = futures::stream::iter(1..=PART_COUNT)
        .map(|number| {
            let block = block.clone();
            let query = format!("partNumber={number}&uploadId={upload}");
            let object = object.clone();
            let repetitions = if number == PART_COUNT { 4 } else { 8 };
            let digest = if repetitions == 4 { digests[1] } else { digests[0] };
            let client = &client;
            async move {
                let started = Instant::now();
                let response = client
                    .send_repeated_with_query(Method::PUT, &object, &query, block, repetitions, digest)
                    .await;
                assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
                let etag = response.headers()["etag"].to_str().unwrap().to_owned();
                (number, etag, started.elapsed())
            }
        })
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
    let parts_elapsed = part_started.elapsed();
    let mut parts = parts;
    parts.sort_unstable_by_key(|part| part.0);
    let mut manifest = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    for (number, etag, _) in &parts {
        write!(
            manifest,
            "<Part><ETag>{etag}</ETag><PartNumber>{number}</PartNumber></Part>"
        )
        .unwrap();
    }
    manifest.push_str("</CompleteMultipartUpload>");
    let complete_started = Instant::now();
    let completed = client
        .send(
            Method::POST,
            &object,
            &format!("uploadId={upload}"),
            manifest.as_bytes(),
            false,
        )
        .await;
    assert_eq!(completed.status(), 200, "{}", completed.text().await.unwrap());
    let completed_body = completed.text().await.unwrap();
    assert!(
        completed_body.contains("</CompleteMultipartUploadResult>"),
        "{completed_body}"
    );
    let complete_elapsed = complete_started.elapsed();
    let last = format!("bytes={}-{}", 100 * BLOCK_BYTES - 128, 100 * BLOCK_BYTES - 1);
    let range = client
        .send_range(Method::GET, &object, "", b"", false, Some(&last))
        .await;
    assert_eq!(range.status(), 206);
    assert_eq!(range.bytes().await.unwrap().as_ref(), &[37; 128]);
    println!(
        "iceberg 100 MiB multipart create={create_elapsed:?} parts={parts_elapsed:?} complete={complete_elapsed:?} slowest_part={:?}",
        parts.iter().map(|part| part.2).max().unwrap()
    );
    assert_multipart_metrics(&client, PART_COUNT, 100 * BLOCK_BYTES).await;
}

pub(super) async fn assert_multipart_metrics(
    client: &TestFileClient,
    part_count: usize,
    logical_bytes: usize,
) {
    let metrics: serde_json::Value = Client::new()
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    println!("iceberg upload flow metrics={}", metrics["upload_flow"]);
    assert_eq!(metrics["upload_flow"]["attempts"], part_count);
    assert_eq!(metrics["upload_flow"]["completed"], part_count);
    assert_eq!(metrics["upload_flow"]["logical_bytes"], logical_bytes);
    assert_eq!(metrics["upload_flow"]["multipart_completions"], 1);
}
