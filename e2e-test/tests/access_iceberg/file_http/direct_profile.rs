// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::setup::{path, setup_with_bounds_and_file_limit};
use crowdb_access_iceberg::catalog::ClearBounds;
use md5::{Digest, Md5};
use reqwest::{Client, Method};
use std::time::Instant;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual 100 MiB upload profile"]
async fn repeated_100_mib_put_streams_without_client_payload_copy() {
    const BLOCK_BYTES: usize = 1024 * 1024;
    const BLOCK_COUNT: usize = 100;
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
    let mut md5 = Md5::new();
    for _ in 0..BLOCK_COUNT {
        md5.update(&block);
    }
    let digest: [u8; 16] = md5.finalize().into();
    let object = path(table, "data/repeated-100-mib.parquet");
    let started = Instant::now();
    let response = client
        .send_repeated(Method::PUT, &object, block.clone(), BLOCK_COUNT, digest)
        .await;
    let put_elapsed = started.elapsed();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let metrics: serde_json::Value = Client::new()
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let upload_flow = &metrics["upload_flow"];
    assert_eq!(upload_flow["attempts"], 1);
    assert_eq!(upload_flow["completed"], 1);
    assert_eq!(upload_flow["logical_bytes"], BLOCK_BYTES * BLOCK_COUNT);
    assert!(upload_flow["strip_write_successes"].as_u64().unwrap() > 0);
    assert!(upload_flow["strip_write_success_ns"].as_u64().unwrap() > 0);
    let head = client.send(Method::HEAD, &object, "", b"", false).await;
    assert_eq!(head.status(), 200);
    assert_eq!(
        head.headers()["content-length"],
        (BLOCK_BYTES * BLOCK_COUNT).to_string()
    );
    let last = format!(
        "bytes={}-{}",
        BLOCK_BYTES * BLOCK_COUNT - 128,
        BLOCK_BYTES * BLOCK_COUNT - 1
    );
    let range = client
        .send_range(Method::GET, &object, "", b"", false, Some(&last))
        .await;
    assert_eq!(range.status(), 206);
    assert_eq!(range.bytes().await.unwrap().as_ref(), &[37; 128]);
    let invalid = path(table, "data/repeated-invalid-md5.parquet");
    let rejected = client
        .send_repeated(Method::PUT, &invalid, block, 1, [0; 16])
        .await;
    assert_eq!(rejected.status(), 400);
    assert!(rejected.text().await.unwrap().contains("BadDigest"));
    let absent = client.send(Method::HEAD, &invalid, "", b"", false).await;
    assert_eq!(absent.status(), 404);
    let sha_object = path(table, "data/signed-sha256.parquet");
    let sha = client
        .send(Method::PUT, &sha_object, "", b"sha256 payload", false)
        .await;
    assert_eq!(sha.status(), 200, "{}", sha.text().await.unwrap());
    println!("iceberg repeated 100 MiB PUT={put_elapsed:?}");
    println!("iceberg direct upload flow metrics={upload_flow}");
}
