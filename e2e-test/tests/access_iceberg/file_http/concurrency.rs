// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::signed_file::TestFileClient;
use super::setup::{path, setup};
use reqwest::{Method, StatusCode};
use std::time::Duration;

const MAX_SLOWDOWN_ATTEMPTS: usize = 4;
const SLOWDOWN_BASE_DELAY_MS: u64 = 100;
const SLOWDOWN_STAGGER_MS: u64 = 25;

async fn put_with_slowdown_retry(
    client: TestFileClient,
    object: String,
    payload: Vec<u8>,
    index: usize,
) -> (StatusCode, String, usize) {
    for attempt in 1..=MAX_SLOWDOWN_ATTEMPTS {
        let response = client.send(Method::PUT, &object, "", &payload, false).await;
        let status = response.status();
        let body = response.text().await.unwrap();
        if status != StatusCode::SERVICE_UNAVAILABLE
            || !body.contains("<Code>SlowDown</Code>")
            || attempt == MAX_SLOWDOWN_ATTEMPTS
        {
            return (status, body, attempt);
        }

        let delay_ms =
            SLOWDOWN_BASE_DELAY_MS * (1 << (attempt - 1)) + (index as u64 % 8) * SLOWDOWN_STAGGER_MS;
        eprintln!("upload {index}: transient SlowDown on attempt {attempt}; retrying in {delay_ms}ms");
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }

    unreachable!("the retry loop always returns on its final attempt")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn thirty_two_concurrent_direct_puts() {
    let (_stack, _process, client, table) = setup().await;
    let payload = vec![0x5a; 8 * 1024 * 1024];
    // Prime one large writer before releasing the synchronized burst. The
    // first foreground allocation also settles the ChunkDB/DiskIO route after
    // the listener becomes reachable, so the burst measures concurrent writes
    // instead of startup discovery.
    let warmup = path(table, "data/parallel-warmup.parquet");
    let response = client.send(Method::PUT, &warmup, "", &payload, false).await;
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let mut requests = tokio::task::JoinSet::new();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(33));
    for index in 0..32 {
        let object = path(table, &format!("data/parallel-{index}.parquet"));
        let payload = payload.clone();
        let client = client.clone();
        let barrier = barrier.clone();
        requests.spawn(async move {
            barrier.wait().await;
            let (status, body, attempts) = put_with_slowdown_retry(client, object, payload, index).await;
            (index, status, body, attempts)
        });
    }
    barrier.wait().await;
    let mut failures = Vec::new();
    while let Some(result) = requests.join_next().await {
        let (index, status, body, attempts) = result.unwrap();
        println!("upload {index}: {status} attempts={attempts} {body}");
        if status != StatusCode::OK {
            failures.push((index, status, attempts, body));
        }
    }
    assert!(failures.is_empty(), "failed uploads: {failures:?}");
}
