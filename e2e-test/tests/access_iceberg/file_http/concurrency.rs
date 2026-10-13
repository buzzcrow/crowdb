// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::setup::{path, setup};
use reqwest::Method;
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn thirty_two_concurrent_direct_puts() {
    let (_stack, _process, client, table) = setup().await;
    let payload = vec![0x5a; 8 * 1024 * 1024];
    let mut requests = tokio::task::JoinSet::new();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(33));
    for index in 0..32 {
        let object = path(table, &format!("data/parallel-{index}.parquet"));
        let request = client.request(Method::PUT, &object, "", &payload, false, None);
        let barrier = barrier.clone();
        requests.spawn(async move {
            barrier.wait().await;
            let response = request.send().await.unwrap();
            (index, response.status(), response.text().await.unwrap())
        });
    }
    barrier.wait().await;
    let mut failures = Vec::new();
    while let Some(result) = requests.join_next().await {
        let (index, status, body) = result.unwrap();
        println!("upload {index}: {status} {body}");
        if status != 200 {
            failures.push((index, status, body));
        }
    }
    assert!(failures.is_empty(), "failed uploads: {failures:?}");
}
