// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::setup::{path, setup};
use reqwest::{Client, Method};
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn small_routing_is_strict_at_the_strip_threshold() {
    let (_stack, _process, client, table) = setup().await;
    let threshold = super::setup::fixture_config()
        .iceberg_small_write()
        .threshold_exclusive();
    let completed = async || {
        let metrics: serde_json::Value = Client::new()
            .get(format!("http://{}/_crowdb/metrics", client.address))
            .bearer_auth("m".repeat(32))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        metrics["chunk_small_write"]["completed"].as_u64().unwrap()
    };
    let mut previous = completed().await;
    for size in [threshold - 1, threshold, threshold + 1] {
        let object = path(table, &format!("data/threshold-{size}.parquet"));
        let bytes = vec![u8::try_from(size % 251).unwrap(); size];
        let response = client.send(Method::PUT, &object, "", &bytes, true).await;
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
        let current = completed().await;
        assert_eq!(current - previous, u64::from(size < threshold), "size={size}");
        previous = current;
        let last = format!("bytes={}-{}", size - 2, size - 1);
        let response = client
            .send_range(Method::GET, &object, "", b"", false, Some(&last))
            .await;
        assert_eq!(response.status(), 206);
        assert_eq!(response.bytes().await.unwrap().as_ref(), &bytes[size - 2..]);
    }
}
