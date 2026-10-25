// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{native::TestNativeStorage, signed_file::TestFileClient};
use super::setup::{fixture_config, path, setup};
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds},
    file::{FileRepository, TableLocation},
};
use crowdb_protocol::frame::MAX_FRAME_V2_PAYLOAD_BYTES;
use futures::StreamExt;
use reqwest::Method;
use std::time::Instant;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release storage fixture"]
async fn small_file_profiles() {
    let (_stack, _process, client, table) = setup().await;
    for (label, size) in [
        ("1KiB", 1024),
        ("512KiB", 512 * 1024),
        ("aligned64KiB", MAX_FRAME_V2_PAYLOAD_BYTES),
        ("over64KiB", MAX_FRAME_V2_PAYLOAD_BYTES + 1),
    ] {
        assert!(size < fixture_config().iceberg_small_write().threshold_exclusive());
        let bytes = vec![0x5a; size];
        let warmup = path(table, &format!("data/small-{label}-warmup.bin"));
        let put = client.send(Method::PUT, &warmup, "", &bytes, true).await;
        assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
        for concurrency in [1, 32] {
            let requests = 128;
            if concurrency == 32 {
                warm_small_routes(&client, table, &bytes, label).await;
            }
            let stages_before = small_stage_counts(&client).await;
            let before = catalog_counts(&client).await;
            let route_before = upload_route_counts(&client).await;
            let receive_before = receive_counts(&client).await;
            let started = Instant::now();
            let client_ref = &client;
            let payload = &bytes;
            let mut results = futures::stream::iter((0..requests).map(|index| {
                let object = path(table, &format!("data/small-{label}-{concurrency}-{index}.bin"));
                async move {
                    let started = Instant::now();
                    let put = client_ref.send(Method::PUT, &object, "", payload, true).await;
                    assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
                    (object, started.elapsed().as_micros())
                }
            }))
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
            let elapsed = started.elapsed();
            let route_after = upload_route_counts(&client).await;
            let stages_after = small_stage_counts(&client).await;
            println!("iceberg small stages {label} concurrency={concurrency} reservation_ns={} prepare_ns={} finish_ns={} publication_ns={} batches={} aggregate_writes={} active_pipelines={} scale_out={}",
                stages_after[0] - stages_before[0], stages_after[1] - stages_before[1],
                stages_after[2] - stages_before[2], stages_after[3] - stages_before[3],
                stages_after[4] - stages_before[4], stages_after[5] - stages_before[5],
                stages_after[6], stages_after[7] - stages_before[7]);
            assert_eq!(route_after.0 - route_before.0, requests as u64);
            assert_eq!(
                route_after.1 - route_before.1,
                0,
                "small object wrote large strips"
            );
            assert_eq!(
                route_after.2 - route_before.2,
                requests as u64,
                "object split into multiple handoffs"
            );
            let receive_after = receive_counts(&client).await;
            let frames = receive_after.0 - receive_before.0;
            assert_eq!(
                frames,
                (requests * size.div_ceil(MAX_FRAME_V2_PAYLOAD_BYTES)) as u64
            );
            assert_eq!(receive_after.1 - receive_before.1, requests as u64);
            println!("iceberg small native {label} concurrency={concurrency} prepared_frames={frames} receive_allocations={} direct_bytes={} prefix_copy_bytes={}",
                receive_after.1 - receive_before.1, receive_after.2 - receive_before.2,
                receive_after.3 - receive_before.3);
            results.sort_by_key(|(_, latency)| *latency);
            println!("iceberg small {label} concurrency={concurrency} requests={requests} small_completions={} large_strips={} writer_feeds={} frames={} elapsed_us={} p50_us={} p95_us={} p99_us={} {}",
                route_after.0 - route_before.0, route_after.1 - route_before.1, route_after.2 - route_before.2,
                size.div_ceil(MAX_FRAME_V2_PAYLOAD_BYTES), elapsed.as_micros(), results[(requests - 1) / 2].1,
                results[(requests * 95).div_ceil(100) - 1].1,
                results[(requests * 99).div_ceil(100) - 1].1,
                catalog_delta(before, catalog_counts(&client).await));
            for (object, _) in results {
                let get = client.send(Method::GET, &object, "", b"", false).await;
                assert_eq!(get.status(), 200);
                assert_eq!(get.bytes().await.unwrap().as_ref(), bytes);
            }
        }
    }
}

pub(super) async fn assert_one_file_chunk(stack: &TestNativeStorage, table: TableLocation) {
    let store = stack.store().await;
    let context = CatalogRepository::new(store.clone(), ClearBounds::default())
        .unwrap()
        .status()
        .await
        .unwrap()
        .0
        .context;
    let published = FileRepository::new(store)
        .load(context, &table.file("data/profile-put.bin").unwrap())
        .await
        .unwrap()
        .unwrap();
    let chunks = published.content.locations(published.length).unwrap().unwrap();
    assert_eq!(chunks.len(), 1, "5MiB file should fit one configured chunk");
    println!("iceberg 5MiB PUT chunks={}", chunks.len());
}

async fn receive_counts(client: &TestFileClient) -> (u64, u64, u64, u64) {
    let metrics: serde_json::Value = client
        .client
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        metrics["upload_flow"]["frames_prepared"].as_u64().unwrap(),
        metrics["native_receive"]["allocations"].as_u64().unwrap(),
        metrics["native_receive"]["direct_bytes"].as_u64().unwrap(),
        metrics["native_receive"]["prefix_copy_bytes"].as_u64().unwrap(),
    )
}

async fn warm_small_routes(client: &TestFileClient, table: TableLocation, bytes: &[u8], label: &str) {
    futures::stream::iter(0..128)
        .map(|index| async move {
            let object = path(table, &format!("data/small-{label}-route-warmup-{index}.bin"));
            let put = client.send(Method::PUT, &object, "", bytes, true).await;
            assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;
}

async fn small_stage_counts(client: &TestFileClient) -> [u64; 8] {
    let metrics: serde_json::Value = client
        .client
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let small = &metrics["chunk_small_write"];
    let upload = &metrics["upload_flow"];
    [
        small["reservation_wait_ns"].as_u64().unwrap(),
        upload["strip_prepare_wait_ns"].as_u64().unwrap(),
        upload["writer_finish_ns"].as_u64().unwrap(),
        upload["publication_ns"].as_u64().unwrap(),
        small["batches"].as_u64().unwrap(),
        small["aggregate_write_requests"].as_u64().unwrap(),
        small["active_pipelines"].as_u64().unwrap(),
        small["scale_out"].as_u64().unwrap(),
    ]
}

async fn catalog_counts(client: &TestFileClient) -> (u64, u64, u64, u64) {
    let response: serde_json::Value = client
        .client
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let catalog = &response["catalog"];
    (
        catalog["get"].as_u64().unwrap(),
        catalog["compare_exchange"].as_u64().unwrap(),
        catalog["scan"].as_u64().unwrap(),
        catalog["conditional_delete"].as_u64().unwrap(),
    )
}

fn catalog_delta(before: (u64, u64, u64, u64), after: (u64, u64, u64, u64)) -> String {
    format!(
        "get={} cas={} scan={} delete={}",
        after.0 - before.0,
        after.1 - before.1,
        after.2 - before.2,
        after.3 - before.3
    )
}

async fn upload_route_counts(client: &TestFileClient) -> (u64, u64, u64) {
    let metrics: serde_json::Value = client
        .client
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        metrics["chunk_small_write"]["completed"].as_u64().unwrap(),
        metrics["upload_flow"]["strip_write_successes"].as_u64().unwrap(),
        metrics["upload_flow"]["writer_feeds"].as_u64().unwrap(),
    )
}

mod large;
