use super::{catalog_counts, catalog_delta, fixture_config, path, setup, upload_route_counts};
use super::{
    CatalogRepository, ClearBounds, Client, FileRepository, Method, TableLocation, TestFileClient,
    TestIcebergStack,
};
use futures::StreamExt;
use std::time::Instant;

pub(super) async fn small_file_profiles() {
    let (_stack, _process, client, table) = setup().await;
    for (label, size) in [("1KiB", 1024), ("512KiB", 512 * 1024), ("aligned64KiB", 65502)] {
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
            assert_eq!(frames, (requests * size.div_ceil(65502)) as u64);
            assert_eq!(receive_after.1 - receive_before.1, requests as u64);
            println!("iceberg small native {label} concurrency={concurrency} prepared_frames={frames} receive_allocations={} direct_bytes={} prefix_copy_bytes={}",
                receive_after.1 - receive_before.1, receive_after.2 - receive_before.2,
                receive_after.3 - receive_before.3);
            results.sort_by_key(|(_, latency)| *latency);
            println!("iceberg small {label} concurrency={concurrency} requests={requests} small_completions={} large_strips={} writer_feeds={} frames={} elapsed_us={} p50_us={} p95_us={} p99_us={} {}",
                route_after.0 - route_before.0, route_after.1 - route_before.1, route_after.2 - route_before.2,
                size.div_ceil(65502), elapsed.as_micros(), results[(requests - 1) / 2].1,
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

pub(super) async fn assert_one_file_chunk(stack: &TestIcebergStack, table: TableLocation) {
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
