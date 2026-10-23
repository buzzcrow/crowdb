// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/rdma_datapath/allocator.rs"]
mod allocator;
#[path = "common/rdma_datapath/http.rs"]
mod http;

use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_client::{ChunkIoClient, RoutedDiskWriter};
use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, TestDiskRoute};
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_rpc_ffi::{RdmaConfig, RdmaWireLayout};
use futures::StreamExt;
use sha2::{Digest, Sha256};

fn test_rdma_config(enabled: bool) -> RdmaConfig {
    RdmaConfig {
        enabled,
        allow_tcp_fallback: false,
        listen_enabled: false,
        wire_layout: RdmaWireLayout::ControlData,
        wire_slot_capacity: (1 << 20) + 1024,
        sq_depth: 4,
        send_batch_size: std::env::var("CROWDB_RDMA_SEND_BATCH_SIZE")
            .map_or(1, |value| value.parse().unwrap()),
        receive_batch_size: std::env::var("CROWDB_RDMA_RECEIVE_BATCH_SIZE")
            .map_or(1, |value| value.parse().unwrap()),
        aggregation_enabled: std::env::var("CROWDB_RDMA_AGGREGATION_ENABLED").as_deref() == Ok("1"),
        rq_depth: 4,
        posted_receive_target: 4,
        cq_depth: 16,
        send_arena_bytes: 4 << 20,
        receive_arena_bytes: 4 * ((1 << 20) + 1024),
        device: std::env::var("CROWDB_RDMA_DEVICE").unwrap_or_default(),
        source_address: std::env::var("CROWDB_RDMA_SOURCE_ADDR").unwrap_or_default(),
        ..RdmaConfig::default()
    }
}

fn test_client() -> Arc<DiskioClient> {
    let endpoint = std::env::var("CROWDB_RDMA_DATAPATH_ENDPOINT").unwrap();
    let enabled = std::env::var("CROWDB_RDMA_DATAPATH_TRANSPORT").unwrap() == "rdma";
    Arc::new(
        DiskioClient::connect_for_tests(
            (1..=2)
                .map(|high| TestDiskRoute {
                    disk_id: DiskId::new(high, 0),
                    rack_id: 1,
                    node_id: 1,
                    disk_group_id: 1,
                    instance_id: 1,
                    endpoint: endpoint.clone(),
                })
                .collect(),
            DiskioClientConfig {
                rdma: Some(test_rdma_config(enabled)),
                normal_connections_per_endpoint: 1,
                priority_connections_per_endpoint: 1,
                rpc_workers: 1,
                ..DiskioClientConfig::default()
            },
        )
        .unwrap(),
    )
}

async fn test_download(http: &reqwest::Client, url: &str, bytes: &[u8]) -> (usize, Option<String>, bool) {
    let get = http.get(url).send().await.unwrap();
    assert_eq!(get.status(), 200);
    let fault = std::env::var("CROWDB_RDMA_DATAPATH_FAULT").as_deref() == Ok("1");
    let mut download = get.bytes_stream();
    let mut downloaded = Vec::new();
    let mut download_error = None;
    while let Some(result) = download.next().await {
        match result {
            Ok(data) => downloaded.extend_from_slice(&data),
            Err(error) => {
                download_error = Some(error.to_string());
                break;
            }
        }
    }
    if fault {
        assert!(download_error.is_some());
        assert!(!downloaded.is_empty() && downloaded.len() < bytes.len());
        assert_eq!(downloaded, bytes[..downloaded.len()]);
    } else {
        assert!(download_error.is_none(), "{download_error:?}");
        assert_eq!(downloaded, bytes);
    }
    (downloaded.len(), download_error, fault)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an owned DiskIO process; run tools/rdma/run-datapath.py"]
async fn http_chunkio_round_trip_uses_selected_transport_and_exact_framed_bytes() {
    let client = test_client();
    let chunks = Arc::new(ChunkIoClient::from_parts(
        Arc::new(allocator::TestAllocator::default()),
        Arc::new(RoutedDiskWriter::from_client(Arc::clone(&client))),
        ChunkType::S3,
    ));
    let handler = Arc::new(http::TestHttp::new(Arc::clone(&chunks)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(crowdb_access_server::s3::serve(
        listener,
        handler.clone(),
        async {
            let _ = stopped.await;
        },
    ));
    let bytes: Vec<_> = (0..3 * (1 << 20) + 12_345)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let url = format!("http://{address}/datapath/object");
    let put = http
        .put(&url)
        .body(bytes.clone())
        .send()
        .await
        .unwrap_or_else(|error| {
            panic!(
                "HTTP PUT failed: {error}; allocator={:?}; transport={}",
                handler.allocator.metrics_snapshot(),
                String::from_utf8_lossy(&client.transport_status_json().unwrap())
            );
        });
    assert_eq!(put.status(), 200);
    let etag = put.headers()["etag"].to_str().unwrap().to_owned();
    let (downloaded_bytes, download_error, fault) = test_download(&http, &url, &bytes).await;
    let locations = handler.locations.load_full();
    assert!(
        locations.len() >= 2,
        "corpus must cross chunk and message boundaries"
    );
    let mut cursor = 0;
    for location in locations.iter() {
        assert_eq!(location.logical_offset, cursor);
        cursor += location.logical_length;
    }
    assert_eq!(cursor, u64::try_from(bytes.len()).unwrap());
    let status: serde_json::Value = serde_json::from_slice(&client.transport_status_json().unwrap()).unwrap();
    if std::env::var("CROWDB_RDMA_DATAPATH_TRANSPORT").unwrap() == "rdma" {
        let rdma = &status["rdma"];
        assert!(
            rdma["direct_payload_bytes"].as_u64().unwrap() >= 2 * cursor,
            "{rdma}"
        );
        for field in [
            "copied_payload_bytes",
            "fallback_count",
            "cq_errors",
            "protocol_errors",
            "send_errors",
            "receive_post_errors",
        ] {
            assert_eq!(rdma[field], 0, "{field}: {status}");
        }
    } else {
        assert!(status["rdma"].is_null());
    }
    assert_eq!(client.status().retries, 0);
    let metrics = handler.allocator.metrics_snapshot();
    assert!(metrics.peak_retained_bytes <= 2 << 20);
    assert!(metrics.allocations >= 4);
    let result = serde_json::json!({
        "effective_uid": std::fs::read_to_string("/proc/self/status").unwrap().lines()
            .find_map(|line| line.strip_prefix("Uid:")).unwrap().split_whitespace().nth(1).unwrap().parse::<u32>().unwrap(),
        "payload_bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(&bytes)),
        "etag": etag, "locations": *locations, "transport": status,
        "http_native": {"direct_bytes": metrics.direct_bytes, "prefix_copy_bytes": metrics.prefix_copy_bytes,
            "peak_retained_bytes": metrics.peak_retained_bytes, "allocations": metrics.allocations,
            "backpressure_events": metrics.backpressure_events},
        "errors": u64::from(fault), "expected_errors": u64::from(fault), "verification_errors": 0,
        "downloaded_bytes": downloaded_bytes, "download_error": download_error,
        "topology": std::env::var("CROWDB_RDMA_DATAPATH_TOPOLOGY").unwrap(),
    });
    std::fs::write(
        std::env::var("CROWDB_RDMA_DATAPATH_RESULT").unwrap(),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
    chunks.shutdown_small_writes().await.unwrap();
}
