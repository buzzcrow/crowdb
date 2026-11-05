// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::setup::{path, setup_with_bounds_and_file_limit};
use crowdb_access_iceberg::{catalog::ClearBounds, file::FileRepository};
use crowdb_protocol::chunkdb::rpc::{QueryChunkRequest, Strip};
use md5::{Digest, Md5};
use reqwest::{Client, Method};
use std::time::Instant;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn ordinary_put_size_matrix_streams_and_reads_ranges() {
    let (stack, _process, client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            request_ms: 120_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        128 * 1024 * 1024,
        128 * 1024 * 1024,
    )
    .await;
    let repository = FileRepository::new(stack.store().await);
    for size in [10 * 1024, 1024 * 1024, 12 * 1024 * 1024, 100 * 1024 * 1024] {
        let key = format!("data/size-{size}.parquet");
        let object = path(table, &key);
        let bytes = (0..size)
            .map(|offset| u8::try_from(offset % 256).unwrap())
            .collect::<Vec<_>>();
        let expected = Md5::digest(&bytes);
        let started = Instant::now();
        let put = client.send(Method::PUT, &object, "", &bytes, true).await;
        assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
        let put_elapsed = started.elapsed();
        let head = client.send(Method::HEAD, &object, "", b"", false).await;
        assert_eq!(head.status(), 200);
        assert_eq!(head.headers()["content-length"], size.to_string());
        let record = repository
            .load(client.credentials.grant().context, &table.file(&key).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.length, size as u64);
        let locations = record.content.locations(record.length).unwrap().unwrap();
        assert!(!locations.is_empty());
        if size == 12 * 1024 * 1024 {
            let chunkdb = crowdb_chunkdb_client::ChunkdbClient::new(
                crowdb_paxos_kv_client::ServiceRegistryClient::new(
                    crowdb_paxos_kv_client::PaxosKvClient::new(crowdb_paxos_kv_client::ClientConfig::new(
                        stack.seeds.clone(),
                    )),
                ),
                std::sync::Arc::new(crowdb_chunkdb_client::ChunkdbRpcTransport::new()),
            );
            let chunk = chunkdb
                .query_chunk(QueryChunkRequest {
                    chunk_id: locations[0].chunk_id,
                })
                .await
                .unwrap()
                .chunk
                .unwrap();
            let expected_copies = super::setup::fixture_config()
                .iceberg
                .large_mirror_copies
                .unwrap();
            assert!(chunk.strips.iter().any(|strip| strip.sealed_length > 0));
            for strip in &chunk.strips {
                let Some(Strip::MirrorStrip(mirror)) = strip.strip.as_ref() else {
                    panic!("protected large Iceberg write has a mirror strip");
                };
                assert_eq!(mirror.segments.len(), usize::try_from(expected_copies).unwrap());
            }
        }
        let get_started = Instant::now();
        let mut response = client.send(Method::GET, &object, "", b"", false).await;
        assert_eq!(response.status(), 200);
        let mut received = Md5::new();
        let mut received_size = 0;
        while let Some(chunk) = response.chunk().await.unwrap() {
            received.update(&chunk);
            received_size += chunk.len();
        }
        assert_eq!(received_size, size);
        assert_eq!(received.finalize().as_slice(), expected.as_slice());
        let get_elapsed = get_started.elapsed();
        for start in [0, usize::min(size - 1, 65500), size - 1] {
            let end = usize::min(size - 1, start + 127);
            let range = format!("bytes={start}-{end}");
            let response = client
                .send_range(Method::GET, &object, "", b"", false, Some(&range))
                .await;
            assert_eq!(response.status(), 206);
            assert_eq!(response.bytes().await.unwrap().as_ref(), &bytes[start..=end]);
        }
        println!("iceberg ordinary size={size} PUT={put_elapsed:?} GET={get_elapsed:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn slow_socket_resumes_upload_after_writer_drains() {
    const BLOCK_BYTES: usize = 1024 * 1024;
    let (_stack, _process, client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            request_ms: 120_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        16 * 1024 * 1024,
        16 * 1024 * 1024,
    )
    .await;
    let block = bytes::Bytes::from(vec![41; BLOCK_BYTES]);
    let mut md5 = Md5::new();
    md5.update(&block);
    md5.update(&block);
    let digest: [u8; 16] = md5.finalize().into();
    let object = path(table, "data/slow-socket.parquet");
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let request = client.request_stream(Method::PUT, &object, 2 * BLOCK_BYTES, digest, receiver);
    let upload = tokio::spawn(async move { request.send().await.unwrap() });
    sender.send(Ok(block.clone())).await.unwrap();
    // Let the first strip finish so no disk task remains to wake the upload.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    sender.send(Ok(block)).await.unwrap();
    drop(sender);
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), upload)
        .await
        .expect("socket readiness must resume the idle upload")
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let get = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().len(), 2 * BLOCK_BYTES);
    let metrics: serde_json::Value = Client::new()
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(metrics["upload_flow"]["body_waits"].as_u64().unwrap() > 0);
    assert!(metrics["upload_flow"]["body_wait_ns"].as_u64().unwrap() > 0);
}
