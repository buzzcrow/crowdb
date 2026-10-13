// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, SmallWritePolicy};
use crowdb_chunk_stream::StreamRegistry;
use crowdb_chunk_stream::{
    ProductionStreamRuntime, StreamBinding, StreamBindingState, StreamConfig, StreamError, StreamName,
};
use std::time::{Duration, Instant};

fn witness() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var("CROWDB_E2E_CONTROL").unwrap())
        .parent()
        .unwrap()
        .join("stream-witness.json")
}

async fn connect_chunk_io() -> ChunkIoClient {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match ChunkIoClient::connect(ChunkIoClientConfig {
            management_seeds: crate::common::management::addresses(11)[..3]
                .iter()
                .map(|ip| crate::common::management::origin(ip))
                .collect(),
            diskio_connections_per_endpoint: 2,
            diskio_rpc_workers: 1,
            small_write: SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::Stream),
        })
        .await
        {
            Ok(client) => return client,
            Err(error) => assert!(Instant::now() < deadline, "chunk services unavailable: {error}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires the owned release container fixture"]
async fn production_idle_writer_stops_after_ownership_takeover() {
    let io = connect_chunk_io().await;
    let config = StreamConfig {
        liveness_interval: Duration::from_millis(50),
        ..StreamConfig::default()
    };
    let old_runtime = ProductionStreamRuntime::new(
        crate::common::hardware::kv_client().into(),
        &io,
        30_000,
        ChunkReadPolicy::default(),
        config,
    )
    .unwrap();
    let name = StreamName { high: 42, low: 142 };
    old_runtime
        .registry()
        .create(StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("idle-authority-e2e".into()),
        })
        .await
        .unwrap();
    let old = old_runtime.create_registered(name, 0, 1).await.unwrap();
    let before = old.append(&[Bytes::from_static(b"old")]).await.unwrap();
    let new_runtime = ProductionStreamRuntime::new(
        crate::common::hardware::kv_client().into(),
        &io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .unwrap();
    let new = new_runtime.open(name, 0, 2).await.unwrap();
    let head = new.observe_metadata(None, 0).unwrap();
    // Observe three real idle periods without invoking an old-writer mutation.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(old.metrics().rollovers, 0);
    assert_eq!(
        old.append(&[Bytes::from_static(b"stale")]).await,
        Err(StreamError::WriteStalled)
    );
    let reader = new_runtime.open_read_only(name, 0, 2).await.unwrap();
    let observed = reader.observe_metadata(None, 0).unwrap();
    assert_eq!(observed.generation, head.generation);
    assert_eq!(observed.active, head.active);
    let after = new.append(&[Bytes::from_static(b"new")]).await.unwrap();
    assert_ne!(before.chunk_id, after.chunk_id);
    assert_eq!(new.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
}

#[tokio::test]
#[ignore = "requires the owned release container fixture"]
async fn production_stream_recovers_exact_bytes_after_service_restarts() {
    let first_io = connect_chunk_io().await;
    let first_runtime = ProductionStreamRuntime::new(
        crate::common::hardware::kv_client().into(),
        &first_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .expect("assemble initial runtime");
    let stream_name = StreamName { high: 42, low: 141 };
    if std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write") {
        first_runtime
            .registry()
            .create(StreamBinding {
                purpose: crowdb_protocol::chunk_stream::StreamPurpose::Stream,
                stream_name,
                metadata_group_id: 1,
                binding_generation: 1,
                state: StreamBindingState::Active,
                owner_kind: Some("restart-e2e".into()),
            })
            .await
            .expect("publish stream binding");

        let first_stream = first_runtime
            .create_registered(stream_name, 0, 1)
            .await
            .expect("create stream");
        let first = first_stream
            .append(&[Bytes::from_static(b"before-restart|")])
            .await
            .expect("append before restart");
        let abandoned_chunk = first.chunk_id.expect("first append chunk identity");
        std::fs::write(witness(), serde_json::to_vec(&abandoned_chunk).unwrap()).unwrap();
        assert_eq!(
            first_stream.read_at(0, 15).await.expect("read before restart"),
            Bytes::from_static(b"before-restart|")
        );
        drop(first_stream);
        drop(first_runtime);
        drop(first_io);

        return;
    }
    let abandoned_chunk = serde_json::from_slice(&std::fs::read(witness()).unwrap()).unwrap();
    let second_io = connect_chunk_io().await;
    let second_runtime = ProductionStreamRuntime::new(
        crate::common::hardware::kv_client().into(),
        &second_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .expect("assemble recovered runtime");
    let second_stream = second_runtime
        .open(stream_name, 0, 2)
        .await
        .expect("recover authoritative stream head");
    let second = second_stream
        .append(&[Bytes::from_static(b"after-restart")])
        .await
        .expect("append after restart");
    assert_ne!(
        second.chunk_id,
        Some(abandoned_chunk),
        "takeover must isolate the abandoned writer chunk"
    );
    assert_eq!(second.begin, 15);
    assert_eq!(
        second_stream.read_at(0, 28).await.expect("read recovered stream"),
        Bytes::from_static(b"before-restart|after-restart")
    );
}

#[tokio::test]
#[ignore = "requires the owned release container fixture"]
async fn production_stream_write_returns_after_diskio_failure() {
    let chunk_io = connect_chunk_io().await;
    let runtime = ProductionStreamRuntime::new(
        crate::common::hardware::kv_client().into(),
        &chunk_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .expect("assemble production runtime");
    let stream_name = StreamName { high: 42, low: 142 };
    runtime
        .registry()
        .create(StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Stream,
            stream_name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("diskio-failure-e2e".into()),
        })
        .await
        .expect("publish stream binding");
    let stream = runtime
        .create_registered(stream_name, 0, 1)
        .await
        .expect("create stream");
    stream
        .append(&[Bytes::from_static(b"durable|")])
        .await
        .expect("append before DiskIO failure");
    for index in 5..11 {
        crate::common::control::node("kill", index).await;
    }

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        stream.append(&[Bytes::from_static(b"unavailable")]),
    )
    .await
    .expect("journal append exceeded the 15-second fault budget");
    assert!(result.is_err(), "journal append succeeded without DiskIO");
    assert_eq!(stream.tail(), 8, "failed append advanced the journal tail");
    assert_eq!(
        stream.metrics().rollovers,
        1,
        "failed mirror write did not rotate once"
    );
}

#[tokio::test]
#[ignore = "requires the owned release container fixture"]
async fn production_stream_write_returns_after_live_diskio_errors() {
    let chunk_io = connect_chunk_io().await;
    let runtime = ProductionStreamRuntime::new(
        crate::common::hardware::kv_client().into(),
        &chunk_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .expect("assemble production runtime");
    let stream_name = StreamName { high: 42, low: 143 };
    runtime
        .registry()
        .create(StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Stream,
            stream_name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("live-diskio-error-e2e".into()),
        })
        .await
        .expect("publish stream binding");
    let stream = runtime
        .create_registered(stream_name, 0, 1)
        .await
        .expect("create stream");
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        stream.append(&[Bytes::from_static(b"record")]),
    )
    .await
    .expect("journal append exceeded the 15-second fault budget");
    assert!(
        result.is_err(),
        "journal append succeeded with all DiskIO writes failing"
    );
    assert_eq!(stream.tail(), 0);
    assert_eq!(stream.metrics().rollovers, 1);
    for index in 5..11 {
        let state = crate::common::control::node("status", index).await;
        assert_eq!(state["Running"], true, "faulted Diskio exited");
    }
}
