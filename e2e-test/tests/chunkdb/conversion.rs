// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod foreground;

use bytes::Bytes;
use crowdb_chunkdb::conversion::io::ConversionDiskIo;
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, Chunk, ChunkType, QueryChunkRequest, SealChunkRequest, Strip, StripType,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "requires the owned protected conversion container fixture"]
async fn manual_partial_mirror_to_ec_preserves_exact_shards() {
    let (_, io, converted, expected) = converted_partial().await;
    assert_shards(&io, &converted, expected).await;
}

pub(super) async fn converted_partial() -> (ChunkdbClient, ConversionDiskIo, Chunk, Vec<Vec<u8>>) {
    let (client, io) = clients().await;
    let (chunk, expected, length) = prepare_partial(&client, &io).await;
    let id = chunk.id.unwrap();
    let sealed = client
        .seal_chunk(SealChunkRequest {
            chunk_id: Some(id),
            seal_bytes: length,
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(sealed.acknowledged_cursor, length);
    trigger(&id).await;
    let converted = wait_converted(&client, &id).await;
    assert_eq!(converted.acknowledged_cursor, length);
    (client, io, converted, expected)
}

pub(super) async fn clients() -> (ChunkdbClient, ConversionDiskIo) {
    let client = ChunkdbClient::new(
        ServiceRegistryClient::new(crate::common::hardware::kv_client()),
        Arc::new(ChunkdbRpcTransport::new()),
    );
    let io = ConversionDiskIo::connect(
        &ServiceRegistryClient::new(crate::common::hardware::kv_client()),
        &HardwareClient::new(crate::common::hardware::kv_client()),
    )
    .await
    .unwrap();
    (client, io)
}

pub(super) async fn prepare_partial(
    client: &ChunkdbClient,
    io: &ConversionDiskIo,
) -> (Chunk, Vec<Vec<u8>>, u64) {
    let chunk = client
        .allocate_chunk(AllocateChunkRequest {
            write_granularity: 1,
            strip_count: 8,
            strip_type: StripType::Mirror as i32,
            copy_count: 3,
            chunk_type: ChunkType::S3 as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(chunk.strips.len(), 8);
    let shard = usize::try_from(chunk.strips[0].capacity).unwrap() * 1024;
    let payload: Vec<u8> = (0..7 * shard + 17)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let expected: Vec<Vec<u8>> = payload.chunks(shard).map(<[u8]>::to_vec).collect();
    for (strip, data) in chunk.strips.iter().zip(&expected) {
        let Strip::MirrorStrip(mirror) = strip.strip.as_ref().unwrap() else {
            panic!("expected mirror")
        };
        let unit = u64::from(strip.unit_kb) * 1024;
        for segment in &mirror.segments {
            io.write_segment(segment, unit, Bytes::from(vec![0xa5; shard]))
                .await
                .unwrap();
            io.write_segment(segment, unit, Bytes::copy_from_slice(data))
                .await
                .unwrap();
            io.fsync_segment(segment).await.unwrap();
            if data.len() < shard {
                let witness = io.read_segment(segment, unit).await.unwrap();
                assert_eq!(&witness[..data.len()], data);
                assert!(witness[data.len()..].iter().all(|byte| *byte == 0xa5));
            }
        }
    }
    (chunk, expected, payload.len() as u64)
}

async fn trigger(id: &crowdb_protocol::common::ChunkId) {
    let path = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap())
        .join("chunkdb-endpoints.json");
    let endpoint: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let response: serde_json::Value = reqwest::Client::new()
        .post(format!(
            "http://{}:7301/convert_chunk",
            endpoint["ip"].as_str().unwrap()
        ))
        .json(&serde_json::json!({"chunk_id": id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        response["accepted_groups"], 1,
        "manual conversion admission: {response}"
    );
}

pub(super) async fn assert_shards(io: &ConversionDiskIo, converted: &Chunk, expected: Vec<Vec<u8>>) {
    assert_shards_with_tail(io, converted, expected, 0).await;
}

pub(super) async fn assert_shards_with_tail(
    io: &ConversionDiskIo,
    converted: &Chunk,
    mut expected: Vec<Vec<u8>>,
    tail: u8,
) {
    let shard = expected[0].len();
    let mut padded = expected.clone();
    for data in &mut padded {
        data.resize(shard, 0);
    }
    let refs: Vec<_> = padded.iter().map(Vec::as_slice).collect();
    expected.extend(encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap());
    let strip = &converted.strips[0];
    let Strip::EcStrip(ec) = strip.strip.as_ref().unwrap() else {
        panic!("expected EC")
    };
    assert_eq!((ec.data_num, ec.code_num, ec.segments.len()), (8, 4, 12));
    for (index, segment) in ec.segments.iter().enumerate() {
        let bytes = io
            .read_segment(segment, u64::from(strip.unit_kb) * 1024)
            .await
            .unwrap();
        assert_eq!(&bytes[..expected[index].len()], expected[index], "shard {index}");
        if expected[index].len() < bytes.len() {
            assert!(bytes[expected[index].len()..].iter().all(|byte| *byte == tail));
        }
    }
}

pub(super) async fn wait_converted(client: &ChunkdbClient, id: &crowdb_protocol::common::ChunkId) -> Chunk {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let chunk = client
            .query_chunk(QueryChunkRequest { chunk_id: Some(*id) })
            .await
            .unwrap()
            .chunk
            .unwrap();
        if chunk.strips.len() == 1
            && matches!(&chunk.strips[0].strip, Some(Strip::EcStrip(ec)) if ec.ec_state == crowdb_protocol::chunkdb::rpc::EcState::Parity as i32)
        {
            return chunk;
        }
        assert!(Instant::now() < deadline, "conversion did not publish: {chunk:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
