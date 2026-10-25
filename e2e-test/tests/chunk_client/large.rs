// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::stack::TestStack as E2eStack;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoWriter, ChunkReadPolicy, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, EcState, Location, Strip};
use crowdb_protocol::frame::{ChunkLocation, MAX_FRAME_V2_PAYLOAD_BYTES};
use std::sync::Arc;
const MIB: usize = 1024 * 1024;
fn ec_4_1() -> EcScheme {
    EcScheme {
        data_num: 4,
        code_num: 1,
    }
}

fn small_policy() -> SmallWritePolicy {
    SmallWritePolicy {
        mirror_copies: 2,
        ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
    }
}

fn policy(max_chunk_size: u64) -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: ec_4_1(),
        client: Arc::new(ChunkClientConfig {
            chunk_type: crowdb_protocol::chunkdb::rpc::ChunkType::S3,
            large_mirror_copies: None,
            max_chunk_size,
            prefetch_strips_per_chunk: 2,
            large_prefetch_max_strips_per_batch: 32,
            large_parallel_strip_writes: 4,
            large_held_buffers: 4,
            parity_depth: 2,
            chunk_preparation_depth: 1,
            large_write_repair_attempts: 3,
            read_buffer_size: MIB,
            max_cached_buffer: 4 * MIB,
            memory_budget: 0,
        }),
    }
}

fn make_test_data(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| u8::try_from((index * 17 + 37) % 251).unwrap())
        .collect()
}

fn assert_bytes_match(actual: &[u8], expected: &[u8]) {
    assert_eq!(actual.len(), expected.len());
    if let Some(index) = actual
        .iter()
        .zip(expected)
        .position(|(left, right)| left != right)
    {
        panic!(
            "first byte mismatch at {index}: actual={}, expected={}, MiB starts={:?}",
            actual[index],
            expected[index],
            (0..actual.len() / MIB)
                .map(|block| actual[block * MIB])
                .collect::<Vec<_>>()
        );
    }
}

async fn assert_ec_parity(stack: &E2eStack, chunk: &Chunk, location: &Location) {
    let mut remaining = location.length;
    for strip in &chunk.strips {
        if remaining == 0 {
            break;
        }
        let Strip::EcStrip(ec) = strip.strip.as_ref().expect("strip body") else {
            panic!("large-write chunk contains a non-EC strip");
        };
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let mut data = Vec::new();
        for segment in ec.segments.iter().take(ec.data_num as usize) {
            let length = remaining.min(unit_bytes);
            let mut shard = vec![0; usize::try_from(unit_bytes).unwrap()];
            if length > 0 {
                let actual = stack
                    .read_segment(segment, unit_bytes, 0, u32::try_from(length).unwrap())
                    .await;
                shard[..usize::try_from(length).unwrap()].copy_from_slice(&actual);
                remaining -= length;
            }
            data.push(shard);
        }
        let refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
        let expected = encode_parity_from_shards(ec_4_1(), &refs).unwrap();
        for (segment, expected) in ec.segments.iter().skip(ec.data_num as usize).zip(expected) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(unit_bytes).unwrap())
                .await;
            assert_eq!(actual, expected);
        }
    }
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_write_multi_strip_persists_data_metadata_and_parity() {
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(12 * MIB);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), policy(1024 * MIB as u64))
        .write_stream(data.as_slice())
        .await
        .unwrap();

    assert_eq!(result.locations.len(), 1);
    let location = &result.locations[0];
    let chunk = stack.query_chunk(location).await;
    assert_eq!(chunk.state, ChunkState::Sealed as i32);
    assert_eq!(
        chunk.sealed_length,
        u32::try_from(location.length.div_ceil(1024)).unwrap()
    );
    assert_eq!(chunk.strips.len(), 4);
    assert_ec_parity(&stack, &chunk, location).await;
    let read = stack.client.read_object(&result.locations).await.unwrap();
    assert!(read.len() > 1, "complete read must retain verified frame buffers");
    assert_bytes_match(&read.concat(), &data);
    assert_eq!(
        stack
            .client
            .read_range(&result.locations, 3 * MIB as u64 + 17, 9 * MIB as u64 + 31)
            .await
            .unwrap()
            .concat(),
        data[3 * MIB + 17..9 * MIB + 31]
    );
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_two_copy_mirror_reads_across_strips() {
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(9 * MIB);
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(2);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), configured)
        .write_stream(data.as_slice())
        .await
        .unwrap();
    let chunk = stack.query_chunk(&result.locations[0]).await;
    assert!(chunk.strips.len() >= 9);
    let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
        panic!("large mirror policy allocated a different strip");
    };
    assert_eq!(mirror.segments.len(), 2);
    assert!(chunk.strips.iter().take(9).all(|strip| strip.sealed_length > 0));
    assert_eq!(
        stack
            .client
            .read_object(&result.locations)
            .await
            .unwrap()
            .concat(),
        data
    );
    assert_eq!(
        stack
            .client
            .read_range(&result.locations, MIB as u64 - 100, MIB as u64 + 100)
            .await
            .unwrap()
            .concat(),
        data[MIB - 100..MIB + 100]
    );
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_raw_buffers_submit_complete_mirror_strips_concurrently() {
    let stack = E2eStack::start(small_policy()).await;
    let mut configured = policy(16 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().large_mirror_copies = Some(2);
    let mut writer = stack.client.prepare_large_write(None, configured);
    let block = Bytes::from(vec![0x5a; 16 * MAX_FRAME_V2_PAYLOAD_BYTES]);
    for _ in 0..8 {
        assert_eq!(
            writer.on_data(block.clone()).await.unwrap(),
            crowdb_chunk_client::FeedStatus::Continue
        );
    }
    let locations = writer.on_finish().await.unwrap();
    let timing = writer.write_timing().unwrap();
    assert_eq!(timing.strip_write_successes, 8);
    assert!(timing.mirror_uncommitted_peak > 0);
    assert!(timing.mirror_active_write_peak > 0);
    assert_eq!(
        stack.client.read_object(&locations).await.unwrap().concat(),
        block.repeat(8)
    );
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_write_rotates_chunks_without_losing_data() {
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(20 * MIB);
    let mut configured = policy(8 * MIB as u64);
    Arc::get_mut(&mut configured.client).unwrap().chunk_type =
        crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable;
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), configured)
        .write_stream(data.as_slice())
        .await
        .unwrap();

    assert_eq!(result.locations.len(), 3);
    assert_eq!(result.locations[0].length, 8 * MIB as u64);
    assert_eq!(result.locations[1].length, 8 * MIB as u64);
    let tail = result.locations.last().unwrap();
    assert_eq!(
        tail.length,
        ChunkLocation {
            chunk_id: tail.chunk_id.unwrap(),
            frame_offset: tail.offset,
            logical_length: tail.logical_length,
        }
        .physical_length()
        .unwrap()
    );
    let mut read_back = Vec::new();
    for location in &result.locations {
        let chunk = stack.query_chunk(location).await;
        assert_eq!(
            chunk.chunk_type,
            crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable as i32
        );
        assert_eq!(
            chunk.id.unwrap().high >> 56,
            crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable as u64
        );
        assert_eq!(chunk.state, ChunkState::Sealed as i32);
        assert_eq!(
            chunk.sealed_length,
            u32::try_from(location.length.div_ceil(1024)).unwrap()
        );
        let written_strips = chunk
            .strips
            .iter()
            .take_while(|strip| strip.sealed_length > 0)
            .count();
        assert!(chunk.strips.len() >= written_strips);
        for strip in chunk.strips.iter().skip(written_strips) {
            let Strip::EcStrip(ec) = strip.strip.as_ref().unwrap() else {
                panic!("large-write chunk contains a non-EC strip");
            };
            assert_eq!(ec.ec_state, EcState::NoParity as i32);
            assert_eq!(strip.sealed_length, 0);
        }
        assert_ec_parity(&stack, &chunk, location).await;
    }

    read_back.extend(
        stack
            .client
            .read_object(&result.locations)
            .await
            .unwrap()
            .concat(),
    );
    assert_eq!(read_back, data);
    assert_eq!(
        stack
            .client
            .read_range(&result.locations, 7 * MIB as u64, 10 * MIB as u64)
            .await
            .unwrap()
            .concat(),
        data[7 * MIB..10 * MIB]
    );
    let read_client = stack
        .client
        .clone()
        .with_read_policy(ChunkReadPolicy {
            stream_window_bytes: 3 * MIB,
            ..ChunkReadPolicy::default()
        })
        .unwrap();
    let mut stream = read_client.read_stream(&result.locations).unwrap();
    let mut streamed = Vec::new();
    while let Some(part) = stream.next_chunk().await {
        let part = part.unwrap();
        assert!(part.len() <= MAX_FRAME_V2_PAYLOAD_BYTES);
        streamed.extend_from_slice(&part);
    }
    assert_eq!(streamed, data);
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_write_unknown_size_partial_tail_is_durable() {
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(5 * MIB + 123);
    let result = stack
        .client
        .prepare_large_write(None, policy(16 * MIB as u64))
        .write_stream(data.as_slice())
        .await
        .unwrap();

    assert_eq!(result.locations.len(), 1);
    let location = &result.locations[0];
    assert_eq!(
        location.length,
        ChunkLocation {
            chunk_id: location.chunk_id.unwrap(),
            frame_offset: location.offset,
            logical_length: data.len() as u64,
        }
        .physical_length()
        .unwrap()
    );
    let chunk = stack.query_chunk(location).await;
    assert_eq!(chunk.state, ChunkState::Sealed as i32);
    assert_eq!(
        chunk.sealed_length,
        u32::try_from(location.length.div_ceil(1024)).unwrap()
    );
    assert!(chunk.strips.len() >= 2);
    assert_eq!(
        stack
            .client
            .read_object(&result.locations)
            .await
            .unwrap()
            .concat(),
        data
    );
}
