// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core large-write E2E coverage through real service processes.

#[path = "common/e2e_stack.rs"]
mod e2e_stack;

use std::sync::Arc;

use crowdb_chunk_client::{ChunkClientConfig, ChunkReadPolicy, LargeWritePolicy, SmallWritePolicy};
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, EcState, Location, Strip};

use e2e_stack::{all_binaries_available, E2eStack};

const MIB: usize = 1024 * 1024;

fn ec_4_1() -> EcScheme {
    EcScheme {
        data_num: 4,
        code_num: 1,
    }
}

fn small_policy() -> SmallWritePolicy {
    SmallWritePolicy {
        mirror_copies: 1,
        ..SmallWritePolicy::default()
    }
}

fn policy(max_chunk_size: u64) -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: ec_4_1(),
        client: Arc::new(ChunkClientConfig {
            max_chunk_size,
            prefetch_strips_per_chunk: 2,
            parity_depth: 2,
            chunk_preparation_depth: 1,
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

async fn read_ec_location(stack: &E2eStack, chunk: &Chunk, location: &Location) -> Vec<u8> {
    assert_eq!(location.offset, 0);
    let mut physical = Vec::new();
    let mut remaining = location.length;
    for strip in &chunk.strips {
        if remaining == 0 {
            break;
        }
        let Strip::EcStrip(ec) = strip.strip.as_ref().expect("strip body") else {
            panic!("large-write chunk contains a non-EC strip");
        };
        assert_eq!(ec.data_num, 4);
        assert_eq!(ec.code_num, 1);
        assert_eq!(
            ec.ec_state,
            EcState::Parity as i32,
            "sealed EC strip metadata: {strip:?}"
        );
        assert!(strip.sealed_length > 0);
        assert_eq!(ec.segments.len(), 5);
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        for segment in ec.segments.iter().take(ec.data_num as usize) {
            let length = remaining.min(unit_bytes);
            if length == 0 {
                break;
            }
            physical.extend(
                stack
                    .read_segment(segment, unit_bytes, 0, u32::try_from(length).unwrap())
                    .await,
            );
            remaining -= length;
        }
    }
    physical
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
async fn large_write_multi_strip_persists_data_metadata_and_parity() {
    if !all_binaries_available() {
        return;
    }
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
    assert_eq!(chunk.sealed_length, 12 * 1024);
    assert_eq!(chunk.strips.len(), 3);
    assert_eq!(read_ec_location(&stack, &chunk, location).await, data);
    assert_ec_parity(&stack, &chunk, location).await;
    let read = stack.client.read_object(&result.locations).await.unwrap();
    assert_bytes_match(&read, &data);
    assert_eq!(
        stack
            .client
            .read_range(&result.locations, 3 * MIB as u64 + 17, 9 * MIB as u64 + 31)
            .await
            .unwrap(),
        data[3 * MIB + 17..9 * MIB + 31]
    );
}

#[tokio::test]
async fn large_write_rotates_chunks_without_losing_data() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start(small_policy()).await;
    let data = make_test_data(20 * MIB);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), policy(8 * MIB as u64))
        .write_stream(data.as_slice())
        .await
        .unwrap();

    assert_eq!(result.locations.len(), 3);
    assert_eq!(
        result
            .locations
            .iter()
            .map(|location| location.length)
            .collect::<Vec<_>>(),
        vec![8 * MIB as u64, 8 * MIB as u64, 4 * MIB as u64]
    );
    let mut read_back = Vec::new();
    for (index, location) in result.locations.iter().enumerate() {
        let chunk = stack.query_chunk(location).await;
        assert_eq!(chunk.state, ChunkState::Sealed as i32);
        assert_eq!(
            chunk.sealed_length,
            u32::try_from(location.length.div_ceil(1024)).unwrap()
        );
        let written_strips = if index < 2 { 2 } else { 1 };
        assert!(chunk.strips.len() >= written_strips);
        for strip in chunk.strips.iter().skip(written_strips) {
            let Strip::EcStrip(ec) = strip.strip.as_ref().unwrap() else {
                panic!("large-write chunk contains a non-EC strip");
            };
            assert_eq!(ec.ec_state, EcState::NoParity as i32);
            assert_eq!(strip.sealed_length, 0);
        }
        read_back.extend(read_ec_location(&stack, &chunk, location).await);
        assert_ec_parity(&stack, &chunk, location).await;
    }
    assert_eq!(read_back, data);

    assert_eq!(stack.client.read_object(&result.locations).await.unwrap(), data);
    assert_eq!(
        stack
            .client
            .read_range(&result.locations, 7 * MIB as u64, 10 * MIB as u64)
            .await
            .unwrap(),
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
        assert!(part.len() <= 3 * MIB);
        streamed.extend_from_slice(&part);
    }
    assert_eq!(streamed, data);
}

#[tokio::test]
async fn large_write_unknown_size_partial_tail_is_durable() {
    if !all_binaries_available() {
        return;
    }
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
    assert_eq!(location.length, data.len() as u64);
    let chunk = stack.query_chunk(location).await;
    assert_eq!(chunk.state, ChunkState::Sealed as i32);
    assert_eq!(
        chunk.sealed_length,
        u32::try_from(location.length.div_ceil(1024)).unwrap()
    );
    assert!(chunk.strips.len() >= 2);
    assert_eq!(read_ec_location(&stack, &chunk, location).await, data);
    assert_ec_parity(&stack, &chunk, location).await;
    assert_eq!(stack.client.read_object(&result.locations).await.unwrap(), data);
}
