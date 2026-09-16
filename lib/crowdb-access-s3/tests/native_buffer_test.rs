// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::poll_fn;

use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{parse_frame, FrameMagic, MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES};
use hyper::body::Http1BodyReceiveProvider;

#[tokio::test]
async fn native_frame_retains_and_releases_allocator_credit() {
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES).unwrap();
    let provider = allocator.object_receiver();
    let mut allocation = poll_fn(|cx| provider.poll_next_buffer(cx, 12)).await.unwrap();
    assert_eq!(allocator.retained_bytes(), MAX_FRAME_BYTES);
    for (slot, value) in allocation.spare_capacity_mut()[..4].iter_mut().zip(*b"body") {
        slot.write(value);
    }
    allocation.advance(4).unwrap();
    let bytes = provider.on_data_ready(allocation).unwrap();
    assert_eq!(&bytes[..], b"body");
    assert_eq!(allocator.retained_bytes(), MAX_FRAME_BYTES);
    assert_eq!(allocator.allocation_count(), 1);

    drop(bytes);
    assert_eq!(allocator.retained_bytes(), 0);
}

#[tokio::test]
async fn native_allocator_blocks_until_credit_is_released() {
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES).unwrap();
    let provider = allocator.object_receiver();
    let first = poll_fn(|cx| provider.poll_next_buffer(cx, 8)).await.unwrap();
    let waiting_allocator = allocator.clone();
    let waiting = tokio::spawn(async move {
        let provider = waiting_allocator.object_receiver();
        poll_fn(|cx| provider.poll_next_buffer(cx, 8)).await.unwrap()
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());

    drop(first);
    let second = waiting.await.unwrap();
    drop(second);
    assert_eq!(allocator.retained_bytes(), 0);
}

#[tokio::test]
async fn object_receiver_carves_frame_payloads_from_one_mib_owner() {
    const MIB: usize = 1024 * 1024;
    let allocator = NativeBodyAllocator::new(MIB, MIB).unwrap();
    let provider = allocator.object_receiver();

    let mut first = poll_fn(|cx| provider.poll_next_buffer(cx, 32)).await.unwrap();
    let first_pointer = first.spare_capacity_mut().as_ptr() as usize;
    first.spare_capacity_mut()[0].write(1);
    first.advance(1).unwrap();
    let first = provider.on_data_ready(first).unwrap();

    let mut second = poll_fn(|cx| provider.poll_next_buffer(cx, 32)).await.unwrap();
    let second_pointer = second.spare_capacity_mut().as_ptr() as usize;
    second.spare_capacity_mut()[0].write(2);
    second.advance(1).unwrap();
    let second = provider.on_data_ready(second).unwrap();

    assert_eq!(second_pointer - first_pointer, MAX_FRAME_BYTES);
    assert_eq!(allocator.allocation_count(), 1);
    assert_eq!(allocator.retained_bytes(), MIB);
    assert_eq!(&first[..], &[1]);
    assert_eq!(&second[..], &[2]);

    drop(provider);
    drop(first);
    drop(second);
    assert_eq!(allocator.retained_bytes(), 0);
}

#[tokio::test]
async fn native_eof_owner_finalizes_reserved_frame_bytes_in_place() {
    let owner_bytes = 2 * MAX_FRAME_BYTES;
    let allocator = NativeBodyAllocator::new(owner_bytes, owner_bytes).unwrap();
    let provider = allocator.object_receiver();
    provider.enable_owner_handoff();
    let mut allocation = poll_fn(|cx| provider.poll_next_buffer(cx, 7)).await.unwrap();
    for (slot, value) in allocation.spare_capacity_mut()[..7].iter_mut().zip(*b"payload") {
        slot.write(value);
    }
    allocation.advance(7).unwrap();
    let payload = provider.on_data_ready(allocation).unwrap();
    assert!(provider.take_ready_owner().is_none());
    let mut owner = provider.finish_owner().unwrap().unwrap();
    assert_eq!(owner.logical_len(), payload.len() as u64);
    assert_eq!(owner.frame_count(), 1);
    let chunk_id = ChunkId { high: 17, low: 29 };
    let range = owner
        .finalize_frame(0, FrameMagic::RepoLargeV1, chunk_id, 123)
        .unwrap();
    let frame = owner.view(range).unwrap();

    assert_eq!(parse_frame(&frame, chunk_id).unwrap().payload, payload);
    assert_eq!(frame.as_ptr().wrapping_add(14), payload.as_ptr());

    drop(frame);
    drop(owner);
    drop(payload);
    drop(provider);
    assert_eq!(allocator.retained_bytes(), 0);
}

#[tokio::test]
async fn full_owner_is_one_contiguous_buffer_across_chunk_views() {
    let owner_bytes = 2 * MAX_FRAME_BYTES;
    let allocator = NativeBodyAllocator::new(owner_bytes, owner_bytes).unwrap();
    let provider = allocator.object_receiver();
    provider.enable_owner_handoff();
    let mut payloads = Vec::new();
    for value in [0x31, 0x72] {
        let mut allocation = poll_fn(|cx| provider.poll_next_buffer(cx, MAX_FRAME_PAYLOAD_BYTES))
            .await
            .unwrap();
        allocation
            .spare_capacity_mut()
            .fill(std::mem::MaybeUninit::new(value));
        allocation.advance(MAX_FRAME_PAYLOAD_BYTES).unwrap();
        payloads.push(provider.on_data_ready(allocation).unwrap());
    }
    let mut owner = provider.take_ready_owner().unwrap();
    assert_eq!(owner.frame_count(), 2);
    assert_eq!(owner.logical_len(), (2 * MAX_FRAME_PAYLOAD_BYTES) as u64);
    let first_chunk = ChunkId { high: 1, low: 2 };
    let second_chunk = ChunkId { high: 3, low: 4 };
    let first = owner
        .finalize_frame(0, FrameMagic::RepoLargeV1, first_chunk, 5)
        .unwrap();
    let second = owner
        .finalize_frame(1, FrameMagic::RepoLargeV1, second_chunk, 6)
        .unwrap();
    assert_eq!(first, 0..MAX_FRAME_BYTES);
    assert_eq!(second, MAX_FRAME_BYTES..owner_bytes);

    let whole = owner.view(0..owner_bytes).unwrap();
    assert_eq!(whole.len(), owner_bytes);
    assert_eq!(
        parse_frame(&whole[..MAX_FRAME_BYTES], first_chunk)
            .unwrap()
            .payload,
        payloads[0]
    );
    assert_eq!(
        parse_frame(&whole[MAX_FRAME_BYTES..], second_chunk)
            .unwrap()
            .payload,
        payloads[1]
    );
}
