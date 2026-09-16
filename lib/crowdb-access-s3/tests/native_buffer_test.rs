// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::poll_fn;

use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_protocol::frame::MAX_FRAME_BYTES;
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
