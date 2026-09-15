// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::poll_fn;

use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use hyper::body::Http1BodyAllocator;

#[tokio::test]
async fn native_frame_retains_and_releases_allocator_credit() {
    let allocator = NativeBodyAllocator::new(16, 8).unwrap();
    let mut allocation = poll_fn(|cx| allocator.poll_allocate(cx, 12)).await.unwrap();
    assert_eq!(allocator.retained_bytes(), 8);
    for (slot, value) in allocation.spare_capacity_mut()[..4].iter_mut().zip(*b"body") {
        slot.write(value);
    }
    allocation.advance(4).unwrap();
    let bytes = allocation.freeze();
    assert_eq!(&bytes[..], b"body");
    assert_eq!(allocator.retained_bytes(), 8);
    assert_eq!(allocator.allocation_count(), 1);

    drop(bytes);
    assert_eq!(allocator.retained_bytes(), 0);
}

#[tokio::test]
async fn native_allocator_blocks_until_credit_is_released() {
    let allocator = NativeBodyAllocator::new(8, 8).unwrap();
    let first = poll_fn(|cx| allocator.poll_allocate(cx, 8)).await.unwrap();
    let waiting_allocator = allocator.clone();
    let waiting = tokio::spawn(async move {
        poll_fn(|cx| waiting_allocator.poll_allocate(cx, 8))
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());

    drop(first);
    let second = waiting.await.unwrap();
    drop(second);
    assert_eq!(allocator.retained_bytes(), 0);
}
