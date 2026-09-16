// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::poll_fn;
use std::time::Duration;

use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{parse_frame, FrameMagic, MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES};
use hyper::body::{Bytes, Http1BodyReceiveProvider};

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
async fn native_allocator_wakes_all_concurrent_credit_waiters() {
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES).unwrap();
    let provider = allocator.object_receiver();
    let occupied = poll_fn(|cx| provider.poll_next_buffer(cx, 8)).await.unwrap();
    let waiting = (0..2)
        .map(|_| {
            let allocator = allocator.clone();
            tokio::spawn(async move {
                let provider = allocator.object_receiver();
                let region = poll_fn(|cx| provider.poll_next_buffer(cx, 8)).await.unwrap();
                drop(region);
            })
        })
        .collect::<Vec<_>>();
    tokio::time::timeout(Duration::from_secs(2), async {
        while allocator.metrics_snapshot().backpressure_events < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both receivers must wait for owner credit");

    drop(occupied);
    tokio::time::timeout(Duration::from_secs(2), async {
        for waiter in waiting {
            waiter.await.unwrap();
        }
    })
    .await
    .expect("both receivers must wake when credit is returned");
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
    let metrics = allocator.metrics_snapshot();
    assert_eq!(metrics.backpressure_events, 1);
    assert!(metrics.backpressure_wait_ns > 0);
    assert_eq!(metrics.allocations, 2);
    assert_eq!(metrics.budget_bytes, MAX_FRAME_BYTES);
    assert_eq!(metrics.owner_bytes, MAX_FRAME_BYTES);
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
    let frame = owner.views(range).unwrap().pop().unwrap();

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

    let whole = owner.views(0..owner_bytes).unwrap().pop().unwrap();
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

#[tokio::test]
async fn prefetched_payload_starts_the_first_native_owner_and_next_fill_appends() {
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES).unwrap();
    let provider = allocator.object_receiver();
    provider.enable_owner_handoff();
    let payload = Bytes::from_static(b"prefetched-body");
    let delivered = provider.on_prefetched_data(payload.clone()).unwrap();
    assert_eq!(delivered, payload);
    let mut next = poll_fn(|cx| provider.poll_next_buffer(cx, 5)).await.unwrap();
    assert_eq!(next.spare_capacity_mut().len(), 5);
    for (slot, value) in next.spare_capacity_mut().iter_mut().zip(*b"-next") {
        slot.write(value);
    }
    next.advance(5).unwrap();
    let appended = provider.on_data_ready(next).unwrap();
    assert_eq!(&appended[..], b"-next");
    let mut owner = provider.finish_owner().unwrap().unwrap();
    let chunk_id = ChunkId { high: 41, low: 43 };
    let range = owner
        .finalize_frame(0, FrameMagic::RepoLargeV1, chunk_id, 47)
        .unwrap();
    let frame = owner.views(range).unwrap().pop().unwrap();

    assert_ne!(frame.as_ptr().wrapping_add(14), payload.as_ptr());
    assert_eq!(
        parse_frame(&frame, chunk_id).unwrap().payload,
        b"prefetched-body-next"
    );
    assert_eq!(owner.logical_len(), 20);
    assert_eq!(allocator.prefix_copy_bytes(), payload.len());
    assert!(provider.owner_handoff_active());
}

#[tokio::test]
async fn prefetched_body_waits_for_credit_and_still_starts_at_body_offset_zero() {
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES).unwrap();
    let occupied_provider = allocator.object_receiver();
    let occupied = poll_fn(|cx| occupied_provider.poll_next_buffer(cx, 8))
        .await
        .unwrap();
    let provider = allocator.object_receiver();
    provider.enable_owner_handoff();
    let prefix = Bytes::from_static(b"body-prefix");
    assert_eq!(provider.on_prefetched_data(prefix.clone()).unwrap(), prefix);
    assert_eq!(allocator.prefix_copy_bytes(), 0);

    let waiting = tokio::spawn(async move {
        let mut next = poll_fn(|cx| provider.poll_next_buffer(cx, 5)).await.unwrap();
        for (slot, value) in next.spare_capacity_mut().iter_mut().zip(*b"-next") {
            slot.write(value);
        }
        next.advance(5).unwrap();
        let appended = provider.on_data_ready(next).unwrap();
        assert_eq!(&appended[..], b"-next");
        provider.finish_owner().unwrap().unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while allocator.metrics_snapshot().backpressure_events == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!waiting.is_finished());
    drop(occupied);
    let mut owner = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    let chunk_id = ChunkId { high: 51, low: 53 };
    let frame_range = owner
        .finalize_frame(0, FrameMagic::RepoLargeV1, chunk_id, 59)
        .unwrap();
    let frame = owner.views(frame_range).unwrap().pop().unwrap();
    assert_eq!(
        parse_frame(&frame, chunk_id).unwrap().payload,
        b"body-prefix-next"
    );
    assert_eq!(owner.logical_len(), 16);
    assert_eq!(allocator.prefix_copy_bytes(), prefix.len());
}

#[tokio::test]
async fn prefetched_body_at_eof_waits_for_credit_before_finalizing() {
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES).unwrap();
    let occupied_provider = allocator.object_receiver();
    let occupied = poll_fn(|cx| occupied_provider.poll_next_buffer(cx, 8))
        .await
        .unwrap();
    let provider = allocator.object_receiver();
    provider.enable_owner_handoff();
    provider
        .on_prefetched_data(Bytes::from_static(b"only-body"))
        .unwrap();
    let waiting = tokio::spawn(async move { provider.finish_owner_when_ready().await.unwrap().unwrap() });
    tokio::time::timeout(Duration::from_secs(2), async {
        while allocator.metrics_snapshot().backpressure_events == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!waiting.is_finished());
    drop(occupied);
    let mut owner = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    let chunk_id = ChunkId { high: 61, low: 67 };
    let frame_range = owner
        .finalize_frame(0, FrameMagic::RepoLargeV1, chunk_id, 71)
        .unwrap();
    let frame = owner.views(frame_range).unwrap().pop().unwrap();
    assert_eq!(parse_frame(&frame, chunk_id).unwrap().payload, b"only-body");
    assert_eq!(allocator.prefix_copy_bytes(), 9);
}

#[tokio::test]
async fn one_mib_owner_boundaries_follow_body_not_prefetched_header_size() {
    const MIB: usize = 1024 * 1024;
    let allocator = NativeBodyAllocator::new(MIB, MIB).unwrap();
    let provider = allocator.object_receiver();
    provider.enable_owner_handoff();
    let prefix = Bytes::from(vec![b'p'; MAX_FRAME_PAYLOAD_BYTES - 2]);
    provider.on_prefetched_data(prefix.clone()).unwrap();

    let mut first = poll_fn(|cx| provider.poll_next_buffer(cx, 4)).await.unwrap();
    assert_eq!(first.spare_capacity_mut().len(), 2);
    for (slot, value) in first.spare_capacity_mut().iter_mut().zip(*b"12") {
        slot.write(value);
    }
    first.advance(2).unwrap();
    assert_eq!(&provider.on_data_ready(first).unwrap()[..], b"12");
    let mut second = poll_fn(|cx| provider.poll_next_buffer(cx, 2)).await.unwrap();
    for (slot, value) in second.spare_capacity_mut().iter_mut().zip(*b"34") {
        slot.write(value);
    }
    second.advance(2).unwrap();
    assert_eq!(&provider.on_data_ready(second).unwrap()[..], b"34");
    let mut owner = provider.finish_owner_when_ready().await.unwrap().unwrap();
    let chunk_id = ChunkId { high: 73, low: 79 };
    assert_eq!(owner.frame_count(), 2);
    let first_range = owner
        .finalize_frame(0, FrameMagic::RepoLargeV1, chunk_id, 0)
        .unwrap();
    let second_range = owner
        .finalize_frame(
            1,
            FrameMagic::RepoLargeV1,
            chunk_id,
            MAX_FRAME_PAYLOAD_BYTES as u64,
        )
        .unwrap();
    assert_eq!(first_range, 0..MAX_FRAME_BYTES);
    assert_eq!(second_range.start, MAX_FRAME_BYTES);
    assert_eq!(owner.logical_len(), (MAX_FRAME_PAYLOAD_BYTES + 2) as u64);
    let first_frame = owner.views(first_range).unwrap().pop().unwrap();
    let second_frame = owner.views(second_range).unwrap().pop().unwrap();
    assert_eq!(
        parse_frame(&first_frame, chunk_id).unwrap().payload.len(),
        MAX_FRAME_PAYLOAD_BYTES
    );
    assert_eq!(&first_frame[14..14 + prefix.len()], &prefix[..]);
    assert_eq!(parse_frame(&second_frame, chunk_id).unwrap().payload, b"34");
    assert_eq!(allocator.allocation_count(), 1);
}
