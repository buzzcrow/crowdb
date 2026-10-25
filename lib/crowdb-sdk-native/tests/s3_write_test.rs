// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/write_storage.rs"]
mod write_storage;

#[path = "common/write_fixture.rs"]
mod fixture;

use crowdb_protocol::frame::{FRAME_V2_HEADER_BYTES, MAX_FRAME_BYTES, MAX_FRAME_V2_PAYLOAD_BYTES};
use crowdb_sdk_native::Error;
use fixture::{context, fixture, options, read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn ordinary_put_streams_mixed_sizes_and_returns_the_published_revision() {
    let (runtime, client, _) = fixture(None).await;
    for (index, length) in [
        0,
        7,
        MAX_FRAME_V2_PAYLOAD_BYTES,
        MAX_FRAME_V2_PAYLOAD_BYTES + 1,
        (1 << 20) + 13,
        (2 << 20) + 31,
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("size-{index}").into_bytes();
        let data = vec![u8::try_from(index).unwrap(); length];
        let result = client
            .put_vec(
                context(index as u64 + 10),
                b"bucket",
                &key,
                data.clone(),
                options(length),
            )
            .await
            .unwrap();
        let selected = client
            .resolve_object(context(100), b"bucket", &key)
            .await
            .unwrap();
        assert_eq!(&result, selected.metadata());
        assert_eq!(result.logical_length, length as u64);
        assert_eq!(read(&client, &key).await, data);
    }
    assert_eq!(runtime.resources().status().requests, 0);
    assert_eq!(runtime.resources().status().held_bytes, 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn framed_put_keeps_original_payload_addresses_through_disk_completion() {
    let (runtime, client, backend) = fixture(None).await;
    let length = 16 * MAX_FRAME_V2_PAYLOAD_BYTES + 19;
    let mut put = client
        .begin_put(context(10), b"bucket", b"native", options(length))
        .await
        .unwrap();
    let first = runtime
        .allocate_producer(1 << 20)
        .unwrap()
        .initialize_framed(16 * MAX_FRAME_V2_PAYLOAD_BYTES, 0x53, |offset, bytes| {
            bytes[0] = u8::try_from(offset / MAX_FRAME_V2_PAYLOAD_BYTES).unwrap();
        })
        .unwrap();
    let first_payload = first.payload_views()[0].as_ptr() as usize;
    assert_eq!(first.allocation_capacity(), 1 << 20);
    put.write_framed(first).await.unwrap();
    let last = runtime
        .allocate_producer(u32::try_from(MAX_FRAME_BYTES).unwrap())
        .unwrap()
        .initialize_framed(19, 0x61, |_, _| {})
        .unwrap();
    put.write_framed(last).await.unwrap();
    assert_eq!(put.accepted_bytes(), length as u64);
    put.finish().await.unwrap();
    assert!(backend
        .data
        .iter()
        .any(|entry| entry.value().as_ptr() as usize + FRAME_V2_HEADER_BYTES == first_payload));
    let data = read(&client, b"native").await;
    assert_eq!(data.len(), length);
    for index in 0..16 {
        assert_eq!(
            data[index * MAX_FRAME_V2_PAYLOAD_BYTES],
            u8::try_from(index).unwrap()
        );
        assert!(
            data[index * MAX_FRAME_V2_PAYLOAD_BYTES + 1..(index + 1) * MAX_FRAME_V2_PAYLOAD_BYTES]
                .iter()
                .all(|byte| *byte == 0x53)
        );
    }
    assert_eq!(&data[16 * MAX_FRAME_V2_PAYLOAD_BYTES..], &[0x61; 19]);
    runtime.shutdown().await.unwrap();
    assert!(runtime.resources().status().producer_bytes > 0);
}

#[tokio::test]
async fn invalid_length_checksum_and_cancel_never_publish_an_object() {
    let (runtime, client, _) = fixture(None).await;
    let mut incomplete = client
        .begin_put(context(10), b"bucket", b"short", options(7))
        .await
        .unwrap();
    incomplete
        .write(runtime.resources().retain_vec(vec![1; 6], 10, 10, 0, 0).unwrap())
        .await
        .unwrap();
    assert_eq!(
        incomplete.finish().await,
        Err(Error::Length {
            expected: 7,
            delivered: 6
        })
    );
    let mut bad = options(7);
    bad.expected_md5 = Some([0; 16]);
    assert_eq!(
        client
            .put_vec(context(11), b"bucket", b"digest", vec![1; 7], bad)
            .await,
        Err(Error::Integrity)
    );
    let cancellation_context = context(12);
    let mut cancelled = client
        .begin_put(cancellation_context.clone(), b"bucket", b"cancel", options(7))
        .await
        .unwrap();
    cancellation_context.cancellation.cancel();
    let input = runtime.resources().retain_vec(vec![1; 7], 12, 12, 0, 0).unwrap();
    assert_eq!(cancelled.write(input).await, Err(Error::Cancelled));
    assert_eq!(cancelled.finish().await, Err(Error::Cancelled));
    for key in [b"short".as_slice(), b"digest", b"cancel"] {
        assert!(matches!(
            client.resolve_object(context(30), b"bucket", key).await,
            Err(Error::S3(crowdb_access_s3::S3ErrorCode::NoSuchKey))
        ));
    }
    assert_eq!(runtime.resources().status().requests, 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_publication_reply_preserves_uncertainty_and_durable_data() {
    let loss = Arc::new(AtomicBool::new(false));
    let (runtime, client, backend) = fixture(Some(loss.clone())).await;
    loss.store(true, Ordering::Release);
    assert_eq!(
        client
            .put_vec(
                context(41),
                b"bucket",
                b"uncertain",
                b"applied".to_vec(),
                options(7)
            )
            .await,
        Err(Error::UncertainMutation {
            request_id: 41,
            reason: crowdb_sdk_native::error::MutationInterruption::Transport
        })
    );
    assert_eq!(backend.deletes.load(Ordering::Acquire), 0);
    loss.store(false, Ordering::Release);
    assert_eq!(read(&client, b"uncertain").await, b"applied");
    assert_eq!(runtime.resources().status().requests, 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_pending_input_terminates_the_writer_and_releases_request_admission() {
    let (runtime, client, backend) = fixture(None).await;
    backend.block_write.store(true, Ordering::Release);
    let length = 32 * MAX_FRAME_V2_PAYLOAD_BYTES;
    let mut put = client
        .begin_put(context(81), b"bucket", b"dropped", options(length))
        .await
        .unwrap();
    let first = runtime
        .allocate_producer(1 << 20)
        .unwrap()
        .initialize_framed(16 * MAX_FRAME_V2_PAYLOAD_BYTES, 0x61, |_, _| {})
        .unwrap();
    put.write_framed(first).await.unwrap();
    backend.write_started.notified().await;
    let second = runtime
        .allocate_producer(1 << 20)
        .unwrap()
        .initialize_framed(16 * MAX_FRAME_V2_PAYLOAD_BYTES, 0x62, |_, _| {})
        .unwrap();
    let mut pending = Box::pin(put.write_framed(second));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut pending)
        .await
        .is_err());
    drop(pending);
    assert_eq!(runtime.resources().status().requests, 0);
    assert_eq!(put.finish().await, Err(Error::Cancelled));
    assert!(matches!(
        client.resolve_object(context(82), b"bucket", b"dropped").await,
        Err(Error::S3(crowdb_access_s3::S3ErrorCode::NoSuchKey))
    ));
    backend.block_write.store(false, Ordering::Release);
    backend.write_allowed.notify_waiters();
    runtime.shutdown().await.unwrap();
}
