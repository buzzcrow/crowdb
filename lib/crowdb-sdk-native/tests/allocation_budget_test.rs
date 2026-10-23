// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_rpc_ffi::OwnedPayload;
use crowdb_sdk_native::{CapacityKind, Error, OperationContext, RuntimeLimits, RuntimeResources};

fn resources(bytes: usize) -> Arc<RuntimeResources> {
    RuntimeResources::new(RuntimeLimits {
        held_bytes: bytes,
        ..RuntimeLimits::default()
    })
    .unwrap()
}

fn payload(capacity: usize) -> OwnedPayload {
    let mut data = Vec::with_capacity(capacity);
    data.extend_from_slice(b"0123456789");
    OwnedPayload::from(data)
}

#[tokio::test]
async fn separate_deliveries_of_one_allocation_share_capacity_until_final_release() {
    let resources = resources(4096);
    let source = payload(4096);
    let original = source.as_ptr();
    let first = resources
        .retain_payload(source.slice(..4), 1, 10, 0, 0)
        .await
        .unwrap();
    let second = resources
        .retain_payload(source.slice(4..), 2, 11, 0, 200)
        .await
        .unwrap();
    assert_eq!(first.as_slice().as_ptr(), original);
    assert_eq!(second.as_slice().as_ptr(), source[4..].as_ptr());
    assert_eq!(second.metadata().logical_offset, 200);
    assert_eq!(second.metadata().allocation_capacity, 4096);
    assert_eq!(resources.status().held_bytes, 4096);
    drop(source);
    drop(first);
    assert_eq!(resources.status().held_bytes, 4096);
    resources.shutdown();
    let bytes = second.into_bytes();
    assert_eq!(bytes, b"456789"[..]);
    assert_eq!(resources.status().held_bytes, 4096);
    drop(bytes);
    assert_eq!(resources.status().held_bytes, 0);
    assert_eq!(resources.status().compatibility_copy_bytes, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publication_and_repeated_final_release_do_not_double_charge_or_miss_wakes() {
    let resources = resources(1024);
    let source = payload(1024);
    for _ in 0..64 {
        let mut deliveries = tokio::task::JoinSet::new();
        let barrier = Arc::new(tokio::sync::Barrier::new(16));
        for id in 1..=16 {
            let resources = resources.clone();
            let data = source.slice(2..5);
            let barrier = barrier.clone();
            deliveries.spawn(async move {
                barrier.wait().await;
                resources.retain_payload(data, id, id, 0, 0).await
            });
        }
        let mut held = Vec::new();
        while let Some(result) = tokio::time::timeout(Duration::from_secs(1), deliveries.join_next())
            .await
            .unwrap()
        {
            held.push(result.unwrap().unwrap());
        }
        assert_eq!(held.len(), 16);
        assert_eq!(resources.status().held_bytes, 1024);
        drop(held);
        assert_eq!(resources.status().held_bytes, 0);
    }
    assert_eq!(resources.status().held_bytes_high_water, 1024);
}

#[tokio::test]
async fn unknown_owner_waits_before_copying_and_resumes_after_capacity_release() {
    let resources = resources(16);
    let held = resources.retain_vec(vec![7; 16], 1, 1, 0, 0).unwrap();
    let data = OwnedPayload::from(Bytes::from_static(b"unknown"));
    let mut pending = Box::pin(resources.retain_payload(data, 2, 2, 0, 0));
    assert!(tokio::time::timeout(Duration::from_millis(10), &mut pending)
        .await
        .is_err());
    assert_eq!(resources.status().compatibility_copy_bytes, 0);
    drop(held);
    let copied = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(copied.as_slice(), b"unknown");
    assert_eq!(copied.metadata().allocation_capacity, 7);
    assert_eq!(resources.status().compatibility_copy_bytes, 7);
    drop(copied);
    assert_eq!(resources.status().held_bytes, 0);
}

#[tokio::test]
async fn capacity_wait_obeys_request_cancellation_and_runtime_shutdown() {
    let resources = resources(16);
    let held = resources.retain_vec(vec![7; 16], 1, 1, 0, 0).unwrap();
    let context = OperationContext::new(
        NonZeroU64::new(2).unwrap(),
        Instant::now() + Duration::from_secs(1),
    );
    let request = resources.admit(context.clone()).unwrap();
    let mut pending = Box::pin(request.run(resources.retain_payload(payload(16), 2, 2, 0, 0)));
    assert!(tokio::time::timeout(Duration::from_millis(10), &mut pending)
        .await
        .is_err());
    context.cancellation.cancel();
    assert!(matches!(pending.await, Err(Error::Cancelled)));
    resources.shutdown();
    assert!(matches!(
        resources.retain_payload(payload(16), 3, 3, 0, 0).await,
        Err(Error::Shutdown)
    ));
    assert_eq!(held.as_slice(), &[7; 16]);
    drop(held);
    assert_eq!(resources.status().held_bytes, 0);
}

#[tokio::test]
async fn a_short_view_cannot_hide_an_oversized_backing_allocation() {
    let resources = resources(32);
    let result = resources
        .retain_payload(payload(4096).slice(..2), 1, 1, 0, 0)
        .await;
    assert!(matches!(result, Err(Error::Capacity(CapacityKind::HeldBuffers))));
    assert_eq!(resources.status().held_bytes, 0);
}
