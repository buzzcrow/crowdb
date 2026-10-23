// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_rpc_ffi::{BufferPool, ProducerBufferAllocator, RdmaConfig, RpcServer};
use crowdb_sdk_native::{CapacityKind, Error, OperationContext, RuntimeLimits, RuntimeResources};

fn resources(requests: usize, bytes: usize) -> Arc<RuntimeResources> {
    RuntimeResources::new(RuntimeLimits {
        requests,
        producer_bytes: bytes,
        held_bytes: bytes,
    })
    .unwrap()
}

fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(1),
    )
}

#[test]
fn cloned_views_charge_actual_allocation_once_until_last_external_owner() {
    let resources = resources(2, 1024);
    let mut data = Vec::with_capacity(1024);
    data.extend_from_slice(b"0123456789");
    let pointer = data.as_ptr();
    let lease = resources.retain_vec(data, 42, 81, 3, 100).unwrap();
    assert_eq!(lease.as_slice().as_ptr(), pointer);
    let tail = lease.slice(4..10).unwrap();
    assert_eq!(tail.metadata().logical_offset, 104);
    assert_eq!(tail.metadata().length, 6);
    assert_eq!(tail.metadata().allocation_capacity, 1024);
    assert!(matches!(
        resources.retain_vec(vec![1], 43, 82, 0, 0),
        Err(Error::Capacity(CapacityKind::HeldBuffers))
    ));
    let external = tail.into_bytes();
    drop(lease);
    assert_eq!(resources.status().held_bytes, 1024);
    resources.shutdown();
    assert_eq!(external, b"456789"[..]);
    drop(external);
    assert_eq!(resources.status().held_bytes, 0);
    assert_eq!(resources.status().held_bytes_high_water, 1024);
}

#[test]
fn frozen_native_input_retains_producer_credit_and_backing_after_shutdown() {
    let pool = BufferPool::new(2);
    let server = Arc::new(RpcServer::new(Some(&pool)));
    server.configure_rdma(&RdmaConfig::default()).unwrap();
    let allocator = ProducerBufferAllocator::new(Arc::clone(&server), false);
    let resources = resources(1, 512);
    let writable = resources.allocate(&allocator, 512).unwrap();
    let lease = writable
        .initialize_with(127, 0xAB, |bytes| {
            bytes[..8].copy_from_slice(&42_u64.to_le_bytes());
        })
        .unwrap();
    assert_eq!(lease.metadata().allocation_capacity, 512);
    assert!(matches!(
        resources.allocate(&allocator, 1),
        Err(Error::Capacity(CapacityKind::Producers))
    ));
    let final_view = lease.slice(8..127).unwrap().into_bytes();
    drop(lease);
    resources.shutdown();
    drop(allocator);
    drop(server);
    drop(pool);
    assert_eq!(resources.status().producer_bytes, 512);
    assert!(final_view.iter().all(|byte| *byte == 0xAB));
    drop(final_view);
    assert_eq!(resources.status().producer_bytes, 0);
}

#[tokio::test]
async fn request_admission_cancellation_and_shared_shutdown_are_independent_of_held_bytes() {
    let resources = resources(1, 64);
    let first_context = context(1);
    let first = resources.admit(first_context.clone()).unwrap();
    assert!(matches!(
        resources.admit(context(2)),
        Err(Error::Capacity(CapacityKind::Requests))
    ));
    let lease = resources.retain_vec(vec![1; 64], 1, 1, 0, 0).unwrap();
    first_context.cancellation.cancel();
    assert_eq!(
        first.run(std::future::pending::<Result<(), Error>>()).await,
        Err(Error::Cancelled)
    );
    drop(first);
    assert_eq!(resources.status().requests, 0);
    assert_eq!(resources.status().held_bytes, 64);
    let second = resources.admit(context(2)).unwrap();
    let running = tokio::spawn(async move { second.run(std::future::pending::<Result<(), Error>>()).await });
    tokio::task::yield_now().await;
    resources.shutdown();
    assert_eq!(running.await.unwrap(), Err(Error::Shutdown));
    assert!(matches!(resources.admit(context(3)), Err(Error::Shutdown)));
    assert_eq!(lease.as_slice(), &[1; 64]);
    drop(lease);
    assert_eq!(resources.status().held_bytes, 0);
}

#[tokio::test]
async fn total_deadline_stops_a_wait_without_returning_caller_held_credit() {
    let resources = resources(1, 8);
    let context = OperationContext::new(
        NonZeroU64::new(7).unwrap(),
        Instant::now() + Duration::from_millis(10),
    );
    let request = resources.admit(context).unwrap();
    let held = resources.retain_vec(vec![9; 8], 7, 7, 0, 0).unwrap();
    assert_eq!(
        request.run(std::future::pending::<Result<(), Error>>()).await,
        Err(Error::Deadline)
    );
    drop(request);
    assert_eq!(resources.status().held_bytes, 8);
    drop(held);
    assert_eq!(resources.status().held_bytes, 0);
}

#[test]
fn rejected_initialized_prefix_returns_storage_and_producer_admission() {
    let pool = BufferPool::new(1);
    let server = Arc::new(RpcServer::new(Some(&pool)));
    let allocator = ProducerBufferAllocator::new(server, false);
    let resources = resources(1, 64);
    assert!(resources
        .allocate(&allocator, 64)
        .unwrap()
        .initialize_with(65, 0, |_| {})
        .is_err());
    assert_eq!(resources.status().producer_bytes, 0);
    assert!(resources.allocate(&allocator, 64).is_ok());
}

#[tokio::test]
async fn asynchronous_copy_retains_host_storage_beyond_stream_and_runtime_close() {
    let resources = resources(1, 1024);
    let host = resources.retain_vec(vec![0xAB; 1024], 90, 91, 0, 0).unwrap();
    let (complete_copy, copied) = tokio::sync::oneshot::channel();
    let copy = tokio::spawn(async move {
        copied.await.unwrap();
        assert!(host.as_slice().iter().all(|byte| *byte == 0xAB));
    });
    resources.shutdown();
    assert_eq!(resources.status().held_bytes, 1024);
    complete_copy.send(()).unwrap();
    copy.await.unwrap();
    assert_eq!(resources.status().held_bytes, 0);
}

#[tokio::test]
async fn an_interrupted_mutation_exposes_uncertainty_and_never_replays() {
    let resources = resources(1, 8);
    let context = context(81);
    let cancellation = context.cancellation.clone();
    let request = resources.admit(context).unwrap();
    let (accepted, accepting) = tokio::sync::oneshot::channel();
    let mutation = tokio::spawn(async move {
        request
            .run_mutation(async {
                accepted.send(()).unwrap();
                std::future::pending::<Result<(), Error>>().await
            })
            .await
    });
    accepting.await.unwrap();
    cancellation.cancel();
    assert_eq!(
        mutation.await.unwrap(),
        Err(Error::UncertainMutation {
            request_id: 81,
            reason: crowdb_sdk_native::error::MutationInterruption::Cancelled,
        })
    );
    assert_eq!(resources.status().requests, 0);
}
