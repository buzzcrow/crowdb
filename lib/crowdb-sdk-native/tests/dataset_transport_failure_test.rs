// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_transport.rs"]
mod fixture;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/rpc_read.rs"]
mod rpc;
#[path = "common/read_storage.rs"]
#[allow(dead_code)] // Reuses the shared data backend, without its S3 facade fixture.
mod storage;

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use fixture::{context, TestDatasetTransport};

async fn partial_read_failure(rdma: bool, cancel: bool) {
    let backend = Arc::new(storage::TestReadStorage::new());
    let server = rpc::TestRpcReadServer::start(&backend, rdma);
    // TCP has its documented retry behavior; the hardware case enables retries
    // and still requires an accepted RDMA read to remain unreplayed.
    let disk = if rdma {
        server.client_with_retries(3, false)
    } else {
        server.client()
    };
    let fixture = TestDatasetTransport::new(backend.clone(), disk.clone()).await;
    let operation = context();
    let cancellation = operation.cancellation.clone();
    let mut read = fixture
        .client
        .read_batch(operation, fixture.request())
        .await
        .unwrap();
    let first = read.body.next().await.unwrap().unwrap();
    assert_eq!(first.field, 0);
    let held = first.buffer;
    assert_eq!(held.as_slice(), b"original");
    assert_eq!(held.metadata().registration_id != 0, rdma);
    let capacity = held.metadata().allocation_capacity;
    let resources = fixture.runtime.resources().clone();
    assert_eq!(fixture.runtime.resources().status().held_bytes, capacity);
    assert_eq!(fixture.runtime.resources().status().requests, 1);
    let before = disk.status();
    server.reply_enabled.store(false, Ordering::Release);
    let pending = tokio::spawn(async move {
        let error = read.body.next().await.err().expect("second sample must fail");
        (read, error)
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.requests.load(Ordering::Acquire) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second storage RPC was not accepted");
    let queries = backend.queries.load(Ordering::Acquire);
    let metadata_reads = fixture.gate.reads.load(Ordering::Acquire);
    let bounded_reads = fixture.gate.bounded_reads.load(Ordering::Acquire);
    let mutations = fixture.gate.calls.load(Ordering::Acquire);
    if cancel {
        cancellation.cancel();
    } else {
        server.server.stop();
    }
    let (mut read, error) = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
    fixture::assert_failure(&error, cancel);
    let terminal = fixture::terminal_rpc_counts(&disk, before, rdma || cancel);
    assert_eq!(
        read.body.next().await.err().expect("failure must remain sticky"),
        error
    );
    read.body.cancel();
    assert_eq!(
        read.body
            .next()
            .await
            .err()
            .expect("cancel must not replace an existing failure"),
        error
    );
    assert!(!read.body.is_complete());
    assert_eq!(fixture.runtime.resources().status().requests, 0);
    assert_eq!(fixture.runtime.resources().status().held_bytes, capacity);
    assert_eq!(fixture.runtime.resources().status().compatibility_copy_bytes, 0);
    let after = disk.status();
    fixture::assert_rpc_counts(after, terminal);
    assert_eq!(after.inflight, 0);
    assert_eq!(backend.queries.load(Ordering::Acquire), queries);
    assert_eq!(
        backend.reads.load(Ordering::Acquire),
        0,
        "no direct backend fallback"
    );
    assert_eq!(server.requests.load(Ordering::Acquire), 2);
    assert_eq!(server.replies.load(Ordering::Acquire), 1);
    assert_eq!(fixture.gate.reads.load(Ordering::Acquire), metadata_reads);
    assert_eq!(fixture.gate.bounded_reads.load(Ordering::Acquire), bounded_reads);
    assert_eq!(fixture.gate.calls.load(Ordering::Acquire), mutations);
    fixture.runtime.shutdown().await.unwrap();
    drop(read);
    drop(fixture);
    drop(disk);
    drop(server);
    assert_eq!(held.as_slice(), b"original");
    drop(held);
    assert_eq!(resources.status().held_bytes, 0);
    assert_eq!(resources.status().requests, 0);
}

#[tokio::test]
async fn tcp_partial_dataset_read_keeps_delivered_owner_and_sticky_storage_failure() {
    partial_read_failure(false, false).await;
}

#[tokio::test]
async fn cancellation_of_an_accepted_dataset_rpc_releases_admission_and_keeps_the_first_owner() {
    partial_read_failure(false, true).await;
}

#[tokio::test]
async fn hardware_accepted_dataset_read_failure_is_sticky_without_replay() {
    if std::env::var_os("CROWDB_RDMA_E2E").is_some() {
        partial_read_failure(true, false).await;
    }
}
