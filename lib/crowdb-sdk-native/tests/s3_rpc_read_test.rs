// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/rpc_read.rs"]
mod rpc;
#[path = "common/read_storage.rs"]
mod storage;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_client::RoutedDiskWriter;
use crowdb_sdk_native::s3::GetOptions;
use storage::{context, TestFixture, TestReadStorage};

async fn assert_rpc_delivery(rdma: bool) {
    let backend = Arc::new(TestReadStorage::new());
    let server = rpc::TestRpcReadServer::start(&backend, rdma);
    let disk = server.client();
    let fixture = TestFixture::with_disk(
        65_536,
        backend,
        Arc::new(RoutedDiskWriter::from_client(disk.clone())),
    )
    .await;
    fixture.publish(&fixture.record(0)).await;
    let mut first = fixture
        .client
        .get_object(context(2), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    let held = first.body.next().await.unwrap().unwrap();
    assert_eq!(held.as_slice(), b"original");
    assert_eq!(held.metadata().registration_id != 0, rdma);
    let capacity = held.metadata().allocation_capacity;
    if rdma {
        assert_eq!(capacity, 65_536);
    }
    assert!(first.body.next().await.unwrap().is_none());
    let mut second = fixture
        .client
        .get_object(context(3), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    if rdma {
        assert!(
            tokio::time::timeout(Duration::from_millis(10), second.body.next())
                .await
                .is_err()
        );
        assert_eq!(server.replies.load(Ordering::Acquire), 2);
    }
    drop(held);
    let lease = second.body.next().await.unwrap().unwrap();
    assert!(second.body.next().await.unwrap().is_none());
    let resources = fixture.runtime.resources().clone();
    assert_eq!(resources.status().compatibility_copy_bytes, 0);
    assert_eq!(resources.status().held_bytes, capacity);
    assert_eq!(disk.status().read_operations, 2);
    assert_eq!(disk.status().retries, 0);
    fixture.runtime.shutdown().await.unwrap();
    drop(first);
    drop(second);
    drop(fixture);
    drop(disk);
    drop(server);
    assert_eq!(lease.as_slice(), b"original");
    assert_eq!(resources.status().held_bytes, capacity);
    drop(lease);
    assert_eq!(resources.status().held_bytes, 0);
}

#[tokio::test]
async fn tcp_rpc_get_preserves_receive_allocation_through_final_sdk_release() {
    assert_rpc_delivery(false).await;
}

#[tokio::test]
async fn hardware_rdma_get_preserves_registered_receive_capacity_and_shutdown_lifetime() {
    if std::env::var_os("CROWDB_RDMA_E2E").is_some() {
        assert_rpc_delivery(true).await;
    }
}
