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
use crowdb_sdk_native::s3::{GetOptions, LocalityOptions};
use crowdb_sdk_native::{error::ReadFailureKind, Error};
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
    let selected = fixture
        .client
        .resolve_object(context(101), b"bucket", b"key")
        .await
        .unwrap();
    let locality = fixture
        .client
        .object_locality(context(102), &selected, &LocalityOptions::default())
        .await
        .unwrap();
    assert!(locality.topology.disks.is_some());
    assert!(locality.topology.data_transport_json.is_some());
    let capability = locality
        .topology
        .transports
        .iter()
        .find(|group| group.group == crowdb_sdk_native::topology::TransportGroup::Data)
        .unwrap();
    assert_eq!(capability.rdma.is_some(), rdma);
    if let Some(capability) = &capability.rdma {
        assert!(capability.ready);
        assert_eq!(capability.data_slot_capacity, 65_536);
    }
    let placement = locality.extents[0].strips[0].fragments[0].disk.as_ref().unwrap();
    assert_eq!(placement.rack_id, 1);
    assert_eq!(placement.node_id, 1);
    assert_eq!(placement.disk_id, crowdb_diskio_client::DiskId::new(1, 1));
    assert_eq!(disk.status().read_operations, 0);
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
    let target =
        crowdb_diskio_client::SegmentTarget::new(crowdb_diskio_client::DiskId::new(1, 1), 0, 0, 1, 1024)
            .unwrap();
    assert!(matches!(
        disk.read(target, 0, 8, disk.normal_options()).await,
        Err(crowdb_diskio_client::DiskioError::TransportUnavailable(_))
    ));
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

#[tokio::test]
async fn hardware_accepted_rdma_get_failure_is_sticky_without_routed_or_replica_replay() {
    if std::env::var_os("CROWDB_RDMA_E2E").is_none() {
        return;
    }
    let backend = Arc::new(TestReadStorage::new());
    let server = rpc::TestRpcReadServer::start(&backend, true);
    let disk = server.client_with_retries(3, true);
    let fixture = TestFixture::with_disk(
        65_536,
        backend,
        Arc::new(RoutedDiskWriter::from_client(disk.clone())),
    )
    .await;
    fixture.publish(&fixture.record(0)).await;
    let mut successful = fixture
        .client
        .get_object(context(201), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    let held = successful.body.next().await.unwrap().unwrap();
    assert_ne!(held.metadata().registration_id, 0);
    assert!(successful.body.next().await.unwrap().is_none());
    server.reply_enabled.store(false, Ordering::Release);
    let baseline = disk.status();
    let mut interrupted = fixture
        .client
        .get_object(context(202), b"bucket", b"key", &GetOptions::default())
        .await
        .unwrap();
    let pending = tokio::spawn(async move {
        let error = interrupted.body.next().await.err().expect("read must fail");
        (interrupted, error)
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.requests.load(Ordering::Acquire) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.server.stop();
    let (mut interrupted, error) = pending.await.unwrap();
    assert!(
        matches!(
            error,
            Error::Read {
                kind: ReadFailureKind::Interrupted,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(
        interrupted.body.next().await.err().expect("failure must persist"),
        error
    );
    assert!(!interrupted.body.is_complete());
    let status = disk.status();
    assert_eq!(status.read_operations, baseline.read_operations + 1);
    assert_eq!(status.retries, baseline.retries);
    assert_eq!(status.connect_attempts, baseline.connect_attempts);
    assert_eq!(status.reconnect_attempts, baseline.reconnect_attempts);
    assert_eq!(server.requests.load(Ordering::Acquire), 2);
    assert_eq!(fixture.runtime.resources().status().requests, 0);
    fixture.runtime.shutdown().await.unwrap();
    assert_eq!(held.as_slice(), b"original");
}
