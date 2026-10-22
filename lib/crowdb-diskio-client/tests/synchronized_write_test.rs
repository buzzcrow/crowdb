// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/synchronized_peer.rs"]
mod peer;

use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskioError, Durability, OperationOptions, SegmentTarget};
use crowdb_protocol::diskio_fb::FBDiskIoRetCode;
use peer::TestDiskioPeer;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn target() -> SegmentTarget {
    SegmentTarget::new(DiskId::new(1, 2), 0, 0, 1024, 4096).unwrap()
}
fn options() -> OperationOptions {
    OperationOptions::within(Duration::from_secs(3))
}

#[tokio::test]
async fn synchronized_write_waits_for_its_proof_and_needs_no_fsync_rpc() {
    let peer = TestDiskioPeer::start(true, true, FBDiskIoRetCode::Success).await;
    let client = peer.client();
    let operation = tokio::spawn(async move {
        client
            .write(
                target(),
                0,
                Bytes::from_static(&[7; 32]),
                Durability::Fsync,
                options(),
            )
            .await
    });
    peer.wait_for_write().await;
    assert!(!operation.is_finished());
    assert_eq!(peer.counts.fsyncs.load(Ordering::Relaxed), 0);
    peer.gate.notify_one();
    operation.await.unwrap().unwrap();
    assert_eq!(peer.counts.writes.load(Ordering::Relaxed), 1);
    assert_eq!(peer.counts.sync_requests.load(Ordering::Relaxed), 1);
    assert_eq!(peer.counts.fsyncs.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn synchronized_response_without_proof_fails_without_extra_io() {
    let peer = TestDiskioPeer::start(false, false, FBDiskIoRetCode::Success).await;
    let error = peer
        .client()
        .write(
            target(),
            0,
            Bytes::from_static(&[7; 32]),
            Durability::Fsync,
            options(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, DiskioError::DurabilityFailure(_)));
    assert_eq!(peer.counts.writes.load(Ordering::Relaxed), 1);
    assert_eq!(peer.counts.sync_requests.load(Ordering::Relaxed), 1);
    assert_eq!(peer.counts.fsyncs.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn buffered_write_retains_no_flush_semantics() {
    let peer = TestDiskioPeer::start(true, false, FBDiskIoRetCode::Success).await;
    peer.client()
        .write(
            target(),
            0,
            Bytes::from_static(&[7; 32]),
            Durability::Buffered,
            options(),
        )
        .await
        .unwrap();
    assert_eq!(peer.counts.writes.load(Ordering::Relaxed), 1);
    assert_eq!(peer.counts.sync_requests.load(Ordering::Relaxed), 0);
    assert_eq!(peer.counts.fsyncs.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn bounded_view_splits_synchronize_only_the_final_ordered_request() {
    let peer = TestDiskioPeer::start(true, false, FBDiskIoRetCode::Success).await;
    let views = vec![Bytes::from_static(&[7]); 17];
    peer.client()
        .write_views(target(), 0, views, Durability::Fsync, options())
        .await
        .unwrap();
    assert_eq!(peer.counts.bytes.load(Ordering::Relaxed), 17);
    assert_eq!(peer.counts.writes.load(Ordering::Relaxed), 2);
    assert_eq!(peer.counts.sync_requests.load(Ordering::Relaxed), 1);
    assert_eq!(peer.counts.fsyncs.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn failed_flush_or_write_never_produces_success_or_a_fallback_request() {
    for code in [
        FBDiskIoRetCode::FsyncFailed,
        FBDiskIoRetCode::IoError,
        FBDiskIoRetCode::PartialWrite,
    ] {
        let peer = TestDiskioPeer::start(true, false, code).await;
        let error = peer
            .client()
            .write(
                target(),
                0,
                Bytes::from_static(&[7; 32]),
                Durability::Fsync,
                options(),
            )
            .await
            .unwrap_err();
        match code {
            FBDiskIoRetCode::FsyncFailed => assert!(matches!(error, DiskioError::DurabilityFailure(_))),
            FBDiskIoRetCode::IoError => assert!(matches!(error, DiskioError::DiskFailure(_))),
            _ => assert_eq!(error, DiskioError::PartialWrite),
        }
        assert_eq!(peer.counts.writes.load(Ordering::Relaxed), 1);
        assert_eq!(peer.counts.fsyncs.load(Ordering::Relaxed), 0);
    }
}
