// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/synchronized_peer.rs"]
mod peer;

use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskioError, Durability, OperationOptions, SegmentTarget};
use crowdb_protocol::diskio_fb::FBDiskIoRetCode;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn drain_requires_a_proven_empty_ordered_synchronized_request() {
    for proof in [true, false] {
        let peer = peer::TestDiskioPeer::start(proof, proof, FBDiskIoRetCode::Success).await;
        let target = SegmentTarget::new(DiskId::new(1, 2), 0, 0, 1, 4096).unwrap();
        let client = peer.client();
        let operation = tokio::spawn(async move {
            client
                .drain_segment(target, OperationOptions::within(Duration::from_secs(3)))
                .await
        });
        peer.wait_for_write().await;
        if proof {
            assert!(!operation.is_finished());
            peer.gate.notify_one();
        }
        let result = operation.await.unwrap();
        if proof {
            result.unwrap();
        } else {
            assert!(
                matches!(result, Err(DiskioError::DurabilityFailure(_))),
                "{result:?}"
            );
        }
        assert_eq!(peer.counts.writes.load(Ordering::Acquire), 1);
        assert_eq!(peer.counts.sync_requests.load(Ordering::Acquire), 1);
        assert_eq!(peer.counts.bytes.load(Ordering::Acquire), 0);
        assert_eq!(peer.counts.fsyncs.load(Ordering::Acquire), 0);
    }
}

#[tokio::test]
async fn an_empty_data_write_cannot_implicitly_become_a_drain() {
    let peer = peer::TestDiskioPeer::start(true, false, FBDiskIoRetCode::Success).await;
    let target = SegmentTarget::new(DiskId::new(1, 2), 0, 0, 1, 4096).unwrap();
    let result = peer
        .client()
        .write(
            target,
            0,
            Bytes::new(),
            Durability::Fsync,
            OperationOptions::within(Duration::from_secs(3)),
        )
        .await;
    assert!(matches!(result, Err(DiskioError::InvalidInput(_))), "{result:?}");
    assert_eq!(peer.counts.writes.load(Ordering::Acquire), 0);
}
