// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crate::common::hardware::UNIT_SIZE_BYTES;
use bytes::Bytes;
use crowdb_diskio_client::{
    DiskId, DiskioClient, DiskioClientConfig, DiskioError, Durability, SegmentTarget,
};
pub(super) async fn assert_large_view_range(client: &DiskioClient, target: SegmentTarget) {
    let count = crowdb_rpc_ffi::BufferChain::maximum_views() * 2 + 1;
    let views: Vec<_> = (0..count)
        .map(|index| Bytes::from(vec![u8::try_from(index).unwrap(); 32]))
        .collect();
    let writes_before = client.status().write_operations;
    let syncs_before = client.status().fsync_operations;
    client
        .write_views(
            target,
            16384,
            views.clone(),
            Durability::Fsync,
            client.normal_options(),
        )
        .await
        .expect("multiple bounded view frames");
    assert_eq!(client.status().write_operations - writes_before, 3);
    // Synchronized frames return durable completion without an extra fsync RPC.
    assert_eq!(client.status().fsync_operations - syncs_before, 0);
    let read = client
        .read(
            target,
            16384,
            u32::try_from(count * 32).unwrap(),
            client.normal_options(),
        )
        .await
        .expect("complete view range read");
    assert_eq!(read.as_ref(), views.concat());
    let invalid = client
        .write_views(
            target,
            target.capacity() - 1,
            views,
            Durability::Buffered,
            client.normal_options(),
        )
        .await;
    assert!(matches!(invalid, Err(DiskioError::InvalidInput(_))));
    assert_eq!(client.status().write_operations - writes_before, 3);
}

pub(super) async fn assert_independent_strip_writes_overlap() {
    let client = Arc::new(
        DiskioClient::connect_with_clients(
            super::registry(),
            super::hardware_client(),
            DiskioClientConfig {
                max_pending_calls: 2,
                ..DiskioClientConfig::default()
            },
        )
        .await
        .unwrap(),
    );
    let writing = client.clone();
    let count = crowdb_rpc_ffi::BufferChain::maximum_views() * 2;
    // Distinct allocated disk blocks belong to independently owned strips.
    let targets =
        [8, 12].map(|unit| SegmentTarget::new(DiskId::new(0, 1), 0, unit, 1, UNIT_SIZE_BYTES).unwrap());
    let payload = vec![Bytes::from_static(b"independent-strip"); count];
    let expected = payload.concat();
    let pending = tokio::spawn(async move {
        let first = writing.write_views(
            targets[0],
            0,
            payload.clone(),
            Durability::Fsync,
            writing.normal_options(),
        );
        let second = writing.write_views(
            targets[1],
            0,
            payload,
            Durability::Fsync,
            writing.normal_options(),
        );
        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        second.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.status().inflight < 2 {
            assert!(!pending.is_finished(), "independent strip writes were serialized");
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    pending.await.unwrap();
    assert_eq!(client.status().write_operations, 4);
    assert_eq!(client.status().fsync_operations, 0);
    for target in targets {
        let read = client
            .read(
                target,
                0,
                u32::try_from(expected.len()).unwrap(),
                client.normal_options(),
            )
            .await
            .unwrap();
        assert_eq!(read.as_ref(), expected);
    }
}

pub(super) async fn assert_view_frames_are_ordered(target: SegmentTarget, aligned: bool) {
    let client = Arc::new(
        DiskioClient::connect_with_clients(
            super::registry(),
            super::hardware_client(),
            DiskioClientConfig {
                max_pending_calls: 2,
                ..DiskioClientConfig::default()
            },
        )
        .await
        .unwrap(),
    );
    let writing = client.clone();
    let count = crowdb_rpc_ffi::BufferChain::maximum_views() * 2;
    // Alignment does not relax the ordering contract within one strip block.
    let payload = if aligned {
        vec![Bytes::from(vec![0x5a; UNIT_SIZE_BYTES as usize / (count / 2)]); count]
    } else {
        vec![Bytes::from_static(b"partial-view"); count]
    };
    let offset = if aligned {
        2 * u64::from(UNIT_SIZE_BYTES)
    } else {
        32768
    };
    let expected = payload.concat();
    let pending = tokio::spawn(async move {
        writing
            .write_views(
                target,
                offset,
                payload,
                Durability::Fsync,
                writing.normal_options(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !pending.is_finished() {
            assert!(
                client.status().inflight <= 1,
                "same-strip disk-block frames overlapped"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    pending.await.unwrap().unwrap();
    assert_eq!(client.status().write_operations, 2);
    assert_eq!(client.status().fsync_operations, 0);
    let read = client
        .read(
            target,
            offset,
            u32::try_from(expected.len()).unwrap(),
            client.normal_options(),
        )
        .await
        .unwrap();
    assert_eq!(read.as_ref(), expected);
}
