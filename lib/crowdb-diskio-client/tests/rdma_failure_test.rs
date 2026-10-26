// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_diskio_client::{
    DiskId, DiskioClient, DiskioClientConfig, DiskioError, Durability, SegmentTarget, TestDiskRoute,
};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{RdmaConfig, RdmaWireLayout, RpcServer};

#[derive(Clone, Copy)]
enum TestOperation {
    Read,
    Write,
    Fsync,
}

fn assert_terminal_error(operation: TestOperation, error: &DiskioError) {
    match operation {
        TestOperation::Write => assert!(matches!(error, DiskioError::AmbiguousWrite(_)), "{error:?}"),
        TestOperation::Read => assert!(
            matches!(error, DiskioError::AcceptedReadInterrupted(_)),
            "{error:?}"
        ),
        TestOperation::Fsync => assert!(matches!(error, DiskioError::TransportUnavailable(_)), "{error:?}"),
    }
}

fn test_rdma_config(address: &str) -> RdmaConfig {
    RdmaConfig {
        enabled: true,
        listen_enabled: true,
        allow_tcp_fallback: true,
        wire_layout: RdmaWireLayout::ControlData,
        wire_slot_capacity: (1 << 20) + 1024,
        sq_depth: 4,
        rq_depth: 4,
        posted_receive_target: 4,
        cq_depth: 16,
        send_arena_bytes: 4 << 20,
        receive_arena_bytes: 4 * ((1 << 20) + 1024),
        source_address: address.to_owned(),
        device: std::env::var("CROWDB_RDMA_DEVICE").unwrap_or_default(),
        ..RdmaConfig::default()
    }
}

async fn assert_accepted_rdma_failure(operation: TestOperation) {
    if std::env::var_os("CROWDB_RDMA_E2E").is_none() {
        return;
    }
    let address = std::env::var("CROWDB_RDMA_E2E_LISTEN_ADDR").unwrap();
    let config = test_rdma_config(&address);
    let server = RpcServer::new(None);
    server.configure_rdma(&config).unwrap();
    server.listen(&address, 0).unwrap();
    let received = Arc::new(AtomicU64::new(0));
    let observed = Arc::clone(&received);
    let message = match operation {
        TestOperation::Read => FBMsgType::EDiskReadRequest,
        TestOperation::Write => FBMsgType::EDiskWriteRequest,
        TestOperation::Fsync => FBMsgType::EDiskFsyncRequest,
    };
    server.register_handler(u16::try_from(message.0).unwrap(), move |request| {
        if matches!(operation, TestOperation::Write) {
            assert_eq!(request.data().unwrap(), b"accepted write");
        } else {
            assert!(request.data().is_none());
        }
        observed.fetch_add(1, Ordering::Release);
        // Drop the receive lease but deliberately send no durable reply.
    });
    server.start();
    let client = Arc::new(
        DiskioClient::connect_for_tests(
            vec![TestDiskRoute {
                disk_id: DiskId::new(7, 9),
                rack_id: 1,
                node_id: 1,
                disk_group_id: 1,
                instance_id: 1,
                endpoint: format!("{address}:{}", server.port()),
            }],
            DiskioClientConfig {
                rdma: Some(RdmaConfig {
                    listen_enabled: false,
                    ..config
                }),
                normal_connections_per_endpoint: 1,
                priority_connections_per_endpoint: 1,
                rpc_workers: 1,
                retry_attempts: 3,
                ..DiskioClientConfig::default()
            },
        )
        .unwrap(),
    );
    let baseline = client.status();
    let writer = Arc::clone(&client);
    let pending = tokio::spawn(async move {
        let target = SegmentTarget::new(DiskId::new(7, 9), 0, 0, 1, 1 << 20).unwrap();
        match operation {
            TestOperation::Read => writer
                .read(target, 0, 14, writer.normal_options())
                .await
                .map(|_| ()),
            TestOperation::Write => {
                writer
                    .write(
                        target,
                        0,
                        Bytes::from_static(b"accepted write"),
                        Durability::Buffered,
                        writer.normal_options(),
                    )
                    .await
            }
            TestOperation::Fsync => writer.fsync(DiskId::new(7, 9), writer.normal_options()).await,
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while received.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.stop();
    let error = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_terminal_error(operation, &error);
    let status = client.status();
    assert_eq!(received.load(Ordering::Acquire), 1);
    assert_eq!(status.retries, baseline.retries);
    assert_eq!(status.reconnect_attempts, baseline.reconnect_attempts);
    assert_eq!(
        status.ambiguous_writes,
        baseline.ambiguous_writes + u64::from(matches!(operation, TestOperation::Write))
    );
}

#[tokio::test]
async fn accepted_rdma_write_failure_does_not_reconnect_or_replay_on_tcp() {
    assert_accepted_rdma_failure(TestOperation::Write).await;
}

#[tokio::test]
async fn accepted_rdma_read_failure_does_not_reconnect_or_retry_on_tcp() {
    assert_accepted_rdma_failure(TestOperation::Read).await;
}

#[tokio::test]
async fn accepted_rdma_fsync_failure_does_not_reconnect_or_retry_on_tcp() {
    assert_accepted_rdma_failure(TestOperation::Fsync).await;
}
