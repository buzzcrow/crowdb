// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/point_payload.rs"]
mod support;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_kv_client::{ClientConfig, ClientError};
use crowdb_protocol::chunk_kv::{
    ChunkKvResponse, ChunkKvRpcErrorCode, ClientRequestId, Id128, OperationResult, PointOperation,
    RpcCompareCondition, RpcFailure, RpcJournalPosition, RpcValue,
};
use support::{client, TestTransport};

fn identity() -> ClientRequestId {
    ClientRequestId {
        client_instance_id: Id128 { high: 7, low: 9 },
        client_sequence: 11,
    }
}

fn response(result: OperationResult) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision: 3,
        journal_position: None,
        result: Ok(result),
    }
}

fn retry(code: ChunkKvRpcErrorCode) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision: 1,
        journal_position: None,
        result: Err(RpcFailure {
            code,
            message: "retry".into(),
            retry_after_ms: Some(1),
            latest_map_revision: Some(3),
            owner_hint: None,
        }),
    }
}

fn large_cas() -> PointOperation {
    PointOperation::CompareExchange {
        key: b"upload-record".to_vec(),
        condition: RpcCompareCondition::Value(vec![3; 245_784]),
        value: vec![5; 245_784],
    }
}

#[tokio::test]
async fn first_dispatch_preserves_original_buffers_for_every_point_variant() {
    let operations = vec![
        PointOperation::Get { key: b"get".to_vec() },
        PointOperation::Put {
            key: b"put".to_vec(),
            value: vec![7; 245_784],
        },
        PointOperation::Delete {
            key: b"delete".to_vec(),
        },
        PointOperation::PutIfAbsent {
            key: b"absent".to_vec(),
            value: vec![9; 245_784],
        },
        large_cas(),
        PointOperation::CompareExchange {
            key: b"revision".to_vec(),
            condition: RpcCompareCondition::Revision(17),
            value: vec![11; 1024],
        },
        PointOperation::ConditionalDelete {
            key: b"conditional".to_vec(),
            condition: RpcCompareCondition::Value(vec![13; 245_784]),
        },
        PointOperation::ConditionalDelete {
            key: b"revision-delete".to_vec(),
            condition: RpcCompareCondition::Revision(19),
        },
        PointOperation::Put {
            key: Vec::new(),
            value: Vec::new(),
        },
    ];
    for operation in operations {
        let expected = response(OperationResult::Value(None));
        let transport = Arc::new(TestTransport::new(&operation, vec![Ok(expected.clone())]));
        let (client, _) = client(transport.clone(), None);
        assert_eq!(
            client
                .execute_with_identity(operation, None, identity())
                .await
                .unwrap(),
            expected
        );
        assert_eq!(transport.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn large_cas_keeps_payload_identity_deadline_and_condition_across_reroutes() {
    let operation = large_cas();
    let expected = response(OperationResult::Mutation {
        applied: false,
        revision: None,
        observed: Some(RpcValue {
            key: b"upload-record".to_vec(),
            value: vec![17; 245_784],
            revision: 23,
        }),
    });
    let transport = Arc::new(TestTransport::new(
        &operation,
        vec![
            Err(ClientError::Transport("response lost".into())),
            Ok(retry(ChunkKvRpcErrorCode::NotMyRange)),
            Ok(expected.clone()),
        ],
    ));
    let (client, catalog) = client(transport.clone(), None);
    let minimum = Some(RpcJournalPosition {
        stream_name: Id128 { high: 1, low: 3 },
        offset: 29,
    });
    assert_eq!(
        client
            .execute_with_identity(operation, minimum, identity())
            .await
            .unwrap(),
        expected
    );
    let calls = transport.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        3,
        "condition conflicts must not issue confirmation GETs"
    );
    assert_eq!(catalog.loads.load(Ordering::Relaxed), 3);
    for (index, (endpoint, routing)) in calls.iter().enumerate() {
        let generation = index as u64 + 1;
        assert_eq!(endpoint, &format!("owner-{generation}"));
        assert_eq!(routing.map_revision, generation);
        assert_eq!(
            routing.partition_id,
            Id128 {
                high: 1,
                low: generation
            }
        );
        assert_eq!(routing.owner_epoch, generation + 10);
        assert_eq!(routing.request_id, identity());
        assert_eq!(routing.min_journal_position, minimum);
        assert_eq!(routing.deadline_ms, calls[0].1.deadline_ms);
        assert!(routing.deadline_ms.is_some());
    }
}

#[tokio::test]
async fn transient_responses_reuse_the_payload_and_warm_route() {
    let operation = large_cas();
    let expected = response(OperationResult::Mutation {
        applied: true,
        revision: Some(31),
        observed: None,
    });
    let replies = [
        ChunkKvRpcErrorCode::Overloaded,
        ChunkKvRpcErrorCode::WriteStalled,
        ChunkKvRpcErrorCode::Recovering,
    ]
    .into_iter()
    .map(|code| Ok(retry(code)))
    .chain([Ok(expected.clone())])
    .collect();
    let transport = Arc::new(TestTransport::new(&operation, replies));
    let (client, catalog) = client(transport.clone(), None);
    assert_eq!(
        client
            .execute_with_identity(operation, None, identity())
            .await
            .unwrap(),
        expected
    );
    assert_eq!(transport.calls.lock().unwrap().len(), 4);
    assert_eq!(catalog.loads.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn timed_out_transport_drops_its_borrow_without_another_attempt() {
    let operation = large_cas();
    let transport = Arc::new(TestTransport::new(&operation, Vec::new()));
    let (client, _) = client(
        transport.clone(),
        Some(ClientConfig {
            operation_timeout: Duration::from_millis(50),
            ..ClientConfig::default()
        }),
    );
    assert!(matches!(
        client.execute_with_identity(operation, None, identity()).await,
        Err(ClientError::Deadline)
    ));
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
    assert_eq!(transport.dropped_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn dropped_request_cancels_its_pending_transport_without_replaying() {
    let operation = large_cas();
    let transport = Arc::new(TestTransport::new(&operation, Vec::new()));
    let (client, _) = client(transport.clone(), None);
    let future = client.execute_with_identity(operation, None, identity());
    let mut future = Box::pin(future);
    assert!(matches!(futures::poll!(&mut future), std::task::Poll::Pending));
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
    assert_eq!(transport.dropped_calls.load(Ordering::Relaxed), 0);
    drop(future);
    assert_eq!(transport.dropped_calls.load(Ordering::Relaxed), 1);
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
}
