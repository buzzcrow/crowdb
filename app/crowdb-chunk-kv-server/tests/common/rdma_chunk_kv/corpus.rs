// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_client::{ChunkKvRpcTransport, ChunkKvTransport};
use crowdb_protocol::chunk_kv::{
    BatchMutationItem, BatchMutationRequest, ChunkKvResponse, ChunkKvRpcErrorCode, ClientRequestId, Id128,
    MultiGetRequest, OperationResult, PartitionRouting, PointOperation, PointRequest, RequestRouting,
    RpcCompareCondition, RpcValue, ScanDirection, ScanRequest, SeekKind, SeekRequest,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::service::{EPOCH, PARTITION};

pub(super) fn routing(sequence: u64) -> RequestRouting {
    RequestRouting {
        request_id: ClientRequestId {
            client_instance_id: Id128 { high: 20, low: 21 },
            client_sequence: sequence,
        },
        map_revision: 1,
        partition_id: PARTITION,
        owner_epoch: EPOCH,
        min_journal_position: None,
        deadline_ms: None,
    }
}

fn records() -> Vec<(Vec<u8>, Vec<u8>)> {
    [
        63 * 1024,
        65_553,
        255 * 1024,
        262_175,
        768 * 1024,
        1_048_583,
        300 * 1024,
        600 * 1024,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, size)| {
        let key = format!("k{index:02}").into_bytes();
        let value = (0..size)
            .map(|offset| u8::try_from(offset % 251).unwrap() ^ u8::try_from(index).unwrap())
            .collect();
        (key, value)
    })
    .collect()
}

fn values(response: ChunkKvResponse) -> (Vec<RpcValue>, Option<crowdb_protocol::chunk_kv::ScanContinuation>) {
    match response.result.unwrap() {
        OperationResult::Scan { items, continuation } => (items, continuation),
        other => panic!("expected scan response: {other:?}"),
    }
}

pub(super) fn scan() -> ScanRequest {
    ScanRequest {
        routing: routing(300),
        start: None,
        end: None,
        direction: ScanDirection::Forward,
        limit: 100,
        continuation: None,
    }
}

pub(super) async fn run(transport: &ChunkKvRpcTransport, endpoint: &str) -> Value {
    let records = records();
    let results = populate(transport, endpoint, &records).await;
    check_conditions(transport, endpoint, &records[0]).await;
    let mut expected = Vec::new();
    for (index, (key, value)) in records.iter().enumerate() {
        let response = transport
            .point(
                endpoint,
                &PointRequest {
                    routing: routing(50 + u64::try_from(index).unwrap()),
                    operation: PointOperation::Get { key: key.clone() },
                },
            )
            .await
            .unwrap();
        match response.result.unwrap() {
            OperationResult::Value(Some(found)) => {
                assert_eq!(&found.key, key);
                assert_eq!(&found.value, value);
                expected.push(found);
            }
            other => panic!("expected stored value: {other:?}"),
        }
    }
    check_groups(transport, endpoint, &expected).await;
    let error = transport
        .point(
            endpoint,
            &PointRequest {
                routing: RequestRouting {
                    deadline_ms: Some(1),
                    ..routing(200)
                },
                operation: PointOperation::Get {
                    key: records[0].0.clone(),
                },
            },
        )
        .await
        .unwrap()
        .result
        .unwrap_err();
    assert_eq!(error.code, ChunkKvRpcErrorCode::RequestExpired);
    let mut stale = scan();
    stale.continuation = Some(crowdb_protocol::chunk_kv::ScanContinuation {
        direction: ScanDirection::Forward,
        last_key: b"k00".to_vec(),
        partition_id: PARTITION,
        owner_epoch: EPOCH + 1,
        map_revision: 1,
    });
    let stale = transport.scan(endpoint, &stale).await.unwrap().result;
    assert!(stale.is_err(), "stale continuation must not produce a scan");
    assert_eq!(stale.unwrap_err().code, ChunkKvRpcErrorCode::RefreshRequired);
    json!({
        "records": records.len(), "payload_bytes": records.iter().map(|(_, v)| v.len()).sum::<usize>(),
        "value_sizes_bytes": records.iter().map(|(_, value)| value.len()).collect::<Vec<_>>(),
        "values_sha256": format!("{:x}", Sha256::digest(serde_json::to_vec(&expected).unwrap())),
        "mutation_results_sha256": format!("{:x}", Sha256::digest(serde_json::to_vec(&results).unwrap())),
        "expected_typed_errors": 2, "typed_errors": [error.code, ChunkKvRpcErrorCode::RefreshRequired], "logical_mutation_sequences": (1..=8).collect::<Vec<_>>(),
        "errors": 0, "verification_errors": 0,
    })
}

async fn populate(
    transport: &ChunkKvRpcTransport,
    endpoint: &str,
    records: &[(Vec<u8>, Vec<u8>)],
) -> Vec<Value> {
    let mut results = Vec::new();
    for (index, (key, value)) in records.iter().take(6).enumerate() {
        let request = PointRequest {
            routing: routing(u64::try_from(index).unwrap() + 1),
            operation: PointOperation::Put {
                key: key.clone(),
                value: value.clone(),
            },
        };
        let result = transport.point(endpoint, &request).await.unwrap();
        assert!(matches!(
            result.result,
            Ok(OperationResult::Mutation { applied: true, .. })
        ));
        assert_eq!(
            transport.point(endpoint, &request).await.unwrap(),
            result,
            "stable mutation identity must deduplicate"
        );
        results.push(serde_json::to_value(result).unwrap());
    }
    let batch = BatchMutationRequest {
        routing: PartitionRouting {
            map_revision: 1,
            partition_id: PARTITION,
            owner_epoch: EPOCH,
            deadline_ms: None,
        },
        operations: records
            .iter()
            .enumerate()
            .skip(6)
            .map(|(index, (key, value))| BatchMutationItem {
                request_id: routing(u64::try_from(index).unwrap() + 1).request_id,
                operation: PointOperation::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
            })
            .collect(),
    };
    let result = transport.batch_mutate(endpoint, &batch).await.unwrap();
    assert_eq!(transport.batch_mutate(endpoint, &batch).await.unwrap(), result);
    for (item, request) in result.result.as_ref().unwrap().iter().zip(&batch.operations) {
        assert_eq!(item.request_id, request.request_id);
        assert!(matches!(
            item.result,
            Ok(OperationResult::Mutation { applied: true, .. })
        ));
    }
    results.push(serde_json::to_value(result).unwrap());
    results
}

async fn check_conditions(transport: &ChunkKvRpcTransport, endpoint: &str, record: &(Vec<u8>, Vec<u8>)) {
    let response = transport
        .point(
            endpoint,
            &PointRequest {
                routing: routing(100),
                operation: PointOperation::PutIfAbsent {
                    key: record.0.clone(),
                    value: b"wrong".to_vec(),
                },
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(response.result, Ok(OperationResult::Mutation { applied: false, observed: Some(ref v), .. }) if v.value == record.1)
    );
    let response = transport
        .point(
            endpoint,
            &PointRequest {
                routing: routing(101),
                operation: PointOperation::CompareExchange {
                    key: record.0.clone(),
                    condition: RpcCompareCondition::Value(b"wrong".to_vec()),
                    value: b"wrong".to_vec(),
                },
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        response.result,
        Ok(OperationResult::Mutation { applied: false, .. })
    ));
    for (sequence, operation) in [
        (
            102,
            PointOperation::Put {
                key: b"temp".to_vec(),
                value: b"value".to_vec(),
            },
        ),
        (
            103,
            PointOperation::ConditionalDelete {
                key: b"temp".to_vec(),
                condition: RpcCompareCondition::Value(b"value".to_vec()),
            },
        ),
        (
            104,
            PointOperation::Delete {
                key: b"missing".to_vec(),
            },
        ),
    ] {
        let response = transport
            .point(
                endpoint,
                &PointRequest {
                    routing: routing(sequence),
                    operation,
                },
            )
            .await
            .unwrap();
        assert!(matches!(response.result, Ok(OperationResult::Mutation { .. })));
    }
}

async fn check_groups(transport: &ChunkKvRpcTransport, endpoint: &str, expected: &[RpcValue]) {
    let indices = [7, 0, 7, 2];
    let request = MultiGetRequest {
        routing: routing(210),
        keys: indices.iter().map(|&i| expected[i].key.clone()).collect(),
    };
    let result = transport
        .multi_get(endpoint, &request)
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(
        result,
        indices
            .iter()
            .map(|&i| Some(expected[i].clone()))
            .collect::<Vec<_>>()
    );
    let seek = SeekRequest {
        routing: routing(211),
        key: b"k03".to_vec(),
        kind: SeekKind::Ceiling,
    };
    assert_eq!(
        transport.seek(endpoint, &seek).await.unwrap().result.unwrap(),
        OperationResult::Value(Some(expected[3].clone()))
    );
    let (items, continuation) = values(transport.scan(endpoint, &scan()).await.unwrap());
    assert_eq!(items, expected);
    assert!(continuation.is_none());
    let mut reverse = scan();
    reverse.direction = ScanDirection::Reverse;
    assert_eq!(
        values(transport.scan(endpoint, &reverse).await.unwrap()).0,
        expected.iter().rev().cloned().collect::<Vec<_>>()
    );
    let mut page = scan();
    page.limit = 2;
    let mut paged = Vec::new();
    loop {
        let (items, continuation) = values(transport.scan(endpoint, &page).await.unwrap());
        assert!(items.len() <= 2);
        paged.extend(items);
        if continuation.is_none() {
            break;
        }
        page.continuation = continuation;
    }
    assert_eq!(paged, expected);
    let (bounded, continuation) = values(
        transport
            .scan_bounded(endpoint, &scan(), 600 * 1024)
            .await
            .unwrap(),
    );
    assert!(!bounded.is_empty());
    assert!(bounded.len() < expected.len());
    assert!(continuation.is_some());
    assert_eq!(bounded, expected[..bounded.len()]);
}
