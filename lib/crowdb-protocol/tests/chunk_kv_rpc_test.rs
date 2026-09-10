// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    BatchMutationItem, BatchMutationRequest, ChunkKvProtocolError, ChunkKvResponse, ChunkKvRpcErrorCode,
    ClientRequestId, Id128, KeyRange, MultiGetRequest, OwnerHint, PartitionRouting, PointOperation,
    PointRequest, RequestRouting, RpcCompareCondition, RpcFailure, ScanContinuation, ScanDirection,
    ScanRequest,
};

fn routing() -> RequestRouting {
    RequestRouting {
        request_id: ClientRequestId {
            client_instance_id: Id128 { high: 7, low: 8 },
            client_sequence: 9,
        },
        map_revision: 10,
        partition_id: Id128 { high: 11, low: 12 },
        owner_epoch: 13,
        min_journal_position: None,
        deadline_ms: Some(14),
    }
}

#[test]
fn scan_continuation_is_bound_to_direction_and_topology() {
    let mut request = ScanRequest {
        routing: routing(),
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
        direction: ScanDirection::Forward,
        limit: 10,
        continuation: Some(ScanContinuation {
            direction: ScanDirection::Forward,
            last_key: b"m".to_vec(),
            partition_id: routing().partition_id,
            owner_epoch: routing().owner_epoch,
            map_revision: routing().map_revision,
        }),
    };
    request.validate().unwrap();
    assert!(request.continuation_matches_topology());
    request.routing.owner_epoch += 1;
    assert!(!request.continuation_matches_topology());
    request.routing.owner_epoch -= 1;
    request.direction = ScanDirection::Reverse;
    assert!(!request.continuation_matches_topology());
}

#[test]
fn request_identity_and_authority_are_mandatory() {
    routing().validate().unwrap();
    let mut invalid = routing();
    invalid.request_id.client_sequence = 0;
    assert_eq!(invalid.validate(), Err(ChunkKvProtocolError::InvalidRpcRequest));
    invalid = routing();
    invalid.owner_epoch = 0;
    assert_eq!(invalid.validate(), Err(ChunkKvProtocolError::InvalidRpcRequest));
}

#[test]
fn mutation_round_trip_preserves_request_identity() {
    let request = PointRequest {
        routing: routing(),
        operation: PointOperation::CompareExchange {
            key: b"object/name".to_vec(),
            condition: RpcCompareCondition::Value(b"old".to_vec()),
            value: b"new".to_vec(),
        },
    };
    let encoded = bincode::serialize(&request).unwrap();
    let decoded: PointRequest = bincode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, request);
    assert!(decoded.operation.is_mutation());
    assert_eq!(decoded.operation.key(), b"object/name");
}

#[test]
fn typed_fence_outcome_survives_round_trip() {
    let response = ChunkKvResponse {
        map_revision: 18,
        journal_position: None,
        result: Err(RpcFailure {
            code: ChunkKvRpcErrorCode::NotMyRange,
            message: "former owner".into(),
            retry_after_ms: None,
            latest_map_revision: Some(18),
            owner_hint: Some(OwnerHint {
                instance_id: 22,
                rpc_endpoint: "127.0.0.1:9902".into(),
                owner_epoch: 6,
            }),
        }),
    };
    let encoded = bincode::serialize(&response).unwrap();
    let decoded: ChunkKvResponse = bincode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, response);
}

#[test]
fn grouped_requests_reject_cross_range_items_before_execution() {
    let range = KeyRange {
        start: b"a".to_vec(),
        end: Some(b"m".to_vec()),
    };
    let mut reads = MultiGetRequest {
        routing: routing(),
        keys: vec![b"b".to_vec(), b"l".to_vec()],
    };
    reads.validate_for_range(&range).unwrap();
    reads.keys.push(b"m".to_vec());
    assert_eq!(
        reads.validate_for_range(&range),
        Err(ChunkKvProtocolError::InvalidRpcRequest)
    );

    let mut writes = BatchMutationRequest {
        routing: PartitionRouting {
            map_revision: 10,
            partition_id: Id128 { high: 11, low: 12 },
            owner_epoch: 13,
            deadline_ms: Some(14),
        },
        operations: vec![BatchMutationItem {
            request_id: routing().request_id,
            operation: PointOperation::Put {
                key: b"b".to_vec(),
                value: b"value".to_vec(),
            },
        }],
    };
    writes.validate_for_range(&range).unwrap();
    writes.operations.push(BatchMutationItem {
        request_id: ClientRequestId {
            client_sequence: 10,
            ..routing().request_id
        },
        operation: PointOperation::Delete { key: b"z".to_vec() },
    });
    assert_eq!(
        writes.validate_for_range(&range),
        Err(ChunkKvProtocolError::InvalidRpcRequest)
    );
}
