// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    ChunkKvProtocolError, ChunkKvResponse, ChunkKvRpcErrorCode, ClientRequestId, Id128, OwnerHint,
    PointOperation, PointRequest, RequestRouting, RpcCompareCondition, RpcFailure,
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
