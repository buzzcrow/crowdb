// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    ChunkKvResponse, ChunkKvRpcErrorCode, ClientRequestId, Id128, OperationResult, OwnerHint, PointOperation,
    PointRequest, RequestRouting, RpcCompareCondition, RpcFailure, RpcJournalPosition, RpcValue,
};
use crowdb_protocol::chunk_kv_wire::{
    decode_point_request, decode_point_response, encode_point_request, encode_point_response,
};

fn routing() -> RequestRouting {
    RequestRouting {
        request_id: ClientRequestId {
            client_instance_id: Id128 { high: 1, low: 2 },
            client_sequence: 3,
        },
        map_revision: 4,
        partition_id: Id128 { high: 5, low: 6 },
        owner_epoch: 7,
        min_journal_position: Some(RpcJournalPosition {
            stream_name: Id128 { high: 8, low: 9 },
            offset: 10,
        }),
        deadline_ms: Some(11),
    }
}

#[test]
fn point_request_round_trip_preserves_condition_and_routing() {
    let request = PointRequest {
        routing: routing(),
        operation: PointOperation::CompareExchange {
            key: Vec::new(),
            condition: RpcCompareCondition::Value(Vec::new()),
            value: Vec::new(),
        },
    };
    let (buffer, offset) = encode_point_request(12, 13, &request).unwrap();
    let decoded = decode_point_request(&buffer[offset..]).unwrap();
    assert_eq!(decoded.rpc_request_id, 12);
    assert_eq!(decoded.rpc_create_nano, 13);
    assert_eq!(decoded.request, request);
}

#[test]
fn point_success_round_trip_preserves_positions_and_values() {
    let response = ChunkKvResponse {
        map_revision: 14,
        journal_position: Some(RpcJournalPosition {
            stream_name: Id128 { high: 15, low: 16 },
            offset: 17,
        }),
        result: Ok(OperationResult::Mutation {
            applied: false,
            revision: None,
            observed: Some(RpcValue {
                key: Vec::new(),
                value: Vec::new(),
                revision: 18,
            }),
        }),
    };
    let (buffer, offset) = encode_point_response(19, 20, &response).unwrap();
    assert_eq!(decode_point_response(&buffer[offset..]).unwrap(), response);
}

#[test]
fn point_failure_round_trip_preserves_redirect_fields() {
    let response = ChunkKvResponse {
        map_revision: 21,
        journal_position: None,
        result: Err(RpcFailure {
            code: ChunkKvRpcErrorCode::NotMyRange,
            message: "stale route".into(),
            retry_after_ms: Some(22),
            latest_map_revision: Some(23),
            owner_hint: Some(OwnerHint {
                instance_id: 24,
                rpc_endpoint: "127.0.0.1:15200".into(),
                owner_epoch: 25,
            }),
        }),
    };
    let (buffer, offset) = encode_point_response(26, 27, &response).unwrap();
    assert_eq!(decode_point_response(&buffer[offset..]).unwrap(), response);
}
