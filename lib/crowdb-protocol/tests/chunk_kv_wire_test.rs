// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    ChunkKvResponse, ChunkKvRpcErrorCode, ClientRequestId, Id128, OperationResult, OwnerHint, PointOperation,
    PointRequest, RequestRouting, RpcCompareCondition, RpcFailure, RpcJournalPosition, RpcValue,
    ScanContinuation, ScanDirection, ScanRequest, SeekKind, SeekRequest,
};
use crowdb_protocol::chunk_kv_ordered_wire::{
    decode_scan_request, decode_seek_request, encode_scan_request, encode_seek_request,
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
    let (buffer, offset) = encode_point_response(19, 20, &response);
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
    let (buffer, offset) = encode_point_response(26, 27, &response);
    assert_eq!(decode_point_response(&buffer[offset..]).unwrap(), response);
}

#[test]
fn target_not_ready_round_trip_preserves_retry_delay() {
    let response = ChunkKvResponse {
        map_revision: 28,
        journal_position: None,
        result: Err(RpcFailure {
            code: ChunkKvRpcErrorCode::TargetNotReady,
            message: "catching up".into(),
            retry_after_ms: Some(10),
            latest_map_revision: Some(28),
            owner_hint: None,
        }),
    };
    let (buffer, offset) = encode_point_response(29, 30, &response);
    assert_eq!(decode_point_response(&buffer[offset..]).unwrap(), response);
}

#[test]
fn ordered_request_round_trips_preserve_bounds_and_continuation() {
    let seek = SeekRequest {
        routing: routing(),
        key: Vec::new(),
        kind: SeekKind::Floor,
    };
    let (buffer, offset) = encode_seek_request(28, 29, &seek).unwrap();
    let decoded = decode_seek_request(&buffer[offset..]).unwrap();
    assert_eq!(decoded.rpc_request_id, 28);
    assert_eq!(decoded.rpc_create_nano, 29);
    assert_eq!(decoded.request, seek);

    let scan = ScanRequest {
        routing: routing(),
        start: Some(Vec::new()),
        end: Some(vec![0xff]),
        direction: ScanDirection::Reverse,
        limit: 30,
        continuation: Some(ScanContinuation {
            direction: ScanDirection::Reverse,
            last_key: Vec::new(),
            partition_id: routing().partition_id,
            owner_epoch: routing().owner_epoch,
            map_revision: routing().map_revision,
        }),
    };
    let (buffer, offset) = encode_scan_request(31, 32, &scan).unwrap();
    let decoded = decode_scan_request(&buffer[offset..]).unwrap();
    assert_eq!(decoded.rpc_request_id, 31);
    assert_eq!(decoded.rpc_create_nano, 32);
    assert_eq!(decoded.request, scan);
}

#[test]
fn scan_response_round_trip_preserves_items_and_cursor() {
    let response = ChunkKvResponse {
        map_revision: 33,
        journal_position: None,
        result: Ok(OperationResult::Scan {
            items: vec![RpcValue {
                key: Vec::new(),
                value: vec![0],
                revision: 34,
            }],
            continuation: Some(ScanContinuation {
                direction: ScanDirection::Forward,
                last_key: Vec::new(),
                partition_id: Id128 { high: 35, low: 36 },
                owner_epoch: 37,
                map_revision: 33,
            }),
        }),
    };
    let (buffer, offset) = encode_point_response(38, 39, &response);
    assert_eq!(decode_point_response(&buffer[offset..]).unwrap(), response);
}
