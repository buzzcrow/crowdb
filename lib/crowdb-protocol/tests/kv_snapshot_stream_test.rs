// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::fb::FBMsgType;
use crowdb_protocol::fb_wrappers::kv_consensus::{FBSnapshotBeginResponseRef, FBSnapshotReadResponseRef};
use crowdb_protocol::kv_consensus_fb::{
    FBKvRetCode, FBSnapshotBeginResponse, FBSnapshotBeginResponseArgs, FBSnapshotReadResponse,
    FBSnapshotReadResponseArgs,
};
use flatbuffers::FlatBufferBuilder;

#[test]
fn snapshot_stream_message_range_and_typed_errors_are_stable() {
    assert_eq!(FBMsgType::ESnapshotBeginRequest.0, 1018);
    assert_eq!(FBMsgType::ESnapshotAbortResponse.0, 1025);
    assert_eq!(FBKvRetCode::SnapshotNotFound.0, 5);
    assert_eq!(FBKvRetCode::SnapshotExpired.0, 6);
    assert_eq!(FBKvRetCode::SnapshotInvalidOffset.0, 7);
    assert_eq!(FBKvRetCode::SnapshotBackpressure.0, 8);
    assert_eq!(FBKvRetCode::SnapshotTopologyChanged.0, 9);
    assert_eq!(FBKvRetCode::SnapshotIntegrity.0, 10);
}

#[test]
fn snapshot_begin_response_wrapper_exposes_identity_and_metadata() {
    let mut builder = FlatBufferBuilder::new();
    let response = FBSnapshotBeginResponse::create(
        &mut builder,
        &FBSnapshotBeginResponseArgs {
            id: 7,
            rpc_create_nano: 8,
            ret_code: FBKvRetCode::Success,
            error_msg: None,
            group_id: 9,
            boot_nonce: 10,
            session_number: 11,
            engine_format: 1,
            at_slot: 12,
            term_at_slot: 13,
            membership_epoch: 14,
            chunk_bytes: 1 << 20,
            total_bytes: 70 << 20,
            final_crc32c: 15,
        },
    );
    builder.finish(response, None);

    let view = FBSnapshotBeginResponseRef::new(builder.finished_data());
    assert!(view.valid());
    assert_eq!(view.request_id(), Some(7));
    assert_eq!(view.group_id(), 9);
    assert_eq!(view.boot_nonce(), 10);
    assert_eq!(view.session_number(), 11);
    assert_eq!(view.engine_format(), 1);
    assert_eq!(view.at_slot(), 12);
    assert_eq!(view.term_at_slot(), 13);
    assert_eq!(view.membership_epoch(), 14);
    assert_eq!(view.chunk_bytes(), 1 << 20);
    assert_eq!(view.total_bytes(), 70 << 20);
    assert_eq!(view.final_crc32c(), 15);
}

#[test]
fn snapshot_read_response_wrapper_exposes_retry_fields() {
    let mut builder = FlatBufferBuilder::new();
    let response = FBSnapshotReadResponse::create(
        &mut builder,
        &FBSnapshotReadResponseArgs {
            id: 1,
            rpc_create_nano: 2,
            ret_code: FBKvRetCode::Success,
            error_msg: None,
            boot_nonce: 3,
            session_number: 4,
            offset: 5,
            payload_crc32c: 6,
            done: true,
        },
    );
    builder.finish(response, None);

    let view = FBSnapshotReadResponseRef::new(builder.finished_data());
    assert!(view.valid());
    assert_eq!(view.request_id(), Some(1));
    assert_eq!(view.boot_nonce(), 3);
    assert_eq!(view.session_number(), 4);
    assert_eq!(view.offset(), 5);
    assert_eq!(view.payload_crc32c(), 6);
    assert!(view.done());
}
