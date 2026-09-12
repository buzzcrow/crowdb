// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_kv_server::{ChunkKvRpcService, ChunkKvService};
use crowdb_protocol::chunk_kv::{
    ChunkKvRpcErrorCode, ClientRequestId, Id128, PointOperation, PointRequest, RequestRouting,
};
use crowdb_protocol::chunk_kv_wire::{decode_point_response, encode_point_request};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, RpcClient, RpcServer};

#[tokio::test]
async fn point_request_crosses_real_rpc_boundary_with_typed_response() {
    crowdb_rpc_ffi::init_test_logging();
    let service = Arc::new(ChunkKvService::new(7, 8).unwrap());
    let rpc_service = Arc::new(ChunkKvRpcService::new(service, tokio::runtime::Handle::current()));
    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).unwrap();
    rpc_service.register_handlers(&server);
    server.start();

    let connection = server
        .connect("127.0.0.1", server.port())
        .expect("connect to point RPC server");
    let client = RpcClient::new();
    client.attach(&connection);
    let request = PointRequest {
        routing: RequestRouting {
            request_id: ClientRequestId {
                client_instance_id: Id128 { high: 1, low: 2 },
                client_sequence: 3,
            },
            map_revision: 4,
            partition_id: Id128 { high: 5, low: 6 },
            owner_epoch: 7,
            min_journal_position: None,
            deadline_ms: None,
        },
        operation: PointOperation::Get {
            key: b"object".to_vec(),
        },
    };
    let (bytes, offset) = encode_point_request(8, 9, &request).unwrap();
    let response = client
        .call(
            &server,
            &connection,
            8,
            Buffer::from_vec_offset(bytes, offset),
            None,
            FBMsgType::EChunkKvPointRequest.0 as u16,
        )
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(10), response)
        .await
        .expect("point RPC timed out")
        .expect("point RPC failed");
    assert_eq!(response.request_id, 8);
    let response =
        decode_point_response(response.control.as_ref().expect("point response control").bytes()).unwrap();
    assert_eq!(response.result.unwrap_err().code, ChunkKvRpcErrorCode::NotMyRange);

    server.stop();
}
