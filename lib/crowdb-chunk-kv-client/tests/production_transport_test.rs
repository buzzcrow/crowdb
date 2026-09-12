// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv_client::{ChunkKvRpcTransport, ChunkKvTransport};
use crowdb_chunk_kv_server::{ChunkKvRpcService, ChunkKvService};
use crowdb_protocol::chunk_kv::{
    ChunkKvRpcErrorCode, ClientRequestId, Id128, PointOperation, PointRequest, RequestRouting,
};
use crowdb_rpc_ffi::RpcServer;

#[tokio::test]
async fn production_transport_calls_direct_owner_rpc() {
    crowdb_rpc_ffi::init_test_logging();
    let service = Arc::new(ChunkKvService::new(7, 8).unwrap());
    let rpc_service = Arc::new(ChunkKvRpcService::new(service, tokio::runtime::Handle::current()));
    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).unwrap();
    rpc_service.register_handlers(&server);
    server.start();

    let transport = ChunkKvRpcTransport::new(4, 1, 1);
    let response = transport
        .point(
            &format!("127.0.0.1:{}", server.port()),
            &PointRequest {
                routing: RequestRouting {
                    request_id: ClientRequestId {
                        client_instance_id: Id128 { high: 1, low: 2 },
                        client_sequence: 1,
                    },
                    map_revision: 1,
                    partition_id: Id128 { high: 3, low: 4 },
                    owner_epoch: 1,
                    min_journal_position: None,
                    deadline_ms: None,
                },
                operation: PointOperation::Get { key: b"key".to_vec() },
            },
        )
        .await
        .unwrap();
    assert_eq!(response.result.unwrap_err().code, ChunkKvRpcErrorCode::NotMyRange);
    server.stop();
}
