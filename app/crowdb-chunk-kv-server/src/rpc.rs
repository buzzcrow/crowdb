// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(unsafe_code)]

//! crowdb-rpc point-operation server boundary.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_protocol::chunk_kv::{
    BatchMutationResponse, ChunkKvResponse, ChunkKvRpcErrorCode, MultiGetResponse, RpcFailure,
};
use crowdb_protocol::chunk_kv_group_wire::{
    decode_batch_mutation_request, decode_multi_get_request, encode_batch_mutation_response,
    encode_multi_get_response,
};
use crowdb_protocol::chunk_kv_ordered_wire::{
    decode_bounded_scan_request, decode_scan_request, decode_seek_request,
};
use crowdb_protocol::chunk_kv_wire::decode_point_request;
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};
use tokio::runtime::Handle;

use crate::ChunkKvService;

mod response;
mod transport;

use response::{submit_group_response, submit_response};

pub struct ChunkKvRpcService {
    service: Arc<ChunkKvService>,
    runtime: Handle,
}

impl ChunkKvRpcService {
    #[must_use]
    pub fn new(service: Arc<ChunkKvService>, runtime: Handle) -> Self {
        Self { service, runtime }
    }

    pub fn register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        let rpc_service = Arc::clone(self);
        let response_server = Arc::clone(server);
        server.register_handler(FBMsgType::EChunkKvPointRequest.0 as u16, move |request| {
            rpc_service.handle_point(request, &response_server);
        });
        let rpc_service = Arc::clone(self);
        let response_server = Arc::clone(server);
        server.register_handler(FBMsgType::EChunkKvSeekRequest.0 as u16, move |request| {
            rpc_service.handle_seek(request, &response_server);
        });
        let rpc_service = Arc::clone(self);
        let response_server = Arc::clone(server);
        server.register_handler(FBMsgType::EChunkKvScanRequest.0 as u16, move |request| {
            rpc_service.handle_scan(request, &response_server);
        });
        let rpc_service = Arc::clone(self);
        let response_server = Arc::clone(server);
        server.register_handler(FBMsgType::EChunkKvBoundedScanRequest.0 as u16, move |request| {
            rpc_service.handle_bounded_scan(request, &response_server);
        });
        let rpc_service = Arc::clone(self);
        let response_server = Arc::clone(server);
        server.register_handler(FBMsgType::EChunkKvMultiGetRequest.0 as u16, move |request| {
            rpc_service.handle_multi_get(request, &response_server);
        });
        let rpc_service = Arc::clone(self);
        let response_server = Arc::clone(server);
        server.register_handler(FBMsgType::EChunkKvBatchMutationRequest.0 as u16, move |request| {
            rpc_service.handle_batch_mutation(request, &response_server);
        });
    }

    fn handle_point(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let rpc_request_id = request.request_id;
            let response = match decode_point_request(request.control()) {
                Ok(envelope) if envelope.rpc_request_id == rpc_request_id => {
                    service
                        .handle_point(envelope.request, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_response("RPC frame and control request IDs differ"),
                Err(error) => invalid_response(&error.to_string()),
            };
            submit_response(&server, &request, &response);
        });
    }

    fn handle_seek(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let rpc_request_id = request.request_id;
            let response = match decode_seek_request(request.control()) {
                Ok(envelope) if envelope.rpc_request_id == rpc_request_id => {
                    service
                        .handle_seek(envelope.request, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_response("RPC frame and control request IDs differ"),
                Err(error) => invalid_response(&error.to_string()),
            };
            submit_response(&server, &request, &response);
        });
    }

    fn handle_scan(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let rpc_request_id = request.request_id;
            let response = match decode_scan_request(request.control()) {
                Ok(envelope) if envelope.rpc_request_id == rpc_request_id => {
                    service
                        .handle_scan(envelope.request, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_response("RPC frame and control request IDs differ"),
                Err(error) => invalid_response(&error.to_string()),
            };
            submit_response(&server, &request, &response);
        });
    }

    fn handle_bounded_scan(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let id = request.request_id;
            let response = match decode_bounded_scan_request(request.control()) {
                Ok((envelope, budget)) if envelope.rpc_request_id == id => {
                    service
                        .handle_bounded_scan(envelope.request, budget, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_response("RPC frame and control request IDs differ"),
                Err(error) => invalid_response(&error.to_string()),
            };
            submit_response(&server, &request, &response);
        });
    }

    fn handle_multi_get(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let id = request.request_id;
            let create_nano = request.rpc_create_nano;
            let response = match decode_multi_get_request(request.control()) {
                Ok((envelope_id, _, request)) if envelope_id == id => {
                    service
                        .handle_multi_get(request, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_multi_get("RPC frame and control request IDs differ"),
                Err(error) => invalid_multi_get(&error.to_string()),
            };
            match encode_multi_get_response(id, create_nano, &response) {
                Ok(bytes) => submit_group_response(
                    &server,
                    &request,
                    Buffer::from_vec(bytes),
                    FBMsgType::EChunkKvMultiGetResponse.0 as u16,
                ),
                Err(error) => {
                    tracing::error!(request_id = id, %error, "Chunk-KV multi-get response encoding failed; closing connection");
                    request.close_connection();
                }
            }
        });
    }

    fn handle_batch_mutation(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let id = request.request_id;
            let create_nano = request.rpc_create_nano;
            let response = match decode_batch_mutation_request(request.control()) {
                Ok((envelope_id, _, request)) if envelope_id == id => {
                    service
                        .handle_batch_mutation(request, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_batch("RPC frame and control request IDs differ"),
                Err(error) => invalid_batch(&error.to_string()),
            };
            match encode_batch_mutation_response(id, create_nano, &response) {
                Ok(bytes) => submit_group_response(
                    &server,
                    &request,
                    Buffer::from_vec(bytes),
                    FBMsgType::EChunkKvBatchMutationResponse.0 as u16,
                ),
                Err(error) => {
                    tracing::error!(request_id = id, %error, "Chunk-KV batch response encoding failed; closing connection");
                    request.close_connection();
                }
            }
        });
    }
}

fn invalid_multi_get(message: &str) -> MultiGetResponse {
    MultiGetResponse {
        map_revision: 0,
        result: Err(invalid_failure(message)),
    }
}

fn invalid_batch(message: &str) -> BatchMutationResponse {
    BatchMutationResponse {
        map_revision: 0,
        result: Err(invalid_failure(message)),
    }
}

fn invalid_failure(message: &str) -> RpcFailure {
    RpcFailure {
        code: ChunkKvRpcErrorCode::InvalidRequest,
        message: message.into(),
        retry_after_ms: None,
        latest_map_revision: None,
        owner_hint: None,
    }
}

fn invalid_response(message: &str) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision: 0,
        journal_position: None,
        result: Err(RpcFailure {
            code: ChunkKvRpcErrorCode::InvalidRequest,
            message: message.into(),
            retry_after_ms: None,
            latest_map_revision: None,
            owner_hint: None,
        }),
    }
}

fn wall_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
