// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(unsafe_code)]

//! crowdb-rpc point-operation server boundary.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_protocol::chunk_kv::{ChunkKvResponse, ChunkKvRpcErrorCode, RpcFailure};
use crowdb_protocol::chunk_kv_ordered_wire::{decode_scan_request, decode_seek_request};
use crowdb_protocol::chunk_kv_wire::{decode_point_request, encode_point_response};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};
use tokio::runtime::Handle;

use crate::ChunkKvService;

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
    }

    fn handle_point(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let rpc_request_id = request.request_id;
            let rpc_create_nano = request.rpc_create_nano;
            let response = match decode_point_request(request.control()) {
                Ok(envelope) if envelope.rpc_request_id == rpc_request_id => {
                    service
                        .handle_point(envelope.request, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_response("RPC frame and control request IDs differ"),
                Err(error) => invalid_response(&error.to_string()),
            };
            submit_response(
                &server,
                request.conn_handle,
                rpc_request_id,
                rpc_create_nano,
                &response,
            );
        });
    }

    fn handle_seek(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let rpc_request_id = request.request_id;
            let rpc_create_nano = request.rpc_create_nano;
            let response = match decode_seek_request(request.control()) {
                Ok(envelope) if envelope.rpc_request_id == rpc_request_id => {
                    service
                        .handle_seek(envelope.request, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_response("RPC frame and control request IDs differ"),
                Err(error) => invalid_response(&error.to_string()),
            };
            submit_response(
                &server,
                request.conn_handle,
                rpc_request_id,
                rpc_create_nano,
                &response,
            );
        });
    }

    fn handle_scan(&self, request: ServerRequest, server: &Arc<RpcServer>) {
        let service = Arc::clone(&self.service);
        let server = Arc::clone(server);
        self.runtime.spawn(async move {
            let rpc_request_id = request.request_id;
            let rpc_create_nano = request.rpc_create_nano;
            let response = match decode_scan_request(request.control()) {
                Ok(envelope) if envelope.rpc_request_id == rpc_request_id => {
                    service
                        .handle_scan(envelope.request, wall_time_ms(), service.monotonic_ms())
                        .await
                }
                Ok(_) => invalid_response("RPC frame and control request IDs differ"),
                Err(error) => invalid_response(&error.to_string()),
            };
            submit_response(
                &server,
                request.conn_handle,
                rpc_request_id,
                rpc_create_nano,
                &response,
            );
        });
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

fn submit_response(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    rpc_request_id: u64,
    rpc_create_nano: u64,
    response: &ChunkKvResponse,
) {
    let (bytes, offset) = encode_point_response(rpc_request_id, rpc_create_nano, response);
    let buffer = Buffer::from_vec_offset(bytes, offset);
    unsafe {
        let _ = server.submit_response_buffer(
            conn_handle,
            buffer,
            None,
            FBMsgType::EChunkKvPointResponse.0 as u16,
            rpc_request_id,
        );
    }
}

fn wall_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
