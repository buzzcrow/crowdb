// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::ChunkKvResponse;
use crowdb_protocol::chunk_kv_wire::encode_point_response;
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};

pub(super) fn submit_response(server: &RpcServer, request: &ServerRequest, response: &ChunkKvResponse) {
    let (bytes, offset) = encode_point_response(request.request_id, request.rpc_create_nano, response);
    submit_group_response(
        server,
        request,
        Buffer::from_vec_offset(bytes, offset),
        FBMsgType::EChunkKvPointResponse.0 as u16,
    );
}

pub(super) fn submit_group_response(
    server: &RpcServer,
    request: &ServerRequest,
    buffer: Buffer,
    message_type: u16,
) {
    // SAFETY: request owns the native Frame and its retained connection;
    // the borrowed server and response owner live through this submission.
    let result = unsafe {
        server.submit_response_buffer(
            request.conn_handle,
            buffer,
            None,
            message_type,
            request.request_id,
        )
    };
    if let Err(error) = result {
        tracing::error!(request_id = request.request_id, %error, "Chunk-KV response submission failed; closing connection");
        request.close_connection();
    }
}
