// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Get requests retain their wire semantics on an optional isolated connection.

use super::{
    parse_kv_response, read_mode_to_fb, Buffer, Error, FBKvGetRequest, FBKvGetRequestArgs, FBMsgType,
    FlatBufferBuilder, KvResponse, PaxosKvRpcTransport, ReadMode, Result,
};

impl PaxosKvRpcTransport {
    /// Send a `Get` request via crowdb-rpc.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_get(
        &self,
        rpc_endpoint: &str,
        key: &[u8],
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
        read_mode: ReadMode,
        min_slot: u64,
    ) -> Result<KvResponse> {
        self.send_get_with_forwarded(
            rpc_endpoint,
            key,
            request_id,
            request_create_ms,
            group_id,
            read_mode,
            min_slot,
            false,
        )
        .await
    }

    /// Send a `Get` request with an explicit forwarded loop-guard flag.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_get_with_forwarded(
        &self,
        rpc_endpoint: &str,
        key: &[u8],
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
        read_mode: ReadMode,
        min_slot: u64,
        forwarded: bool,
    ) -> Result<KvResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for_point(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let fb_key = builder.create_vector(key);
        let args = FBKvGetRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            key: Some(fb_key),
            request_id,
            request_create_ms,
            group_id,
            read_mode: read_mode_to_fb(read_mode),
            min_slot,
            forwarded,
        };
        let req = FBKvGetRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvGetRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_get_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_get_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: rpc_endpoint.to_string(),
            status: "get response missing control buffer".into(),
        })?;
        parse_kv_response(ctrl.bytes())
    }
}
