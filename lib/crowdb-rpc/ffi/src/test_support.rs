// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Controlled replies for callers testing response validation.

use crate::{sys, RpcServer, ServerRequest};

impl RpcServer {
    /// Pause encoded-control SENDs after the absolute fragment counter limit.
    /// Used to fail a real service response at a deterministic partial boundary.
    #[must_use]
    pub fn limit_control_fragments_for_tests(&self, limit: u64) -> bool {
        // SAFETY: self retains the native server; the hook changes one atomic.
        unsafe { sys::crowdb_rpc_server_limit_control_fragments_for_tests(self.handle(), limit) != 0 }
    }

    /// Register a synchronous reply fixture, keeping raw handles in the FFI.
    pub fn register_reply_for_tests<F>(&self, msg_type: u16, reply: F)
    where
        F: Fn(&ServerRequest) -> (Vec<u8>, Vec<u8>) + Send + Sync + 'static,
    {
        let server = self.handle() as usize;
        self.register_handler(msg_type, move |request| {
            let (control, data) = reply(&request);
            let (Ok(control_len), Ok(data_len)) = (u32::try_from(control.len()), u32::try_from(data.len()))
            else {
                return;
            };
            // SAFETY: synchronous dispatch keeps the server and connection
            // alive; byte owners outlive the C ABI call, which copies them.
            let status = unsafe {
                sys::crowdb_rpc_server_submit_response(
                    server as sys::crowdb_rpc_server_t,
                    request.conn_handle,
                    control.as_ptr(),
                    control_len,
                    data.as_ptr(),
                    data_len,
                    msg_type,
                    request.request_id,
                )
            };
            if status != sys::CROWDB_RPC_OK {
                eprintln!(
                    "test reply submit failed: request={} status={status}",
                    request.request_id
                );
            }
        });
    }
}
