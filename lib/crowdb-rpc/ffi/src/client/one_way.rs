// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{sys, Buffer, Connection, RpcClient, RpcError, RpcServer};

impl RpcClient {
    /// Submit a notification without creating a response completion slot.
    pub fn send_one_way(
        &self,
        server: &RpcServer,
        conn: &Connection,
        request_id: u64,
        control: Buffer,
        data: Option<Buffer>,
        msg_type: u16,
    ) -> Result<(), RpcError> {
        let status = unsafe {
            sys::crowdb_rpc_client_send_one_way(
                self.handle,
                server.handle(),
                conn.handle(),
                request_id,
                control.into_raw(),
                data.map_or(std::ptr::null_mut(), Buffer::into_raw),
                msg_type,
            )
        };
        if status == sys::CROWDB_RPC_OK {
            Ok(())
        } else {
            Err(RpcError::from_status(status))
        }
    }

    /// Submit a notification using a connection retained from a server request.
    ///
    /// The handle must remain valid through submission and belong to `server`.
    #[allow(
        clippy::not_unsafe_ptr_arg_deref,
        reason = "FFI wrapper mirrors the retained handler connection ABI"
    )]
    pub fn send_one_way_to_handle(
        &self,
        server: &RpcServer,
        conn: *mut std::ffi::c_void,
        request_id: u64,
        control: Buffer,
        data: Option<Buffer>,
        msg_type: u16,
    ) -> Result<(), RpcError> {
        let status = unsafe {
            sys::crowdb_rpc_client_send_one_way_conn(
                self.handle,
                server.handle(),
                conn,
                request_id,
                control.into_raw(),
                data.map_or(std::ptr::null_mut(), Buffer::into_raw),
                msg_type,
            )
        };
        if status == sys::CROWDB_RPC_OK {
            Ok(())
        } else {
            Err(RpcError::from_status(status))
        }
    }
}
