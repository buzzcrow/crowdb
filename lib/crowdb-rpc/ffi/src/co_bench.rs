// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Safe wrapper over the C++ coroutine client (`crowdb_rpc_co_spawn`).
//!
//! [`co_spawn`] runs `num_coroutines` C++ coroutines across the given
//! connections, each looping `build_request → submit → co_await →
//! on_response` until the [`CoBenchHandler`] returns `false`. This is
//! the high-throughput path used by `bench rpc --mode coroutine`: one
//! heap-allocated coroutine frame per loader, zero per-op tokio
//! scheduling. The handler methods fire on C++ coroutine threads and
//! must be non-blocking.
//!
//! `co_spawn` blocks until all coroutines finish; callers should run it
//! on a blocking thread (e.g. `tokio::task::spawn_blocking`).
//! Configure the client's request timeout reaper before spawning so stalled
//! requests complete through the ordinary callback path.

use std::ffi::c_void;
use std::sync::Arc;

use crate::{sys, Buffer, Connection, RpcClient, RpcError, RpcServer};

/// Per-coroutine callback pair. Both methods are invoked on C++ coroutine
/// threads and must be non-blocking. The handler is shared across all
/// coroutines (shared state must be thread-safe — atomics / lock-free).
pub trait CoBenchHandler: Send + Sync {
    /// Build one request's control + data buffers. Return `None` to stop
    /// this coroutine (e.g. deadline reached). The buffers' ownership
    /// transfers to the C++ layer.
    fn build_request(&self, request_id: u64) -> Option<(Buffer, Buffer)>;
    /// One response arrived. `status` is `Ok` on success. Return `false`
    /// to stop this coroutine. The C++ layer releases the response
    /// buffers after this returns — do not release them here.
    fn on_response(&self, request_id: u64, status: Result<(), RpcError>, latency_ns: u64) -> bool;

    /// Borrow response bytes for validation without cloning buffer owners.
    /// The slices are valid only during this call.
    fn on_response_data(
        &self,
        request_id: u64,
        status: Result<(), RpcError>,
        latency_ns: u64,
        _control: &[u8],
        _data: &[u8],
    ) -> bool {
        self.on_response(request_id, status, latency_ns)
    }
}

/// Native coroutine totals include submission rejections and response errors.
pub type CoStats = sys::CrowdbRpcCoStats;

/// Shared state passed through the C ABI as `ctx`.
struct CoCtx {
    handler: Arc<dyn CoBenchHandler>,
}

/// Spawn `num_coroutines` C++ coroutines across `conns`, each looping
/// build → submit → await → on_response until the handler returns
/// `false`. Blocks until all coroutines finish.
///
/// `conns` is partitioned round-robin across coroutines
/// (`conn[i % num_conns]`). The completion pool is sized to the next
/// power of two >= `num_coroutines` by the caller via
/// `RpcClient::set_completion_pool_size`.
#[allow(clippy::missing_panics_doc)]
pub fn co_spawn(
    client: &RpcClient,
    server: &RpcServer,
    conns: &[Connection],
    num_coroutines: u32,
    msg_type: u16,
    handler: Arc<dyn CoBenchHandler>,
) -> CoStats {
    if conns.is_empty() || num_coroutines == 0 {
        return CoStats::default();
    }
    let ctx = Box::new(CoCtx { handler });
    let ctx_ptr = Box::into_raw(ctx) as *mut c_void;
    let conn_handles: Vec<sys::crowdb_rpc_conn_t> = conns.iter().map(Connection::handle).collect();

    // SAFETY: client/server handles are valid for the duration of
    // co_spawn (the caller's Arcs outlive this call). conn_handles are
    // valid Connection* owned by the server. ctx_ptr is a valid CoCtx*
    // reclaimed below after co_spawn returns (no callback fires after).
    unsafe {
        sys::crowdb_rpc_co_spawn(
            client.handle(),
            server.handle(),
            conn_handles.as_ptr(),
            conn_handles.len(),
            num_coroutines,
            msg_type,
            Some(build_cb),
            Some(resp_cb),
            ctx_ptr,
        );
    }
    // SAFETY: co_spawn has returned; no callback will fire again.
    unsafe { drop(Box::from_raw(ctx_ptr.cast::<CoCtx>())) };
    let mut stats = CoStats::default();
    // SAFETY: all coroutines and callbacks joined; client remains borrowed.
    unsafe { sys::crowdb_rpc_co_get_stats(client.handle(), &mut stats) };
    stats
}

// SAFETY: ctx is a valid CoCtx* set up by co_spawn. out_control /
// out_data are valid out-pointers.
unsafe extern "C" fn build_cb(
    ctx: *mut c_void,
    request_id: u64,
    out_control: *mut sys::crowdb_rpc_buffer_t,
    out_data: *mut sys::crowdb_rpc_buffer_t,
) -> bool {
    let c = &*(ctx.cast::<CoCtx>());
    match c.handler.build_request(request_id) {
        Some((ctrl, data)) => {
            *out_control = ctrl.into_raw();
            *out_data = data.into_raw();
            true
        }
        None => false,
    }
}

// SAFETY: ctx is a valid CoCtx*. The response buffers are released by
// the C++ layer after this callback returns.
unsafe extern "C" fn resp_cb(
    ctx: *mut c_void,
    request_id: u64,
    control: sys::crowdb_rpc_buffer_t,
    data: sys::crowdb_rpc_buffer_t,
    status: sys::crowdb_rpc_status,
    latency_ns: u64,
) -> bool {
    let c = &*(ctx.cast::<CoCtx>());
    let r = if status == sys::CROWDB_RPC_OK {
        Ok(())
    } else {
        Err(RpcError::from_status(status))
    };
    c.handler.on_response_data(
        request_id,
        r,
        latency_ns,
        response_bytes(control),
        response_bytes(data),
    )
}

// SAFETY: response buffer owners remain in the C++ coroutine until resp_cb
// returns. Empty/null buffers produce an empty slice; no owner is transferred.
unsafe fn response_bytes<'a>(handle: sys::crowdb_rpc_buffer_t) -> &'a [u8] {
    if handle.is_null() {
        return &[];
    }
    let length = sys::crowdb_rpc_buffer_len(handle) as usize;
    let data = sys::crowdb_rpc_buffer_data(handle);
    if data.is_null() || length == 0 {
        return &[];
    }
    std::slice::from_raw_parts(data, length)
}
