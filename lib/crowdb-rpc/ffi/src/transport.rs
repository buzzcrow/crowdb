// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cold transport configuration and owned status snapshots.

use std::ffi::CString;
use std::mem::MaybeUninit;

use crate::{sys, Buffer, Connection, RpcError, RpcServer};

/// Layout selected during TCP preflight and bound into the CM offer.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[repr(u32)]
pub enum RdmaWireLayout {
    #[default]
    Contiguous = 0,
    ControlData = 1,
}

/// Explicit configuration overrides native environment defaults before listen.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct RdmaConfig {
    pub enabled: bool,
    pub allow_tcp_fallback: bool,
    pub listen_enabled: bool,
    pub port: u32,
    pub cq_count: u32,
    pub cq_depth: u32,
    pub sq_depth: u32,
    pub rq_depth: u32,
    pub posted_receive_target: u32,
    pub wire_slot_capacity: u32,
    pub wire_layout: RdmaWireLayout,
    pub poll_batch: u32,
    pub verbs_poll_batch: u32,
    pub poll_empty_budget: u32,
    pub send_queue_capacity: u32,
    pub aggregation_enabled: bool,
    pub aggregation_max_bytes: u32,
    pub aggregation_max_frames: u32,
    pub send_arena_bytes: u64,
    pub receive_arena_bytes: u64,
    pub registered_memory_budget_bytes: u64,
    pub device: String,
    pub source_address: String,
    pub gid: String,
}

fn native_defaults() -> sys::CrowdbRpcRdmaConfig {
    let mut config = MaybeUninit::uninit();
    // SAFETY: initializer fills every field of the versioned C ABI struct.
    unsafe {
        sys::crowdb_rpc_rdma_config_init(config.as_mut_ptr());
        config.assume_init()
    }
}

impl Default for RdmaConfig {
    fn default() -> Self {
        let raw = native_defaults();
        Self {
            enabled: raw.enabled != 0,
            allow_tcp_fallback: raw.allow_tcp_fallback != 0,
            listen_enabled: raw.listen_enabled != 0,
            port: raw.port,
            cq_count: raw.cq_count,
            cq_depth: raw.cq_depth,
            sq_depth: raw.sq_depth,
            rq_depth: raw.rq_depth,
            posted_receive_target: raw.posted_receive_target,
            wire_slot_capacity: raw.wire_slot_capacity,
            wire_layout: RdmaWireLayout::Contiguous,
            poll_batch: raw.poll_batch,
            verbs_poll_batch: raw.verbs_poll_batch,
            poll_empty_budget: raw.poll_empty_budget,
            send_queue_capacity: raw.send_queue_capacity,
            aggregation_enabled: raw.aggregation_enabled != 0,
            aggregation_max_bytes: raw.aggregation_max_bytes,
            aggregation_max_frames: raw.aggregation_max_frames,
            send_arena_bytes: raw.send_arena_bytes,
            receive_arena_bytes: raw.receive_arena_bytes,
            registered_memory_budget_bytes: raw.registered_memory_budget_bytes,
            device: String::new(),
            source_address: String::new(),
            gid: String::new(),
        }
    }
}

impl RpcServer {
    /// Configure before listen/start; strings are copied by the native layer.
    ///
    /// # Errors
    /// Rejects invalid bounds, an unavailable requested HCA, or late changes.
    pub fn configure_rdma(&self, config: &RdmaConfig) -> Result<(), RpcError> {
        let device = CString::new(config.device.as_str()).map_err(|_| RpcError::InvalidArg)?;
        let source = CString::new(config.source_address.as_str()).map_err(|_| RpcError::InvalidArg)?;
        let gid = CString::new(config.gid.as_str()).map_err(|_| RpcError::InvalidArg)?;
        let mut raw = native_defaults();
        raw.enabled = u32::from(config.enabled);
        raw.allow_tcp_fallback = u32::from(config.allow_tcp_fallback);
        raw.listen_enabled = u32::from(config.listen_enabled);
        raw.port = config.port;
        raw.cq_count = config.cq_count;
        raw.cq_depth = config.cq_depth;
        raw.sq_depth = config.sq_depth;
        raw.rq_depth = config.rq_depth;
        raw.posted_receive_target = config.posted_receive_target;
        raw.wire_slot_capacity = config.wire_slot_capacity;
        raw.poll_batch = config.poll_batch;
        raw.verbs_poll_batch = config.verbs_poll_batch;
        raw.poll_empty_budget = config.poll_empty_budget;
        raw.send_queue_capacity = config.send_queue_capacity;
        raw.aggregation_enabled = u32::from(config.aggregation_enabled);
        raw.aggregation_max_bytes = config.aggregation_max_bytes;
        raw.aggregation_max_frames = config.aggregation_max_frames;
        raw.send_arena_bytes = config.send_arena_bytes;
        raw.receive_arena_bytes = config.receive_arena_bytes;
        raw.registered_memory_budget_bytes = config.registered_memory_budget_bytes;
        raw.device = device.as_ptr();
        raw.source_address = source.as_ptr();
        raw.gid = gid.as_ptr();
        // SAFETY: handle is borrowed, struct and CStrings live through the call.
        let status = unsafe {
            sys::crowdb_rpc_server_configure_rdma_v2(self.handle(), &raw, config.wire_layout as u32)
        };
        if status == sys::CROWDB_RPC_OK {
            Ok(())
        } else {
            Err(RpcError::from_status(status))
        }
    }

    /// Return an owned UTF-8 JSON snapshot with a versioned schema.
    ///
    /// # Errors
    /// Returns an error if native status allocation fails.
    pub fn transport_status_json(&self) -> Result<Vec<u8>, RpcError> {
        // SAFETY: server stays borrowed until the owned buffer is copied.
        copy_status(unsafe { sys::crowdb_rpc_server_status_json(self.handle()) })
    }

    /// Register a benchmark control RPC returning the same status schema.
    pub fn register_status_handler(&self, msg_type: u16) {
        // SAFETY: registration copies the handler into the borrowed server.
        unsafe { sys::crowdb_rpc_server_register_status_handler(self.handle(), msg_type) };
    }
}

impl Connection {
    /// Actual path and immutable QP/CQ/provider capacities, without reading a
    /// live QP pointer during teardown.
    ///
    /// # Errors
    /// Returns an error if native status allocation fails.
    pub fn transport_status_json(&self) -> Result<Vec<u8>, RpcError> {
        // SAFETY: connection wrapper owns/borrows the native handle.
        copy_status(unsafe { sys::crowdb_rpc_conn_status_json(self.handle()) })
    }
}

fn copy_status(handle: sys::crowdb_rpc_buffer_t) -> Result<Vec<u8>, RpcError> {
    if handle.is_null() {
        return Err(RpcError::RegistrationFailed);
    }
    let buffer = Buffer::from_raw(handle);
    Ok(buffer.as_slice().to_vec())
}
