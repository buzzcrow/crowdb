// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Scoped registration keeps compatibility with the original exported ABI.

use super::{
    crowdb_kv_on_complete, crowdb_svc_client_t, parse_json_or_default, spawn_op, DiskGroupUsageSummary,
    ServiceRegistryClient,
};
use std::{
    ffi::CStr,
    os::raw::{c_char, c_void},
    ptr,
};

/// Register immutable physical scope before opening media.
#[no_mangle]
pub unsafe extern "C" fn crowdb_svc_heartbeat_diskio_scoped(
    client: crowdb_svc_client_t,
    instance_id: u64,
    rpc_endpoint: *const c_char,
    rack_id: u64,
    node_id: u64,
    configured_group: u64,
    owned_dg_ids_json: *const c_char,
    group_usages_json: *const c_char,
    callback: crowdb_kv_on_complete,
    user_data: *mut c_void,
) {
    if client.is_null() || rpc_endpoint.is_null() {
        callback(-1, ptr::null(), user_data);
        return;
    }
    let svc = (*(client as *const ServiceRegistryClient)).clone();
    let endpoint = if let Ok(value) = CStr::from_ptr(rpc_endpoint).to_str() {
        value.to_string()
    } else {
        callback(-1, ptr::null(), user_data);
        return;
    };
    let dg_ids: Vec<u64> = parse_json_or_default(owned_dg_ids_json);
    let usages: Vec<DiskGroupUsageSummary> = parse_json_or_default(group_usages_json);
    spawn_op(callback, user_data, move || async move {
        svc.kv()
            .refresh_topology()
            .await
            .map_err(|error| error.to_string())?;
        svc.heartbeat_diskio_scoped(
            instance_id,
            &endpoint,
            &crowdb_protocol::common::DiskioHardwareScope {
                version: 1,
                rack_id,
                node_id,
                disk_group_id: configured_group,
            },
            &dg_ids,
            &usages,
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok("{}".to_string())
    });
}
