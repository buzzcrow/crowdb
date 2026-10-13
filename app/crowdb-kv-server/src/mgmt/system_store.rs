// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Create the system store after its operation has been durably accepted.

use super::{err_json, system_bootstrap::ManagementError, RegistryArc};
use axum::http::StatusCode;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use std::{net::SocketAddr, sync::Arc};
use tracing::info;

pub(super) async fn ensure(state: &RegistryArc) -> Result<Arc<PxKvStore>, ManagementError> {
    if !state.contains_store(0) {
        let port = super::resolve_store_port(state, None, 0).await;
        let addr: SocketAddr = format!("0.0.0.0:{port}")
            .parse()
            .map_err(|e| err_json(StatusCode::BAD_REQUEST, format!("invalid address: {e}")))?;
        let mut store = PxKvStore::new(0, addr);
        store.rpc_workers = state.rpc_workers;
        if let Some(ref mr) = state.metrics_registry {
            store.set_metrics_registry(Arc::clone(mr));
        }
        store.set_scan_byte_budget(state.config.server.scan_byte_budget);
        store.set_peer_pool_size(state.config.server.peer_pool_size);
        store.set_enable_nagle(state.config.server.enable_nagle);
        store.set_quickack(state.config.server.quickack);
        store.set_event_write(state.config.server.event_write);
        store.set_send_queue_capacity(state.config.server.send_queue_capacity);
        store.set_snapshot_source_config(
            state.config.server.snapshot_chunk_bytes,
            state.config.server.snapshot_source_sessions,
            state.config.server.snapshot_session_lease_ms,
        );
        let store = Arc::new(store);
        store.start().await.map_err(|e| {
            err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to start store 0: {e}"),
            )
        })?;
        store.wire_rpc_transport();
        state.add_store(0, &store);
        info!(s = 0, "system store 0 created via /system/init");
    }

    let store = state.get_store(0).ok_or_else(|| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store 0 not found after creation",
        )
    })?;
    Ok(store)
}
