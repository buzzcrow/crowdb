// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! Host-wide shutdown keeps every consensus listener alive until every
//! accepted runtime prefix has drained, including peers hosted in this process.
use crate::store_registry::PaxosKvStoreRegistry;
use crowdb_paxos_kv::common::OperationReport;
use std::time::Duration;

/// Perform one bounded shutdown attempt. An incomplete accepted drain returns
/// an error before any store is torn down, so callers may repair and retry.
pub async fn shutdown_stores_once(registry: &PaxosKvStoreRegistry) -> OperationReport {
    let stores = registry.stores_snapshot();
    let timeout = Duration::from_millis(registry.config.server.shutdown_timeout_ms);
    for store in stores.values() {
        store.close_runtime_proposals();
    }
    let mut report = OperationReport::new();
    for store in stores.values() {
        report.merge(store.prepare_runtime_shutdown(timeout).await);
    }
    if !report.is_clean() {
        return report;
    }
    for store in stores.values() {
        report.merge(store.shutdown(timeout).await);
    }
    report
}

/// Complete host shutdown. A timed-out accepted prefix is reported while the
/// process retains its communication/repair owners and awaits real progress.
/// Backend checkpoint failures remain explicit terminal errors in the result.
pub async fn shutdown_stores(registry: &PaxosKvStoreRegistry) -> OperationReport {
    loop {
        let report = shutdown_stores_once(registry).await;
        let stores = registry.stores_snapshot();
        if !stores.values().any(|store| store.runtime_shutdown_pending()) {
            return report;
        }
        for error in &report.errors {
            tracing::error!(error = %error, "shutdown incomplete; consensus remains live; next step: restore peer progress to drain accepted work");
        }
        for store in stores.values() {
            store.await_runtime_shutdown_ready().await;
        }
    }
}
