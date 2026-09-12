// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! chunkdb range binding monitor wiring.
//!
//! Spawns the generic `BindingMonitor` (from `crowdb-kv-client`) with the
//! chunkdb range strategy as a leader-gated background task on this
//! `crowdb-kv-server`'s group-0 replica. Only the group-0 leader writes
//! the binding table; followers run the tick (read instances + compute
//! assignment) but skip the write phase, so they are ready to take over
//! immediately on leader change.
//!
//! See `doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md` §5
//! (Dynamic Binding Monitor).

use std::sync::Arc;

use crowdb_kv_client::{
    BindingMonitor, ChunkdbRangeStrategy, ClientConfig, CrowdbKvClient, ServiceRegistryClient,
};
use tracing::{info, info_span, warn, Instrument};

use crate::store_registry::KvStoreRegistry;

/// Handle to the spawned binding monitor task. Drop to stop (sends the
/// stop signal); await is unnecessary — the task exits on the next tick
/// after the signal is sent.
pub struct BindingMonitorHandle {
    stop_tx: tokio::sync::watch::Sender<bool>,
    supervisor: Option<tokio::task::JoinHandle<()>>,
}

/// Runs one monitor child at a time and reconstructs it after an unexpected
/// return or panic. The receiver is cloned into each child, so shutdown drains
/// the active child before the supervisor exits.
pub fn spawn_restarting_monitor<F, Fut>(
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
    make_monitor: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(tokio::sync::watch::Receiver<bool>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            if *stop_rx.borrow() {
                break;
            }
            let mut child = tokio::spawn(make_monitor(stop_rx.clone()));
            tokio::select! {
                result = &mut child => {
                    if !*stop_rx.borrow() {
                        warn!(?result, "domain monitor task exited; restarting");
                        tokio::task::yield_now().await;
                    }
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        let _ = child.await;
                        break;
                    }
                }
            }
        }
    })
}

impl BindingMonitorHandle {
    /// Stop the monitor task. Returns immediately; the task exits on
    /// its next tick.
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    /// Stop the monitor and wait until its current task and supervisor exit.
    pub async fn stop_and_wait(mut self) {
        self.stop();
        if let Some(supervisor) = self.supervisor.take() {
            let _ = supervisor.await;
        }
    }
}

impl Drop for BindingMonitorHandle {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
    }
}

/// Spawn the chunkdb range binding monitor on this group-0 replica.
///
/// `group0_endpoint` is the crowdb-rpc endpoint of this server's group-0
/// listener (used by the monitor's `CrowdbKvClient` to read/write the
/// binding table + scan the service registry). `mgmt_endpoint` is the
/// HTTP management URL used as a `/topology` discovery seed when the
/// local node is not the group-0 leader. `interval_secs` is the tick
/// interval; `0` is rejected by the caller (do not call this function
/// when the interval is 0).
///
/// The monitor checks leader status on each tick via the local
/// registry's group-0 replica. If this node is the group-0 leader, it
/// writes the computed binding table; otherwise it computes but skips
/// the write.
#[must_use]
pub fn spawn_chunkdb_binding_monitor(
    registry: &Arc<KvStoreRegistry>,
    group0_endpoint: String,
    mgmt_endpoint: String,
    interval_secs: u64,
) -> BindingMonitorHandle {
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![mgmt_endpoint])));
    kv.seed_leader(0, 0, group0_endpoint);

    info!(interval_secs, "chunkdb binding monitor spawning");
    let registry = Arc::clone(registry);
    let supervisor = spawn_restarting_monitor(stop_rx, move |child_stop| {
        let kv = Arc::clone(&kv);
        let registry = Arc::clone(&registry);
        async move {
            let monitor = BindingMonitor::new(
                Arc::clone(&kv),
                ServiceRegistryClient::from_shared(Arc::clone(&kv)),
                ChunkdbRangeStrategy::new(),
                std::time::Duration::from_secs(interval_secs),
                "chunkdb",
            );
            monitor
                .run(child_stop, move || {
                    registry
                        .get_store(0)
                        .and_then(|store| store.get_group(0))
                        .is_some_and(|group| group.local_replica().is_leader())
                })
                .instrument(info_span!("binding_monitor", s = 0, g = 0))
                .await;
        }
    });

    BindingMonitorHandle {
        stop_tx,
        supervisor: Some(supervisor),
    }
}
