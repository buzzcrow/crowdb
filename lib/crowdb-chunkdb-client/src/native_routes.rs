// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Nonblocking native route snapshots. Discovery runs only on the async worker.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use crowdb_protocol::chunk_slot::{ChunkSlot, CHUNK_SLOT_COUNT};
use crowdb_protocol::common::ChunkId;
use crowdb_rpc_ffi::OwnedClientRoute;
use tokio::sync::Notify;

use super::ChunkdbClient;
use crate::{ChunkdbRpcTransport, Result};

struct Snapshot {
    slots: Box<[usize; CHUNK_SLOT_COUNT as usize]>,
    routes: Vec<Option<OwnedClientRoute>>,
}

struct State {
    snapshot: ArcSwap<Snapshot>,
    refresh: Notify,
}

/// A synchronous resolver whose returned route retains its connection owners
/// across endpoint refresh. No discovery, connection setup or runtime blocking
/// occurs in `resolve`; failed calls request an asynchronous refresh.
pub struct NativeChunkRoutes {
    state: Arc<State>,
    worker: tokio::task::JoinHandle<()>,
}

impl NativeChunkRoutes {
    /// Resolve an existing chunk, or choose a live nonempty owner for allocation.
    #[must_use]
    pub fn resolve(&self, chunk: Option<ChunkId>, refresh: bool) -> Option<OwnedClientRoute> {
        if refresh {
            self.state.refresh.notify_one();
        }
        let snapshot = self.state.snapshot.load();
        match chunk {
            Some(id) => {
                snapshot.routes[snapshot.slots[usize::from(ChunkSlot::for_chunk(&id).value())]].clone()
            }
            None => snapshot.routes.iter().find_map(Clone::clone),
        }
    }
}

impl Drop for NativeChunkRoutes {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl ChunkdbClient {
    /// Start a retained native resolver backed only by the service slot map.
    ///
    /// # Errors
    /// Rejects missing/invalid maps or initial discovery failures.
    pub async fn native_routes(self: &Arc<Self>) -> Result<Arc<NativeChunkRoutes>> {
        // Native request IDs are independent of the async Rust transport.
        let transport = Arc::new(ChunkdbRpcTransport::new());
        let state = Arc::new(State {
            snapshot: ArcSwap::from_pointee(self.native_snapshot(&transport).await?),
            refresh: Notify::new(),
        });
        let client = Arc::clone(self);
        let worker_state = Arc::clone(&state);
        let worker = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick.tick() => {},
                    () = worker_state.refresh.notified() => {},
                }
                match client.native_snapshot(&transport).await {
                    Ok(snapshot) => worker_state.snapshot.store(Arc::new(snapshot)),
                    Err(error) => tracing::warn!(%error, "native chunk service route refresh failed"),
                }
            }
        });
        Ok(Arc::new(NativeChunkRoutes { state, worker }))
    }

    async fn native_snapshot(&self, transport: &ChunkdbRpcTransport) -> Result<Snapshot> {
        self.refresh_routes().await?;
        let mut slots = Box::new([0; CHUNK_SLOT_COUNT as usize]);
        let mut routes = Vec::new();
        for binding in self.range_binding.snapshot() {
            if binding.slots.is_empty() {
                continue;
            }
            let index = routes.len();
            for slot in binding.slots.slots() {
                slots[usize::from(slot.value())] = index;
            }
            let route = if binding.rpc_endpoint.is_empty() {
                None
            } else {
                match transport.owned_route(&binding.rpc_endpoint) {
                    Ok(route) => Some(route),
                    Err(error) => {
                        tracing::warn!(owner = binding.instance_id, %error, "native chunk owner endpoint unavailable");
                        None
                    }
                }
            };
            routes.push(route);
        }
        Ok(Snapshot { slots, routes })
    }
}
