// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Connection acquisition and owned transport lifecycle.

use super::{normalize_endpoint, parse_endpoint, ChunkdbRpcTransport, REQUEST_IDENTITY};
use crate::{ChunkdbClientError, Result};
use crowdb_rpc_ffi::{
    ConnectionPoolError, ConnectionPoolIndex, OwnedClientRoute, RpcClient, RpcServer, SelectedConnection,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

impl ChunkdbRpcTransport {
    /// Closes this owned transport and joins its workers and timeout reaper.
    pub fn shutdown(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.server.stop();
        self.connections.clear();
        self.rpc.stop_reaper();
    }

    /// Create a new crowdb-rpc transport with 2 I/O workers (default).
    /// The `RpcServer` is the client-side transport — it does not listen
    /// but is used to establish connections to remote endpoints.
    #[must_use]
    pub fn new() -> Self {
        Self::with_pool_size(1, 2)
    }

    /// Create a new crowdb-rpc transport with `workers` I/O worker threads.
    #[must_use]
    pub fn with_workers(workers: u32) -> Self {
        Self::with_pool_size(1, workers)
    }

    /// Create a transport with `pool_size` connections per endpoint and
    /// `workers` crowdb-rpc I/O workers.
    #[must_use]
    pub fn with_pool_size(pool_size: usize, workers: u32) -> Self {
        let server = Arc::new(RpcServer::with_engines(None, 1, workers));
        server.start();
        server.register_conn_count_gauge("rpc.client.connections");
        let rpc = Arc::new(RpcClient::new());
        rpc.set_completion_pool_size(1024);
        // A chunk allocation spans DiskDB allocate + commit and two KV state
        // writes. Keep the client deadline aligned with DiskDB's 10-second
        // mutation window so a temporary KV tail does not abandon the whole
        // chunk after its physical blocks have already been allocated.
        rpc.start_reaper(10_000_000_000, 500_000_000);
        Self {
            closed: AtomicBool::new(false),
            server,
            rpc,
            connections: ConnectionPoolIndex::new(pool_size, None),
            next_req_id: AtomicU64::new(1),
        }
    }

    pub(super) fn next_id(&self) -> u64 {
        REQUEST_IDENTITY
            .try_with(|(transport, id)| (*transport == self as *const Self as usize).then_some(*id))
            .ok()
            .flatten()
            .unwrap_or_else(|| self.next_req_id.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) async fn with_request_identity<F: std::future::Future>(&self, future: F) -> F::Output {
        let id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        REQUEST_IDENTITY
            .scope((self as *const Self as usize, id), future)
            .await
    }

    /// Get or create a `Connection` for the given rpc endpoint.
    pub(super) fn conn_for(&self, rpc_endpoint: &str) -> Result<SelectedConnection> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ChunkdbClientError::Unreachable(
                "ChunkDB RPC transport is closed".into(),
            ));
        }
        let normalized = normalize_endpoint(rpc_endpoint);
        let (host, port) = parse_endpoint(&normalized).map_err(|reason| {
            ChunkdbClientError::Unreachable(format!("invalid endpoint {rpc_endpoint}: {reason}"))
        })?;
        self.connections
            .get_or_try_install(&normalized, || {
                let conn = self.server.connect(&host, port).map_err(|error| {
                    ChunkdbClientError::ConnectFailed(format!("rpc connect to {host}:{port}: {error:?}"))
                })?;
                self.rpc.attach(&conn);
                Ok(conn)
            })
            .map_err(|error| match error {
                ConnectionPoolError::Closed => {
                    ChunkdbClientError::ConnectFailed("connection closed during acquisition".into())
                }
                ConnectionPoolError::Connect(error) => error,
                ConnectionPoolError::Capacity { max_endpoints } => ChunkdbClientError::Unreachable(format!(
                    "endpoint connection limit {max_endpoints} reached"
                )),
            })
    }

    /// Resolve an endpoint and retain every crowdb-rpc owner needed by a
    /// higher-level FFI transport.
    pub fn owned_route(&self, rpc_endpoint: &str) -> Result<OwnedClientRoute> {
        let connection = self.conn_for(rpc_endpoint)?;
        Ok(OwnedClientRoute::new(
            Arc::clone(&self.rpc),
            Arc::clone(&self.server),
            connection.into_connection(),
        ))
    }
}
