// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared transport owners and independently invalidated point/ordinary pools.

use super::{
    Arc, AtomicU64, ConnectionPoolError, ConnectionPoolIndex, Error, PaxosKvRpcTransport, Result, RpcClient,
    RpcError, RpcServer, SelectedConnection,
};

impl PaxosKvRpcTransport {
    /// Create a new crowdb-rpc transport with `pool_size` connections
    /// per endpoint, using the given RPC tunables and `workers` I/O
    /// worker threads. The `RpcServer` is the client-side transport —
    /// it does not listen but is used to establish connections to
    /// remote endpoints.
    #[must_use]
    pub fn with_pool_size(
        pool_size: usize,
        enable_nagle: bool,
        quickack: bool,
        event_write: bool,
        send_queue_capacity: u32,
        workers: u32,
    ) -> Self {
        let server = Arc::new(RpcServer::with_engines(None, 1, workers));
        server.set_tcp_nodelay(!enable_nagle);
        server.set_quickack(quickack);
        server.set_event_write(event_write);
        server.set_send_queue_capacity(send_queue_capacity);
        server.start();
        server.register_conn_count_gauge("rpc.client.connections");
        let rpc = Arc::new(RpcClient::new());
        rpc.set_completion_pool_size(1024);
        rpc.start_reaper(5_000_000_000, 500_000_000);
        Self {
            server,
            rpc,
            connections: ConnectionPoolIndex::new(pool_size, None),
            point_connections: None,
            next_req_id: AtomicU64::new(1),
        }
    }

    /// Create a new crowdb-rpc transport with a single connection per
    /// endpoint and default tunables (backward-compatible).
    #[must_use]
    pub fn new() -> Self {
        Self::with_pool_size(1, false, false, false, 4096, 2)
    }

    /// Reserve one connection per endpoint for Get replies, independently of
    /// scan and mutation frames. Uses the same I/O workers, correlation pool,
    /// deadlines and tunables. Ordinary transports keep their existing pool.
    #[must_use]
    pub fn with_isolated_point_reads(mut self) -> Self {
        self.point_connections = Some(ConnectionPoolIndex::new(1, None));
        self
    }

    /// Clear both pools after an explicit reconnect request.
    pub fn clear_connections(&self) {
        self.connections.clear();
        if let Some(points) = &self.point_connections {
            points.clear();
        }
    }

    fn point_index(&self) -> &ConnectionPoolIndex {
        self.point_connections.as_ref().unwrap_or(&self.connections)
    }

    pub(super) fn map_rpc_err(&self, error: RpcError, endpoint: &str, generation: u64) -> Error {
        map_error(error, endpoint, generation, &self.connections)
    }

    pub(super) fn map_get_rpc_err(&self, error: RpcError, endpoint: &str, generation: u64) -> Error {
        map_error(error, endpoint, generation, self.point_index())
    }

    pub(super) fn conn_for(&self, endpoint: &str) -> Result<SelectedConnection> {
        self.conn_for_in(endpoint, &self.connections)
    }

    pub(super) fn conn_for_point(&self, endpoint: &str) -> Result<SelectedConnection> {
        self.conn_for_in(endpoint, self.point_index())
    }

    fn conn_for_in(&self, rpc_endpoint: &str, index: &ConnectionPoolIndex) -> Result<SelectedConnection> {
        let normalized = normalize_endpoint(rpc_endpoint);
        let (host, port) = parse_endpoint(&normalized).map_err(|e| Error::InvalidEndpoint {
            endpoint: rpc_endpoint.to_string(),
            reason: e,
        })?;
        index
            .get_or_try_install(&normalized, || {
                let conn = self.server.connect(&host, port).map_err(|e| Error::Transport {
                    endpoint: rpc_endpoint.to_string(),
                    status: format!("rpc connect to {host}:{port}: {e:?}"),
                })?;
                self.rpc.attach(&conn);
                Ok(conn)
            })
            .map_err(|error| match error {
                ConnectionPoolError::Closed => Error::Transport {
                    endpoint: rpc_endpoint.to_string(),
                    status: "connection closed during acquisition".into(),
                },
                ConnectionPoolError::Connect(error) => error,
                ConnectionPoolError::Capacity { max_endpoints } => Error::Transport {
                    endpoint: rpc_endpoint.to_string(),
                    status: format!("endpoint connection limit {max_endpoints} reached"),
                },
            })
    }
}

fn map_error(error: RpcError, endpoint: &str, generation: u64, index: &ConnectionPoolIndex) -> Error {
    if matches!(
        error,
        RpcError::ConnectionClosed | RpcError::ConnectionError | RpcError::Timeout
    ) {
        index.invalidate(&normalize_endpoint(endpoint), generation);
    }
    Error::Transport {
        endpoint: endpoint.to_string(),
        status: format!("rpc error: {error:?}"),
    }
}

pub(super) fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}

fn parse_endpoint(endpoint: &str) -> std::result::Result<(String, i32), String> {
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let (host, port_str) = without_scheme
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid endpoint: {endpoint}"))?;
    let port: i32 = port_str
        .parse()
        .map_err(|_| format!("invalid port in endpoint: {endpoint}"))?;
    Ok((host.to_string(), port))
}
