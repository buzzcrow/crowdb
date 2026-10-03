// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use crowdb_rpc_ffi::{OwnedClientRoute, RpcClient, RpcServer};

use super::{topology, DiskioClient, DiskioError, DiskioResult, NativeDiskIoRoutes};

impl DiskioClient {
    /// Export dedicated connections for the native tree transport's request-ID
    /// namespace. Native callers must not share Rust's RPC completion slots.
    ///
    /// # Errors
    /// Returns topology or connection errors for unavailable disk endpoints.
    pub fn native_routes(&self) -> DiskioResult<NativeDiskIoRoutes> {
        let snapshot = self.routes.load_full();
        let server = Arc::new(RpcServer::with_engines(None, 1, self.config.rpc_workers));
        server.start();
        let client = Arc::new(RpcClient::new());
        client.set_completion_pool_size(1024);
        client.start_reaper(10_000_000_000, 500_000_000);
        let mut endpoints = HashMap::new();
        let mut routes = Vec::with_capacity(snapshot.routes.len());
        for (disk_id, target) in &snapshot.routes {
            let route = if let Some(route) = endpoints.get(&target.endpoint) {
                OwnedClientRoute::clone(route)
            } else {
                let (host, port) = topology::parse_endpoint(&target.endpoint)?;
                let connection = server.connect(host, port).map_err(|error| {
                    DiskioError::TransportUnavailable(format!("native DiskIO connection: {error:?}"))
                })?;
                client.attach(&connection);
                let route = OwnedClientRoute::new(Arc::clone(&client), Arc::clone(&server), connection);
                endpoints.insert(target.endpoint.clone(), route.clone());
                route
            };
            routes.push((*disk_id, route));
        }
        Ok(NativeDiskIoRoutes { routes })
    }
}
