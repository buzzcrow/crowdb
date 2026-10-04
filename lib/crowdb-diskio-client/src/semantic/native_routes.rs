// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Notify;

use crowdb_rpc_ffi::{OwnedClientRoute, RpcClient, RpcServer};

use super::{topology, DiskioClient, DiskioError, DiskioResult, NativeDiskIoRoutes};

/// Retained native routes refreshed outside synchronous tree callbacks.
pub struct NativeDiskRouteResolver {
    routes: Arc<ArcSwap<NativeDiskIoRoutes>>,
    refresh: Arc<Notify>,
    worker: tokio::task::JoinHandle<()>,
}

impl NativeDiskRouteResolver {
    #[must_use]
    pub fn resolve(&self, high: u64, low: u64) -> Option<OwnedClientRoute> {
        let id = crate::DiskId::new(high, low);
        let routes = self.routes.load();
        let route = routes.routes.iter().find(|(disk, _)| *disk == id)?.1.clone();
        if route.is_open() {
            Some(route)
        } else {
            self.refresh.notify_one();
            None
        }
    }
}

impl Drop for NativeDiskRouteResolver {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl DiskioClient {
    /// Keep native `DiskIO` connections current after endpoint restarts.
    ///
    /// # Errors
    /// Returns the initial topology or connection error.
    pub fn native_route_resolver(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> DiskioResult<Arc<NativeDiskRouteResolver>> {
        let routes = Arc::new(ArcSwap::from_pointee(self.native_routes_with_timeout(timeout)?));
        let refresh = Arc::new(Notify::new());
        let worker_routes = Arc::clone(&routes);
        let worker_refresh = Arc::clone(&refresh);
        let client = Arc::clone(self);
        let mut observed = self.routes.load_full();
        let worker = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick.tick() => {},
                    () = worker_refresh.notified() => {},
                }
                client.refresh_topology_if_due().await;
                let current = client.routes.load_full();
                if observed.routes == current.routes
                    && worker_routes
                        .load()
                        .routes
                        .iter()
                        .all(|(_, route)| route.is_open())
                {
                    continue;
                }
                match client.native_routes_with_timeout(timeout) {
                    Ok(snapshot) => {
                        worker_routes.store(Arc::new(snapshot));
                        observed = current;
                    }
                    Err(error) => tracing::warn!(%error, "native DiskIO route refresh failed"),
                }
            }
        });
        Ok(Arc::new(NativeDiskRouteResolver {
            routes,
            refresh,
            worker,
        }))
    }

    /// Export dedicated connections for the native tree transport's request-ID
    /// namespace. Native callers must not share Rust's RPC completion slots.
    ///
    /// # Errors
    /// Returns topology or connection errors for unavailable disk endpoints.
    pub fn native_routes(&self) -> DiskioResult<NativeDiskIoRoutes> {
        self.native_routes_with_timeout(Duration::from_secs(10))
    }

    fn native_routes_with_timeout(&self, timeout: Duration) -> DiskioResult<NativeDiskIoRoutes> {
        let snapshot = self.routes.load_full();
        let server = Arc::new(RpcServer::with_engines(None, 1, self.config.rpc_workers));
        server.start();
        let client = Arc::new(RpcClient::new());
        client.set_completion_pool_size(1024);
        client.start_reaper(u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX), 100_000_000);
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
