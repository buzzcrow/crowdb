// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Owned handles for consumers that bridge crowdb-rpc into another FFI.

use std::ffi::c_void;
use std::sync::Arc;

use crate::{Connection, RpcClient, RpcServer};

mod retirement;

pub(crate) use retirement::shutdown as drain_before_native_shutdown;

/// Supplies the raw crowdb-rpc client handle while an owner keeps it alive.
pub trait RpcClientHandle: Send + Sync {
    #[doc(hidden)]
    fn rpc_client_handle(&self) -> *mut c_void;
}

impl RpcClientHandle for RpcClient {
    fn rpc_client_handle(&self) -> *mut c_void {
        self.handle().cast()
    }
}

/// A matching client, server, and connection whose owners travel together.
///
/// This is the safe handoff boundary for higher-level FFIs that retain raw
/// crowdb-rpc handles after their constructor returns. Clones share one owner
/// bundle. Its final release schedules native teardown outside RPC callbacks;
/// those callbacks must return without waiting for that teardown.
#[derive(Clone)]
pub struct OwnedClientRoute {
    handles: Arc<RouteHandles>,
}

struct RouteHandles {
    owners: Option<RouteOwners>,
}

struct RouteOwners {
    client: Arc<dyn RpcClientHandle>,
    server: Arc<RpcServer>,
    connection: Connection,
}

impl Drop for RouteHandles {
    fn drop(&mut self) {
        if let Some(owners) = self.owners.take() {
            retirement::retire(owners);
        }
    }
}

impl OwnedClientRoute {
    fn owners(&self) -> &RouteOwners {
        self.handles.owners.as_ref().expect("live route owners")
    }

    /// Whether this retained connection can still accept requests.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.owners().connection.is_open()
    }

    #[must_use]
    pub fn new<C>(client: Arc<C>, server: Arc<RpcServer>, connection: Connection) -> Self
    where
        C: RpcClientHandle + 'static,
    {
        // Initialize the lifecycle worker before publishing a route. Ordinary
        // request-path clones and releases only change this Arc's refcount.
        retirement::register_owner();
        Self {
            handles: Arc::new(RouteHandles {
                owners: Some(RouteOwners {
                    client,
                    server,
                    connection,
                }),
            }),
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn raw_handles(&self) -> (*mut c_void, *mut c_void, *mut c_void) {
        let owners = self.owners();
        (
            owners.client.rpc_client_handle(),
            owners.server.handle().cast(),
            owners.connection.handle().cast(),
        )
    }

    /// Wait for queued and recursively queued owner teardown after route
    /// creators and callbacks have quiesced. Never call from an RPC callback.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn drain_retired_for_tests(timeout: std::time::Duration) -> bool {
        retirement::drain(timeout)
    }

    /// Number of owner bundles queued or being destroyed on the lifecycle worker.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn pending_retired_for_tests() -> usize {
        retirement::pending()
    }

    /// Finish queued native teardown after quiescing route creators and
    /// callbacks and dropping their routes. Never call from an RPC callback.
    #[must_use]
    pub fn drain_retired(timeout: std::time::Duration) -> bool {
        retirement::drain(timeout) && retirement::quiescent()
    }
}

impl std::fmt::Debug for OwnedClientRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("OwnedClientRoute").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_route_keeps_matching_handles() {
        let server = Arc::new(RpcServer::new(None));
        server.listen("127.0.0.1", 0).unwrap();
        server.start();
        let connection = server.connect("127.0.0.1", server.port()).unwrap();
        let client = Arc::new(RpcClient::new());
        client.attach(&connection);

        let route = OwnedClientRoute::new(client, server, connection);
        let (client, server, connection) = route.raw_handles();
        assert!(!client.is_null());
        assert!(!server.is_null());
        assert!(!connection.is_null());
    }
}
