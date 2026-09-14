// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Owned handles for consumers that bridge crowdb-rpc into another FFI.

use std::ffi::c_void;
use std::sync::Arc;

use crate::{Connection, RpcClient, RpcServer};

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
/// crowdb-rpc handles after their constructor returns.
#[derive(Clone)]
pub struct OwnedClientRoute {
    client: Arc<dyn RpcClientHandle>,
    server: Arc<RpcServer>,
    connection: Connection,
}

impl OwnedClientRoute {
    #[must_use]
    pub fn new<C>(client: Arc<C>, server: Arc<RpcServer>, connection: Connection) -> Self
    where
        C: RpcClientHandle + 'static,
    {
        Self {
            client,
            server,
            connection,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn raw_handles(&self) -> (*mut c_void, *mut c_void, *mut c_void) {
        (
            self.client.rpc_client_handle(),
            self.server.handle().cast(),
            self.connection.handle().cast(),
        )
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
