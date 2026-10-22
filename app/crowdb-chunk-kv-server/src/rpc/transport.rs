// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_rpc_ffi::{RdmaConfig, RdmaWireLayout, RpcError, RpcServer};

use super::ChunkKvRpcService;

impl ChunkKvRpcService {
    /// Create the service's aggregate-capacity listener before binding it.
    ///
    /// # Errors
    /// Rejects invalid classes/queues or unavailable explicitly enabled RDMA.
    pub fn create_server(workers: u32, rdma: Option<&RdmaConfig>) -> Result<Arc<RpcServer>, RpcError> {
        let server = if let Some(rdma) = rdma {
            if rdma.enabled && rdma.control_data_capacity().is_none() {
                return Err(RpcError::InvalidArg);
            }
            let server = RpcServer::with_engines(None, 1, workers.max(1));
            let mut native = rdma.clone();
            native.listen_enabled = native.enabled;
            server.configure_rdma(&native)?;
            server
        } else {
            RpcServer::with_rdma_layout(None, 1, workers.max(1), RdmaWireLayout::ControlData, 262_144)?
        };
        Ok(Arc::new(server))
    }
}
