// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crowdb_protocol::chunk_kv::ChunkKvResponse;
use crowdb_protocol::chunk_kv_wire::decode_point_response;
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{
    Buffer, ConnectionPoolError, ConnectionPoolIndex, RdmaConfig, RdmaWireLayout, RpcClient, RpcError,
    RpcServer, SelectedConnection,
};

use super::config::ChunkKvRpcTransportConfig;
use super::endpoint::{normalize_endpoint, parse_endpoint};
use crate::{ClientError, Result};

pub(super) struct RpcGroup {
    pub(super) server: Arc<RpcServer>,
    pub(super) connections: ConnectionPoolIndex,
    pub(super) data_capacity: Option<u32>,
}

impl RpcGroup {
    fn new(
        max_owners: usize,
        pool_size: usize,
        workers: u32,
        capacity: u32,
        config: Option<&RdmaConfig>,
    ) -> Result<Self> {
        let server = if let Some(config) = config {
            let server = RpcServer::with_engines(None, 1, workers.max(1));
            let mut native = config.clone();
            native.listen_enabled = false;
            server.configure_rdma(&native).map_err(rpc_error)?;
            server
        } else {
            RpcServer::with_rdma_layout(None, 1, workers.max(1), RdmaWireLayout::ControlData, capacity)
                .map_err(rpc_error)?
        };
        server.start();
        Ok(Self {
            server: Arc::new(server),
            connections: ConnectionPoolIndex::new(pool_size, Some(max_owners)),
            data_capacity: config.map_or(Some(capacity), RdmaConfig::control_data_capacity),
        })
    }

    fn connection(&self, endpoint: &str, rpc: &RpcClient) -> Result<SelectedConnection> {
        let (host, port) = parse_endpoint(endpoint)?;
        self.connections
            .get_or_try_install(endpoint, || {
                let connection = self.server.connect(&host, port).map_err(|error| {
                    ClientError::Transport(format!("connect to {host}:{port}: {error:?}"))
                })?;
                rpc.attach(&connection);
                Ok(connection)
            })
            .map_err(|error| match error {
                ConnectionPoolError::Closed => {
                    ClientError::Transport("connection closed during acquisition".into())
                }
                ConnectionPoolError::Connect(error) => error,
                ConnectionPoolError::Capacity { .. } => {
                    ClientError::Transport("owner connection limit reached".into())
                }
            })
    }
}

/// Fixed data-class transports sharing the existing RPC completion engine.
pub struct ChunkKvRpcTransport {
    pub(super) metadata: RpcGroup,
    pub(super) aggregate: RpcGroup,
    pub(super) rpc: RpcClient,
    next_rpc_request_id: AtomicU64,
}

impl ChunkKvRpcTransport {
    /// Create the fixed service layouts with environment transport policy.
    ///
    /// # Panics
    /// Panics if the native engine cannot create the fixed service layouts.
    /// `with_config` returns initialization failures to callers that need them.
    #[must_use]
    pub fn new(max_owners: usize, pool_size: usize, workers: u32) -> Self {
        Self::with_config(
            max_owners,
            pool_size,
            workers,
            &ChunkKvRpcTransportConfig::default(),
        )
        .expect("fixed Chunk-KV service data classes are valid")
    }

    /// Build fixed class groups before publishing the client.
    ///
    /// # Errors
    /// Rejects invalid class/queue/budget settings or unavailable explicit RDMA.
    pub fn with_config(
        max_owners: usize,
        pool_size: usize,
        workers: u32,
        config: &ChunkKvRpcTransportConfig,
    ) -> Result<Self> {
        config.validate()?;
        let metadata = RpcGroup::new(
            max_owners,
            pool_size,
            workers,
            65_536,
            config.metadata_rdma.as_ref(),
        )?;
        let aggregate = RpcGroup::new(
            max_owners,
            pool_size,
            workers,
            262_144,
            config.aggregate_rdma.as_ref(),
        )?;
        metadata
            .server
            .register_conn_count_gauge("chunk_kv.client.metadata.connections");
        aggregate
            .server
            .register_conn_count_gauge("chunk_kv.client.aggregate.connections");
        let rpc = RpcClient::new();
        rpc.set_completion_pool_size(1024);
        rpc.start_reaper(30_000_000_000, 500_000_000);
        Ok(Self {
            metadata,
            aggregate,
            rpc,
            next_rpc_request_id: AtomicU64::new(1),
        })
    }

    pub(super) fn next_id(&self) -> Result<u64> {
        let id = self.next_rpc_request_id.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            Err(ClientError::Transport("RPC request identity exhausted".into()))
        } else {
            Ok(id)
        }
    }

    fn group(&self, message_type: u16) -> &RpcGroup {
        if message_type == FBMsgType::EChunkKvPointRequest.0 as u16 {
            &self.metadata
        } else {
            &self.aggregate
        }
    }

    pub(super) async fn call_control(
        &self,
        endpoint: &str,
        request_id: u64,
        control: Buffer,
        message_type: u16,
    ) -> Result<Buffer> {
        let group = self.group(message_type);
        let normalized = normalize_endpoint(endpoint);
        let connection = group.connection(&normalized, &self.rpc)?;
        let map_error = |error: RpcError| {
            if error.is_retryable() {
                group.connections.invalidate(&normalized, connection.generation());
            }
            rpc_error(error)
        };
        let response = self
            .rpc
            .call(
                &group.server,
                &connection,
                request_id,
                control,
                None,
                message_type,
            )
            .map_err(&map_error)?
            .await
            .map_err(map_error)?;
        response
            .control
            .ok_or_else(|| ClientError::Transport("RPC response omitted its control buffer".into()))
    }

    pub(super) async fn call_point_response(
        &self,
        endpoint: &str,
        request_id: u64,
        control: Buffer,
        message_type: u16,
    ) -> Result<ChunkKvResponse> {
        let response = self
            .call_control(endpoint, request_id, control, message_type)
            .await?;
        decode_point_response(response.bytes()).map_err(|error| super::wire_error(&error))
    }
}

fn rpc_error(error: RpcError) -> ClientError {
    ClientError::Transport(error.to_string())
}
