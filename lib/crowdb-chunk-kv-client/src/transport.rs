// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{
    BatchMutationRequest, BatchMutationResponse, ChunkKvResponse, MultiGetRequest, MultiGetResponse,
    PointRequest, ScanRequest, SeekRequest,
};
use crowdb_protocol::chunk_kv_group_wire::{
    decode_batch_mutation_response, decode_multi_get_response, encode_batch_mutation_request,
    encode_multi_get_request,
};
use crowdb_protocol::chunk_kv_ordered_wire::{encode_scan_request, encode_seek_request};
use crowdb_protocol::chunk_kv_wire::{decode_point_response, encode_point_request, ChunkKvWireError};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{
    Buffer, ConnectionPoolError, ConnectionPoolIndex, RpcClient, RpcError, RpcServer, SelectedConnection,
};

use crate::{ClientError, Result};

#[async_trait]
pub trait ChunkKvTransport: Send + Sync {
    /// Sends directly to one catalog owner endpoint.
    ///
    /// # Errors
    ///
    /// Returns only connection/transport failures; typed server outcomes remain
    /// inside `ChunkKvResponse`.
    async fn point(&self, endpoint: &str, request: &PointRequest) -> Result<ChunkKvResponse>;

    /// Sends one range-validated multi-get group directly to its owner.
    ///
    /// # Errors
    ///
    /// Returns only transport failures; group and item outcomes stay typed.
    async fn multi_get(&self, _endpoint: &str, _request: &MultiGetRequest) -> Result<MultiGetResponse> {
        Err(ClientError::Transport(
            "multi-get transport is not implemented".into(),
        ))
    }

    /// Sends one ordered partition-local mutation group directly to its owner.
    ///
    /// # Errors
    ///
    /// Returns only transport failures; group and item outcomes stay typed.
    async fn batch_mutate(
        &self,
        _endpoint: &str,
        _request: &BatchMutationRequest,
    ) -> Result<BatchMutationResponse> {
        Err(ClientError::Transport(
            "batch mutation transport is not implemented".into(),
        ))
    }

    /// Sends one ordered seek directly to a partition owner.
    ///
    /// # Errors
    ///
    /// Returns only transport failures; typed server outcomes remain encoded.
    async fn seek(&self, _endpoint: &str, _request: &SeekRequest) -> Result<ChunkKvResponse> {
        Err(ClientError::Transport("seek transport is not implemented".into()))
    }

    /// Sends one bounded directional partition scan directly to its owner.
    ///
    /// # Errors
    ///
    /// Returns only transport failures; typed server outcomes remain encoded.
    async fn scan(&self, _endpoint: &str, _request: &ScanRequest) -> Result<ChunkKvResponse> {
        Err(ClientError::Transport("scan transport is not implemented".into()))
    }
}

/// Bounded production crowdb-rpc connection pool for direct owner calls.
pub struct ChunkKvRpcTransport {
    server: Arc<RpcServer>,
    rpc: RpcClient,
    connections: ConnectionPoolIndex,
    next_rpc_request_id: AtomicU64,
}

impl ChunkKvRpcTransport {
    #[must_use]
    pub fn new(max_owners: usize, pool_size: usize, workers: u32) -> Self {
        let server = Arc::new(RpcServer::with_engines(None, 1, workers.max(1)));
        server.start();
        server.register_conn_count_gauge("chunk_kv.client.connections");
        let rpc = RpcClient::new();
        rpc.set_completion_pool_size(1024);
        rpc.start_reaper(30_000_000_000, 500_000_000);
        Self {
            server,
            rpc,
            connections: ConnectionPoolIndex::new(pool_size, Some(max_owners)),
            next_rpc_request_id: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> Result<u64> {
        let id = self.next_rpc_request_id.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            Err(ClientError::Transport("RPC request identity exhausted".into()))
        } else {
            Ok(id)
        }
    }

    fn connection(&self, endpoint: &str) -> Result<SelectedConnection> {
        let endpoint = normalize_endpoint(endpoint);
        let (host, port) = parse_endpoint(&endpoint)?;
        self.connections
            .get_or_try_install(&endpoint, || {
                let connection = self.server.connect(&host, port).map_err(|error| {
                    ClientError::Transport(format!("connect to {host}:{port}: {error:?}"))
                })?;
                self.rpc.attach(&connection);
                Ok(connection)
            })
            .map_err(|error| match error {
                ConnectionPoolError::Connect(error) => error,
                ConnectionPoolError::Capacity { .. } => {
                    ClientError::Transport("owner connection limit reached".into())
                }
            })
    }

    async fn call_control(
        &self,
        endpoint: &str,
        request_id: u64,
        control: Buffer,
        message_type: u16,
    ) -> Result<Buffer> {
        let connection = self.connection(endpoint)?;
        let normalized = normalize_endpoint(endpoint);
        let map_error = |error: RpcError| {
            if error.is_retryable() {
                self.connections.invalidate(&normalized, connection.generation());
            }
            ClientError::Transport(error.to_string())
        };
        let response = self
            .rpc
            .call(&self.server, &connection, request_id, control, None, message_type)
            .map_err(&map_error)?
            .await
            .map_err(map_error)?;
        response
            .control
            .ok_or_else(|| ClientError::Transport("RPC response omitted its control buffer".into()))
    }

    async fn call_point_response(
        &self,
        endpoint: &str,
        request_id: u64,
        control: Buffer,
        message_type: u16,
    ) -> Result<ChunkKvResponse> {
        let response = self
            .call_control(endpoint, request_id, control, message_type)
            .await?;
        decode_point_response(response.bytes()).map_err(|error| wire_error(&error))
    }
}

#[async_trait]
impl ChunkKvTransport for ChunkKvRpcTransport {
    async fn point(&self, endpoint: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        let id = self.next_id()?;
        let (bytes, offset) =
            encode_point_request(id, wall_time_ns(), request).map_err(|error| wire_error(&error))?;
        self.call_point_response(
            endpoint,
            id,
            Buffer::from_vec_offset(bytes, offset),
            FBMsgType::EChunkKvPointRequest.0 as u16,
        )
        .await
    }

    async fn seek(&self, endpoint: &str, request: &SeekRequest) -> Result<ChunkKvResponse> {
        let id = self.next_id()?;
        let (bytes, offset) =
            encode_seek_request(id, wall_time_ns(), request).map_err(|error| wire_error(&error))?;
        self.call_point_response(
            endpoint,
            id,
            Buffer::from_vec_offset(bytes, offset),
            FBMsgType::EChunkKvSeekRequest.0 as u16,
        )
        .await
    }

    async fn scan(&self, endpoint: &str, request: &ScanRequest) -> Result<ChunkKvResponse> {
        let id = self.next_id()?;
        let (bytes, offset) =
            encode_scan_request(id, wall_time_ns(), request).map_err(|error| wire_error(&error))?;
        self.call_point_response(
            endpoint,
            id,
            Buffer::from_vec_offset(bytes, offset),
            FBMsgType::EChunkKvScanRequest.0 as u16,
        )
        .await
    }

    async fn multi_get(&self, endpoint: &str, request: &MultiGetRequest) -> Result<MultiGetResponse> {
        let id = self.next_id()?;
        let bytes = encode_multi_get_request(id, wall_time_ns(), request)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        let response = self
            .call_control(
                endpoint,
                id,
                Buffer::from_vec(bytes),
                FBMsgType::EChunkKvMultiGetRequest.0 as u16,
            )
            .await?;
        let (response_id, _, response) = decode_multi_get_response(response.bytes())
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        if response_id != id {
            return Err(ClientError::Transport(
                "multi-get response identity mismatch".into(),
            ));
        }
        Ok(response)
    }

    async fn batch_mutate(
        &self,
        endpoint: &str,
        request: &BatchMutationRequest,
    ) -> Result<BatchMutationResponse> {
        let id = self.next_id()?;
        let bytes = encode_batch_mutation_request(id, wall_time_ns(), request)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        let response = self
            .call_control(
                endpoint,
                id,
                Buffer::from_vec(bytes),
                FBMsgType::EChunkKvBatchMutationRequest.0 as u16,
            )
            .await?;
        let (response_id, _, response) = decode_batch_mutation_response(response.bytes())
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        if response_id != id {
            return Err(ClientError::Transport(
                "batch mutation response identity mismatch".into(),
            ));
        }
        Ok(response)
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    endpoint.replacen("0.0.0.0:", "127.0.0.1:", 1)
}

fn parse_endpoint(endpoint: &str) -> Result<(String, i32)> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| ClientError::Transport(format!("invalid owner endpoint: {endpoint}")))?;
    let port = port
        .parse::<i32>()
        .map_err(|_| ClientError::Transport(format!("invalid owner endpoint port: {endpoint}")))?;
    if host.is_empty() || !(1..=u16::MAX.into()).contains(&port) {
        return Err(ClientError::Transport(format!(
            "invalid owner endpoint: {endpoint}"
        )));
    }
    Ok((host.into(), port))
}

fn wall_time_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}

fn wire_error(error: &ChunkKvWireError) -> ClientError {
    ClientError::Transport(error.to_string())
}
