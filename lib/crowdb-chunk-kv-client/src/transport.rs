// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{
    BatchMutationRequest, BatchMutationResponse, ChunkKvResponse, MultiGetRequest, MultiGetResponse,
    PointRequest, ScanRequest, SeekRequest,
};
use crowdb_protocol::chunk_kv_group_wire::{
    decode_batch_mutation_response, decode_multi_get_response, encode_batch_mutation_request,
    encode_multi_get_request,
};
use crowdb_protocol::chunk_kv_ordered_wire::{
    encode_bounded_scan_request, encode_scan_request, encode_seek_request,
};
use crowdb_protocol::chunk_kv_wire::{encode_point_request, ChunkKvWireError};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::Buffer;

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
    /// Sends a strict forward scan; implementations must not fall back to legacy scanning.
    async fn scan_bounded(
        &self,
        _endpoint: &str,
        _request: &ScanRequest,
        _max_bytes: usize,
    ) -> Result<ChunkKvResponse> {
        Err(ClientError::InvalidRequest(
            "strict scan transport unavailable".into(),
        ))
    }
}

mod config;
mod endpoint;
mod rpc;
mod status;

pub use config::ChunkKvRpcTransportConfig;
pub use rpc::ChunkKvRpcTransport;

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

    async fn scan_bounded(
        &self,
        endpoint: &str,
        request: &ScanRequest,
        max_bytes: usize,
    ) -> Result<ChunkKvResponse> {
        let id = self.next_id()?;
        let (bytes, offset) = encode_bounded_scan_request(
            id,
            wall_time_ns(),
            request,
            u64::try_from(max_bytes).map_err(|_| ClientError::TooLarge)?,
        )
        .map_err(|error| wire_error(&error))?;
        self.call_point_response(
            endpoint,
            id,
            Buffer::from_vec_offset(bytes, offset),
            FBMsgType::EChunkKvBoundedScanRequest.0 as u16,
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
