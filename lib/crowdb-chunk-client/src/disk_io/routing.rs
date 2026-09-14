// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk data-path adapter over the routed semantic DiskIO client.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_diskio_client::{
    DiskId, DiskioClient, DiskioClientConfig, DiskioError, Durability, NativeDiskIoRoutes, SegmentTarget,
};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::diskdb::rpc::Segment;

use crate::{DiskWriter, IoError, Result};

/// Disk writer backed by the complete semantic DiskIO client.
pub struct RoutedDiskWriter {
    client: Arc<DiskioClient>,
}

impl RoutedDiskWriter {
    /// Discover live DiskIO owners and connect to their endpoints.
    pub async fn connect(service: &ServiceRegistryClient, hardware: &HardwareClient) -> Result<Self> {
        Self::connect_with_connections(service, hardware, 1).await
    }

    /// Discover owners and keep a fixed normal connection group per endpoint.
    pub async fn connect_with_connections(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
        connections_per_endpoint: usize,
    ) -> Result<Self> {
        Self::connect_with_connections_and_workers(service, hardware, connections_per_endpoint, 1).await
    }

    /// Discover owners with fixed connection groups and RPC I/O workers.
    pub async fn connect_with_connections_and_workers(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
        connections_per_endpoint: usize,
        rpc_workers: u32,
    ) -> Result<Self> {
        let config = DiskioClientConfig {
            normal_connections_per_endpoint: connections_per_endpoint,
            rpc_workers,
            ..DiskioClientConfig::default()
        };
        let client = DiskioClient::connect_with_clients(service.clone(), hardware.clone(), config)
            .await
            .map_err(map_topology_error)?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Publish a refreshed complete route generation.
    pub async fn refresh(&self, _service: &ServiceRegistryClient, _hardware: &HardwareClient) -> Result<()> {
        self.client
            .refresh()
            .await
            .map(|_| ())
            .map_err(map_topology_error)
    }

    /// Produce opaque retained routes for the native tree page store.
    pub fn storage_routes(&self) -> Result<NativeDiskIoRoutes> {
        self.client.native_routes().map_err(map_topology_error)
    }

    fn target(seg: &Segment, unit_bytes: u64) -> Result<SegmentTarget> {
        let disk_id = seg
            .disk_id
            .map(|disk_id| DiskId::new(disk_id.high, disk_id.low))
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let unit_size = u32::try_from(unit_bytes)
            .map_err(|_| IoError::WriteFailed("segment unit size exceeds u32".into()))?;
        SegmentTarget::new(
            disk_id,
            seg.zone_index,
            seg.unit_offset,
            seg.unit_count,
            unit_size,
        )
        .map_err(map_write_error)
    }
}

#[async_trait]
impl DiskWriter for RoutedDiskWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        let target = Self::target(seg, unit_bytes)?;
        self.client
            .write(
                target,
                0,
                data,
                Durability::Buffered,
                self.client.normal_options(),
            )
            .await
            .map_err(map_write_error)
    }

    async fn write_priority_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let target = Self::target(seg, unit_bytes)?;
        self.client
            .write(
                target,
                byte_offset,
                data,
                Durability::Buffered,
                self.client.normal_options().priority(),
            )
            .await
            .map_err(map_write_error)
    }

    async fn write_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let target = Self::target(seg, unit_bytes)?;
        self.client
            .write(
                target,
                byte_offset,
                data,
                Durability::Buffered,
                self.client.normal_options(),
            )
            .await
            .map_err(map_write_error)
    }

    async fn fsync(&self, seg: &Segment) -> Result<()> {
        let disk_id = disk_id(seg).map_err(map_write_error)?;
        self.client
            .fsync(disk_id, self.client.normal_options())
            .await
            .map_err(map_write_error)
    }

    async fn fsync_priority(&self, seg: &Segment) -> Result<()> {
        let disk_id = disk_id(seg).map_err(map_write_error)?;
        self.client
            .fsync(disk_id, self.client.normal_options().priority())
            .await
            .map_err(map_write_error)
    }

    async fn read(&self, seg: &Segment, unit_bytes: u64, segment_offset: u64, length: u32) -> Result<Bytes> {
        let target = Self::target(seg, unit_bytes).map_err(|error| IoError::ReadFailed(error.to_string()))?;
        self.client
            .read(target, segment_offset, length, self.client.normal_options())
            .await
            .map_err(map_read_error)
    }
}

fn disk_id(segment: &Segment) -> std::result::Result<DiskId, DiskioError> {
    segment
        .disk_id
        .map(|disk_id| DiskId::new(disk_id.high, disk_id.low))
        .ok_or_else(|| DiskioError::InvalidInput("segment missing disk_id".into()))
}

#[allow(clippy::needless_pass_by_value, reason = "used directly as a map_err adapter")]
fn map_topology_error(error: DiskioError) -> IoError {
    IoError::Topology(error.to_string())
}

#[allow(clippy::needless_pass_by_value, reason = "used directly as a map_err adapter")]
fn map_write_error(error: DiskioError) -> IoError {
    IoError::WriteFailed(error.to_string())
}

#[allow(clippy::needless_pass_by_value, reason = "used directly as a map_err adapter")]
fn map_read_error(error: DiskioError) -> IoError {
    match error {
        DiskioError::TopologyUnavailable(_)
        | DiskioError::TransportUnavailable(_)
        | DiskioError::Backpressure(_)
        | DiskioError::DeadlineExceeded => IoError::TransientRead(error.to_string()),
        DiskioError::InvalidInput(_)
        | DiskioError::TopologyInconsistent(_)
        | DiskioError::DiskFailure(_)
        | DiskioError::PartialWrite
        | DiskioError::DurabilityFailure(_)
        | DiskioError::AmbiguousWrite(_)
        | DiskioError::Protocol(_) => IoError::ReadFailed(error.to_string()),
    }
}
