// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Priority-lane adapter for background conversion and repair DiskIO.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, Durability, SegmentTarget};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::diskdb::rpc::Segment;

use crate::chunkdb_config::ConversionIoConfig;

#[derive(Debug, thiserror::Error)]
pub enum ConversionIoError {
    #[error("conversion DiskIO topology error: {0}")]
    Topology(String),
    #[error("conversion DiskIO operation failed: {0}")]
    Io(String),
}

/// Conversion-specific policy adapter over the shared semantic client.
pub struct ConversionDiskIo {
    client: ArcSwapOption<DiskioClient>,
    config: ConversionIoConfig,
}

impl ConversionDiskIo {
    pub fn deferred(config: ConversionIoConfig) -> Self {
        Self {
            client: ArcSwapOption::empty(),
            config,
        }
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn empty_for_tests() -> Self {
        Self::deferred(ConversionIoConfig::default())
    }

    pub async fn connect(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
    ) -> Result<Self, ConversionIoError> {
        Self::connect_with_config(service, hardware, &ConversionIoConfig::default()).await
    }

    pub async fn connect_with_config(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
        config: &ConversionIoConfig,
    ) -> Result<Self, ConversionIoError> {
        let io = Self::deferred(config.clone());
        io.refresh(service, hardware).await?;
        Ok(io)
    }

    pub async fn refresh(
        &self,
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
    ) -> Result<(), ConversionIoError> {
        if let Some(client) = self.client.load_full() {
            return client
                .refresh()
                .await
                .map(|_| ())
                .map_err(|error| ConversionIoError::Topology(error.to_string()));
        }
        let client = DiskioClient::connect_with_clients(
            service.clone(),
            hardware.clone(),
            DiskioClientConfig {
                normal_connections_per_endpoint: self.config.normal_connections_per_endpoint,
                priority_connections_per_endpoint: self.config.priority_connections_per_endpoint,
                rpc_workers: self.config.rpc_workers,
                ..DiskioClientConfig::default()
            },
        )
        .await
        .map_err(|error| ConversionIoError::Topology(error.to_string()))?;
        self.client.store(Some(Arc::new(client)));
        Ok(())
    }

    pub async fn read_segment(&self, segment: &Segment, unit_bytes: u64) -> Result<Bytes, ConversionIoError> {
        let target = target(segment, unit_bytes)?;
        let length = u32::try_from(target.capacity())
            .map_err(|_| ConversionIoError::Io("segment read size exceeds u32".into()))?;
        let client = self.client()?;
        client
            .read(target, 0, length, client.normal_options().priority())
            .await
            .map_err(|error| ConversionIoError::Io(error.to_string()))
    }

    /// Read a byte range from a segment without imposing frame alignment on
    /// callers. DiskIO owns any device-level read-modify policy.
    pub async fn read_segment_range(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, ConversionIoError> {
        #[cfg(feature = "test-util")]
        if self.client.load().is_none() {
            return Ok(Bytes::from(vec![
                0;
                usize::try_from(length).expect("u32 fits usize")
            ]));
        }
        let target = target(segment, unit_bytes)?;
        let client = self.client()?;
        client
            .read(target, offset, length, client.normal_options().priority())
            .await
            .map_err(|error| ConversionIoError::Io(error.to_string()))
    }

    pub async fn write_segment(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        data: Bytes,
    ) -> Result<(), ConversionIoError> {
        let target = target(segment, unit_bytes)?;
        let client = self.client()?;
        client
            .write(
                target,
                0,
                data,
                Durability::Buffered,
                client.normal_options().priority(),
            )
            .await
            .map_err(|error| ConversionIoError::Io(error.to_string()))
    }

    pub async fn fsync_segment(&self, segment: &Segment) -> Result<(), ConversionIoError> {
        let disk_id = disk_id(segment)?;
        let client = self.client()?;
        client
            .fsync(disk_id, client.normal_options().priority())
            .await
            .map_err(|error| ConversionIoError::Io(error.to_string()))
    }

    fn client(&self) -> Result<Arc<DiskioClient>, ConversionIoError> {
        self.client
            .load_full()
            .ok_or_else(|| ConversionIoError::Topology("background DiskIO client is not connected".into()))
    }
}

fn target(segment: &Segment, unit_bytes: u64) -> Result<SegmentTarget, ConversionIoError> {
    let unit_size = u32::try_from(unit_bytes)
        .map_err(|_| ConversionIoError::Io("segment unit size exceeds u32".into()))?;
    SegmentTarget::new(
        disk_id(segment)?,
        segment.zone_index,
        segment.unit_offset,
        segment.unit_count,
        unit_size,
    )
    .map_err(|error| ConversionIoError::Io(error.to_string()))
}

fn disk_id(segment: &Segment) -> Result<DiskId, ConversionIoError> {
    segment
        .disk_id
        .map(|disk_id| DiskId::new(disk_id.high, disk_id.low))
        .ok_or_else(|| ConversionIoError::Topology("segment has no disk id".into()))
}
