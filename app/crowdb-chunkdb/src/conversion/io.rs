// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Priority-lane adapter for background conversion and repair DiskIO.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, Durability, SegmentTarget};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::diskdb::rpc::Segment;

#[derive(Debug, thiserror::Error)]
pub enum ConversionIoError {
    #[error("conversion DiskIO topology error: {0}")]
    Topology(String),
    #[error("conversion DiskIO operation failed: {0}")]
    Io(String),
}

/// Conversion-specific policy adapter over the shared semantic client.
pub struct ConversionDiskIo {
    client: Option<Arc<DiskioClient>>,
}

impl ConversionDiskIo {
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn empty_for_tests() -> Self {
        Self { client: None }
    }

    pub async fn connect(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
    ) -> Result<Self, ConversionIoError> {
        let client = DiskioClient::connect_with_clients(
            service.clone(),
            hardware.clone(),
            DiskioClientConfig {
                normal_connections_per_endpoint: 1,
                priority_connections_per_endpoint: 1,
                ..DiskioClientConfig::default()
            },
        )
        .await
        .map_err(|error| ConversionIoError::Topology(error.to_string()))?;
        Ok(Self {
            client: Some(Arc::new(client)),
        })
    }

    pub async fn refresh(
        &self,
        _service: &ServiceRegistryClient,
        _hardware: &HardwareClient,
    ) -> Result<(), ConversionIoError> {
        self.client()?
            .refresh()
            .await
            .map(|_| ())
            .map_err(|error| ConversionIoError::Topology(error.to_string()))
    }

    pub async fn read_segment(&self, segment: &Segment, unit_bytes: u64) -> Result<Bytes, ConversionIoError> {
        let target = target(segment, unit_bytes)?;
        let length = u32::try_from(target.capacity())
            .map_err(|_| ConversionIoError::Io("segment read size exceeds u32".into()))?;
        self.client()?
            .read(target, 0, length, self.client()?.normal_options().priority())
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
        if self.client.is_none() {
            return Ok(Bytes::from(vec![
                0;
                usize::try_from(length).expect("u32 fits usize")
            ]));
        }
        let target = target(segment, unit_bytes)?;
        self.client()?
            .read(target, offset, length, self.client()?.normal_options().priority())
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
        self.client()?
            .write(
                target,
                0,
                data,
                Durability::Buffered,
                self.client()?.normal_options().priority(),
            )
            .await
            .map_err(|error| ConversionIoError::Io(error.to_string()))
    }

    pub async fn fsync_segment(&self, segment: &Segment) -> Result<(), ConversionIoError> {
        let disk_id = disk_id(segment)?;
        self.client()?
            .fsync(disk_id, self.client()?.normal_options().priority())
            .await
            .map_err(|error| ConversionIoError::Io(error.to_string()))
    }

    fn client(&self) -> Result<&DiskioClient, ConversionIoError> {
        self.client
            .as_deref()
            .ok_or_else(|| ConversionIoError::Topology("test DiskIO client is not connected".into()))
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
