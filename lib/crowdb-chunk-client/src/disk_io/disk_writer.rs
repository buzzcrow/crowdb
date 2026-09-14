// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskWriter` test seam and segment validation.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::diskdb::rpc::Segment;

use crate::{IoError, Result};

/// Block-IO seam. Production routing is owned by `crowdb-diskio-client`.
#[async_trait]
pub trait DiskWriter: Send + Sync {
    /// Write `data` to the disk/zone/offset described by `seg`.
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()>;

    /// Write a conversion-data range without queueing behind ordinary writes.
    async fn write_priority_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        self.write_at_byte_offset(seg, unit_bytes, byte_offset, data)
            .await
    }

    /// Flush the disk containing `seg`.
    async fn fsync(&self, _seg: &Segment) -> Result<()> {
        Ok(())
    }

    /// Flush conversion data through the priority lane when available.
    async fn fsync_priority(&self, seg: &Segment) -> Result<()> {
        self.fsync(seg).await
    }

    /// Read an arbitrary byte range relative to the start of `seg`.
    async fn read(
        &self,
        _seg: &Segment,
        _unit_bytes: u64,
        _segment_offset: u64,
        _length: u32,
    ) -> Result<Bytes> {
        Err(IoError::ReadFailed(
            "reads are unsupported by this DiskIO seam".into(),
        ))
    }

    /// Write a unit-aligned range relative to the start of `seg`.
    async fn write_at(&self, seg: &Segment, unit_bytes: u64, segment_offset: u64, data: Bytes) -> Result<()> {
        validate_segment_write(seg, unit_bytes, segment_offset, data.len())?;
        let mut adjusted = *seg;
        let offset_units = segment_offset / unit_bytes;
        adjusted.unit_offset = adjusted
            .unit_offset
            .checked_add(offset_units)
            .ok_or_else(|| IoError::WriteFailed("segment unit offset overflow".into()))?;
        adjusted.unit_count = adjusted
            .unit_count
            .checked_sub(u32::try_from(offset_units).map_err(|_| {
                IoError::WriteFailed("segment-relative offset exceeds unit-count range".into())
            })?)
            .ok_or_else(|| IoError::WriteFailed("segment-relative offset exceeds segment".into()))?;
        self.write(&adjusted, unit_bytes, data).await
    }

    /// Write at an arbitrary byte offset within the segment.
    async fn write_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()>;
}

fn validate_segment_write(
    seg: &Segment,
    unit_bytes: u64,
    segment_offset: u64,
    data_len: usize,
) -> Result<()> {
    if unit_bytes == 0 {
        return Err(IoError::WriteFailed("unit_bytes must be nonzero".into()));
    }
    let data_len =
        u64::try_from(data_len).map_err(|_| IoError::WriteFailed("write length exceeds u64".into()))?;
    if segment_offset % unit_bytes != 0 || data_len == 0 || data_len % unit_bytes != 0 {
        return Err(IoError::WriteFailed(
            "segment-relative offset and write length must be unit aligned".into(),
        ));
    }
    let segment_bytes = u64::from(seg.unit_count)
        .checked_mul(unit_bytes)
        .ok_or_else(|| IoError::WriteFailed("segment byte capacity overflow".into()))?;
    let end = segment_offset
        .checked_add(data_len)
        .ok_or_else(|| IoError::WriteFailed("segment-relative write end overflow".into()))?;
    if end > segment_bytes {
        return Err(IoError::WriteFailed(format!(
            "segment-relative write end {end} exceeds segment capacity {segment_bytes}"
        )));
    }
    Ok(())
}
