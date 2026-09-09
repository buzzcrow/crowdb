// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskWriter` trait + `DiskioBlockWriter` (production impl).
//!
//! `DiskWriter` is the block-IO seam. `write` takes a `Segment`
//! directly (carrying disk_id, zone_index, unit_offset) + `unit_bytes`
//! to compute the byte offset — removing the repeated
//! disk_id/zone/offset extraction from call sites. `fsync` flushes a
//! disk. Production impl wraps `DiskioClient`; test impl
//! (`LocalFileDiskWriter`) writes to local files.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskIoRetCode, DiskioClient};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::{Connection, RpcServer};

use crate::{IoError, Result};

/// Block-IO seam. A successful write is durable for production BlockDisk.
#[async_trait]
pub trait DiskWriter: Send + Sync {
    /// Write `data` to the disk/zone/offset described by `seg`.
    /// `unit_bytes` converts `seg.unit_offset` to a byte offset.
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

    /// Flush the disk containing `seg`. Implementations whose writes are
    /// already durably synchronous may keep the default no-op.
    async fn fsync(&self, _seg: &Segment) -> Result<()> {
        Ok(())
    }

    /// Flush conversion data on its dedicated transport when available.
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

    /// Write an aligned range relative to the start of `seg`.
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

    /// Write `data` at an arbitrary byte offset within the segment, bypassing
    /// unit alignment.
    async fn write_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()>;
}

pub(super) fn validate_segment_byte_write(
    seg: &Segment,
    unit_bytes: u64,
    byte_offset: u64,
    length: usize,
) -> Result<()> {
    if unit_bytes == 0 {
        return Err(IoError::WriteFailed("unit_bytes must be nonzero".into()));
    }
    if length == 0 {
        return Err(IoError::WriteFailed("write length must be nonzero".into()));
    }
    let capacity = u64::from(seg.unit_count)
        .checked_mul(unit_bytes)
        .ok_or_else(|| IoError::WriteFailed("segment byte capacity overflow".into()))?;
    let length =
        u64::try_from(length).map_err(|_| IoError::WriteFailed("disk write length exceeds u64".into()))?;
    let end = byte_offset
        .checked_add(length)
        .ok_or_else(|| IoError::WriteFailed("disk write range overflow".into()))?;
    if end > capacity {
        return Err(IoError::WriteFailed(
            "segment-relative byte write exceeds segment".into(),
        ));
    }
    Ok(())
}

fn validate_segment_read(seg: &Segment, unit_bytes: u64, segment_offset: u64, length: u32) -> Result<()> {
    if unit_bytes == 0 {
        return Err(IoError::ReadFailed("unit_bytes must be nonzero".into()));
    }
    let segment_bytes = u64::from(seg.unit_count)
        .checked_mul(unit_bytes)
        .ok_or_else(|| IoError::ReadFailed("segment byte capacity overflow".into()))?;
    let end = segment_offset
        .checked_add(u64::from(length))
        .ok_or_else(|| IoError::ReadFailed("segment-relative read end overflow".into()))?;
    if end > segment_bytes {
        return Err(IoError::ReadFailed(format!(
            "segment-relative read end {end} exceeds segment capacity {segment_bytes}"
        )));
    }
    Ok(())
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

/// Production `DiskWriter` — wraps `DiskioClient` + `RpcServer` +
/// `Connection`. Each durable `write` sends the RPC and awaits the response.
pub struct DiskioBlockWriter {
    client: Arc<DiskioClient>,
    server: Arc<RpcServer>,
    conn: Connection,
}

impl DiskioBlockWriter {
    /// Construct a new writer. The client must be attached to the
    /// connection before use.
    #[must_use]
    pub fn new(client: Arc<DiskioClient>, server: Arc<RpcServer>, conn: Connection) -> Self {
        Self { client, server, conn }
    }
}

#[async_trait]
impl DiskWriter for DiskioBlockWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        let disk_id = seg
            .disk_id
            .as_ref()
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let disk_id = DiskId::new(disk_id.high, disk_id.low);
        let zone_offset = seg.unit_offset * unit_bytes;
        let fut = self
            .client
            .write_bytes(
                &self.server,
                &self.conn,
                disk_id,
                seg.zone_index,
                zone_offset,
                data,
            )
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        let code = DiskioClient::await_write_response(fut)
            .await
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!("disk write returned {code:?}")));
        }
        Ok(())
    }

    async fn write_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        validate_segment_byte_write(seg, unit_bytes, byte_offset, data.len())?;
        let disk_id = seg
            .disk_id
            .as_ref()
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let disk_id = DiskId::new(disk_id.high, disk_id.low);
        let zone_offset = seg
            .unit_offset
            .checked_mul(unit_bytes)
            .and_then(|offset| offset.checked_add(byte_offset))
            .ok_or_else(|| IoError::WriteFailed("disk write offset overflow".into()))?;
        let fut = self
            .client
            .write_bytes(
                &self.server,
                &self.conn,
                disk_id,
                seg.zone_index,
                zone_offset,
                data,
            )
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        let code = DiskioClient::await_write_response(fut)
            .await
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!("disk write returned {code:?}")));
        }
        Ok(())
    }

    async fn read(&self, seg: &Segment, unit_bytes: u64, segment_offset: u64, length: u32) -> Result<Bytes> {
        validate_segment_read(seg, unit_bytes, segment_offset, length)?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        let disk_id = seg
            .disk_id
            .as_ref()
            .ok_or_else(|| IoError::ReadFailed("segment missing disk_id".into()))?;
        let disk_id = DiskId::new(disk_id.high, disk_id.low);
        let zone_offset = seg
            .unit_offset
            .checked_mul(unit_bytes)
            .and_then(|offset| offset.checked_add(segment_offset))
            .ok_or_else(|| IoError::ReadFailed("disk read offset overflow".into()))?;
        let future = self
            .client
            .read(
                &self.server,
                &self.conn,
                disk_id,
                seg.zone_index,
                zone_offset,
                length,
                0,
            )
            .map_err(|error| IoError::TransientRead(error.to_string()))?;
        let (code, data) = DiskioClient::await_read_response(future)
            .await
            .map_err(|error| IoError::TransientRead(error.to_string()))?;
        read_response(code, data, length)
    }
}

fn read_response(code: DiskIoRetCode, data: Option<Vec<u8>>, expected: u32) -> Result<Bytes> {
    if code != DiskIoRetCode::Success {
        let message = format!("disk read returned {code:?}");
        return Err(match code {
            DiskIoRetCode::DiskNotExist | DiskIoRetCode::ZoneNotExist | DiskIoRetCode::IoError => {
                IoError::ReadFailed(message)
            }
            DiskIoRetCode::Success => unreachable!(),
            DiskIoRetCode::PartialWrite
            | DiskIoRetCode::InvalidAlignment
            | DiskIoRetCode::ConnectionError => IoError::TransientRead(message),
        });
    }
    let data = data.ok_or_else(|| IoError::TransientRead("successful disk read omitted data".into()))?;
    if data.len() != expected as usize {
        return Err(IoError::TransientRead(format!(
            "disk read returned {} bytes, expected {expected}",
            data.len()
        )));
    }
    Ok(Bytes::from(data))
}
