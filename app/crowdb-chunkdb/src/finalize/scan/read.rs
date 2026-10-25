// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crowdb_chunk_client::{FrameWindowReader, IoError, Result};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, Strip};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::MAX_FRAME_SCAN_INPUT_BYTES;

use crate::conversion::io::ConversionDiskIo;

pub(super) struct FinalizerWindowReader {
    io: Arc<ConversionDiskIo>,
    chunk: Arc<Chunk>,
    replica: usize,
}

impl FinalizerWindowReader {
    pub(super) fn new(io: Arc<ConversionDiskIo>, chunk: Arc<Chunk>, replica: usize) -> Self {
        Self { io, chunk, replica }
    }

    async fn read_part(&self, strip: &ChunkStrip, offset: u64, length: u32) -> Result<Bytes> {
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let (segment, local, count) = match &strip.strip {
            Some(Strip::MirrorStrip(mirror)) => (
                mirror
                    .segments
                    .get(self.replica)
                    .ok_or_else(|| failed("mirror replica absent"))?,
                offset,
                length,
            ),
            Some(Strip::EcStrip(ec)) => {
                let shard_bytes = ec
                    .segments
                    .first()
                    .map_or(0, |segment| u64::from(segment.unit_count) * unit_bytes);
                if shard_bytes == 0 {
                    return Err(failed("empty EC shard"));
                }
                let shard = usize::try_from(offset / shard_bytes).map_err(|_| failed("EC shard overflow"))?;
                if shard >= ec.data_num as usize {
                    return Err(failed("EC range exceeds data shards"));
                }
                let local = offset % shard_bytes;
                (
                    &ec.segments[shard],
                    local,
                    u32::try_from(u64::from(length).min(shard_bytes - local)).expect("bounded window"),
                )
            }
            None => return Err(failed("missing physical layout")),
        };
        self.read_segment(strip, segment, unit_bytes, local, count).await
    }

    async fn read_segment(
        &self,
        strip: &ChunkStrip,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        count: u32,
    ) -> Result<Bytes> {
        if strip.unavailable_segments.contains(segment) {
            return Err(failed("physical segment is unavailable"));
        }
        let bytes = self
            .io
            .read_segment_range(segment, unit_bytes, offset, count)
            .await
            .map_err(|error| failed(&error.to_string()))?;
        if bytes.len() != count as usize {
            return Err(failed("short physical segment read"));
        }
        Ok(bytes)
    }
}

#[async_trait]
impl FrameWindowReader for FinalizerWindowReader {
    async fn read_window(&self, offset: u64, length: u32) -> Result<Bytes> {
        let end = offset
            .checked_add(u64::from(length))
            .ok_or_else(|| failed("window overflows"))?;
        if length == 0
            || length as usize > MAX_FRAME_SCAN_INPUT_BYTES
            || end > u64::from(self.chunk.capacity) * 1024
        {
            return Err(failed("physical window exceeds layout or budget"));
        }
        let mut output = BytesMut::with_capacity(length as usize);
        let mut cursor = offset;
        while cursor < end {
            let index = self
                .chunk
                .strips
                .partition_point(|strip| u64::from(strip.chunk_offset) * 1024 <= cursor)
                .checked_sub(1)
                .ok_or_else(|| failed("window precedes layout"))?;
            let strip = &self.chunk.strips[index];
            let start = u64::from(strip.chunk_offset) * 1024;
            let strip_end = start + u64::from(strip.capacity) * 1024;
            if cursor >= strip_end {
                return Err(failed("window has a layout gap"));
            }
            let count = u32::try_from((end.min(strip_end)) - cursor).expect("bounded window");
            let bytes = self.read_part(strip, cursor - start, count).await?;
            cursor += bytes.len() as u64;
            output.extend_from_slice(&bytes);
        }
        Ok(output.freeze())
    }
}

fn failed(message: &str) -> IoError {
    IoError::ReadFailed(message.into())
}
