// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crowdb_protocol::{chunkdb::rpc::Strip, frame::MAX_FRAME_SCAN_INPUT_BYTES};
use std::sync::Arc;
use tokio::time::{timeout_at, Instant};

use super::MirroredLayout;
use crate::{FrameWindowReader, IoError, Result};

pub(super) struct MirroredWindowReader {
    pub(super) layout: Arc<MirroredLayout>,
    pub(super) replica: usize,
}

#[async_trait]
impl FrameWindowReader for MirroredWindowReader {
    async fn read_window(&self, offset: u64, length: u32) -> Result<Bytes> {
        self.check_deadline()?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or_else(|| invalid("physical range overflows"))?;
        if length == 0 || length as usize > MAX_FRAME_SCAN_INPUT_BYTES || end > self.layout.capacity {
            return Err(invalid("physical window exceeds its allocated scope"));
        }
        let mut index = self
            .layout
            .chunk
            .strips
            .partition_point(|strip| u64::from(strip.chunk_offset) * 1024 <= offset)
            .checked_sub(1)
            .ok_or_else(|| invalid("window precedes allocated strips"))?;
        let mut cursor = offset;
        let mut output: Option<BytesMut> = None;
        while cursor < end {
            self.check_deadline()?;
            let strip = self
                .layout
                .chunk
                .strips
                .get(index)
                .ok_or_else(|| invalid("window exceeds allocated strips"))?;
            let start = u64::from(strip.chunk_offset) * 1024;
            let strip_end = start + u64::from(strip.capacity) * 1024;
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return Err(invalid("window has no mirror layout"));
            };
            let segment = mirror
                .segments
                .get(self.replica)
                .ok_or_else(|| invalid("replica is absent from an allocated strip"))?;
            if strip.unavailable_segments.contains(segment) {
                return Err(invalid("replica is marked unavailable in this layout"));
            }
            let take = u32::try_from((end.min(strip_end)) - cursor).expect("bounded window");
            let data = timeout_at(
                self.layout.deadline,
                self.layout
                    .disk_io
                    .read(segment, u64::from(strip.unit_kb) * 1024, cursor - start, take),
            )
            .await
            .map_err(|_| invalid("physical layout read deadline expired"))??;
            self.check_deadline()?;
            if data.len() != take as usize {
                return Err(invalid("short physical segment read"));
            }
            if cursor == offset && take == length {
                return Ok(data);
            }
            output
                .get_or_insert_with(|| BytesMut::with_capacity(length as usize))
                .extend_from_slice(&data);
            cursor += u64::from(take);
            index += 1;
        }
        Ok(output.expect("cross-strip window has data").freeze())
    }
}

impl MirroredWindowReader {
    fn check_deadline(&self) -> Result<()> {
        if Instant::now() >= self.layout.deadline {
            Err(invalid("physical layout read deadline expired"))
        } else {
            Ok(())
        }
    }
}

fn invalid(message: &str) -> IoError {
    IoError::ReadFailed(message.into())
}
