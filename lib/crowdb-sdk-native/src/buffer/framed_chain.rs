// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Logical frame offsets over independently retained producer allocations.

use bytes::Bytes;
use crowdb_access_s3::native_buffer::NativeFramedOwner;
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::{
    common::ChunkId,
    frame::{FrameError, FrameMagic, MAX_FRAME_BYTES},
};
use std::ops::Range;

pub(crate) struct FramedChain(pub Vec<NativeFramedOwner>);

impl FramedWriteBuffer for FramedChain {
    fn logical_len(&self) -> u64 {
        self.0.iter().map(FramedWriteBuffer::logical_len).sum()
    }

    fn frame_count(&self) -> usize {
        self.0.iter().map(FramedWriteBuffer::frame_count).sum()
    }

    fn frame_payload_len(&self, mut index: usize) -> Option<usize> {
        for owner in &self.0 {
            if index < owner.frame_count() {
                return owner.frame_payload_len(index);
            }
            index -= owner.frame_count();
        }
        None
    }

    fn prepare_frames(&mut self, magic: FrameMagic, write_time_ms: u64) -> Result<(), FrameError> {
        for owner in &mut self.0 {
            owner.prepare_frames(magic, write_time_ms)?;
        }
        Ok(())
    }

    fn finalize_frame(
        &mut self,
        mut index: usize,
        magic: FrameMagic,
        chunk_id: ChunkId,
        write_time_ms: u64,
    ) -> Result<Range<usize>, FrameError> {
        let mut offset = 0;
        for owner in &mut self.0 {
            if index < owner.frame_count() {
                let range = owner.finalize_frame(index, magic, chunk_id, write_time_ms)?;
                return Ok(offset + range.start..offset + range.end);
            }
            index -= owner.frame_count();
            offset += owner.frame_count() * MAX_FRAME_BYTES;
        }
        Err(FrameError::InvalidLocationRange)
    }

    fn views(&self, range: Range<usize>) -> Result<Vec<Bytes>, FrameError> {
        if range.is_empty() {
            return Err(FrameError::InvalidLocationRange);
        }
        let mut offset = 0;
        let mut covered = range.start;
        let mut views = Vec::new();
        for owner in &self.0 {
            let end = offset + owner.frame_count() * MAX_FRAME_BYTES;
            let start = range.start.max(offset);
            let stop = range.end.min(end);
            if start < stop {
                views.extend(owner.views(start - offset..stop - offset)?);
                covered = stop;
            }
            offset = end;
        }
        if covered != range.end {
            return Err(FrameError::InvalidLocationRange);
        }
        Ok(views)
    }
}
