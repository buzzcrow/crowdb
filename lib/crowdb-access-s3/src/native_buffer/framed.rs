// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One-time frame publication over immutable native payload storage.

use super::{NativeFramedOwner, NativePhysicalFrameView};
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{
    encode_frame_regions, prepare_frame_regions, set_frame_chunk_id, FrameError, FrameMagic,
    FRAME_FOOTER_BYTES, FRAME_V2_HEADER_BYTES, MAX_FRAME_BYTES,
};
use hyper::body::Bytes;
use std::ops::Range;
use std::sync::Arc;

impl FramedWriteBuffer for NativeFramedOwner {
    fn logical_len(&self) -> u64 {
        self.logical_len
    }

    fn frame_count(&self) -> usize {
        self.payload_lengths.len()
    }

    fn frame_payload_len(&self, index: usize) -> Option<usize> {
        self.payload_lengths.get(index).copied().map(usize::from)
    }

    fn prepare_frames(&mut self, magic: FrameMagic, write_time_ms: u64) -> Result<(), FrameError> {
        if !matches!(magic, FrameMagic::RepoSmallV2 | FrameMagic::RepoLargeV2) {
            return Err(FrameError::InvalidRole);
        }
        if self.magic != magic && self.finalized_chunks.iter().any(Option::is_some) {
            return Err(FrameError::InvalidLocationRange);
        }
        if self.magic != magic {
            self.prepared = false;
            self.prepared_slots = 0;
            self.magic = magic;
        }
        if self.prepared {
            return Ok(());
        }
        for index in self.prepared_slots..self.payload_lengths.len() {
            if self.finalized_chunks[index].is_some() {
                continue;
            }
            let payload_len = usize::from(self.payload_lengths[index]);
            let frame_offset = index
                .checked_mul(MAX_FRAME_BYTES)
                .ok_or(FrameError::LengthOverflow)?;
            let payload_offset = frame_offset + FRAME_V2_HEADER_BYTES;
            // SAFETY: socket receive has completed this slot. Header and
            // footer regions are disjoint from immutable payload views.
            unsafe {
                let header = std::slice::from_raw_parts_mut(
                    self.owner.pointer.as_ptr().add(frame_offset),
                    FRAME_V2_HEADER_BYTES,
                );
                let payload =
                    std::slice::from_raw_parts(self.owner.pointer.as_ptr().add(payload_offset), payload_len);
                let footer = std::slice::from_raw_parts_mut(
                    self.owner.pointer.as_ptr().add(payload_offset + payload_len),
                    FRAME_FOOTER_BYTES,
                );
                prepare_frame_regions(magic, payload, write_time_ms, header, footer)?;
            }
        }
        self.prepared = true;
        Ok(())
    }

    fn finalize_frame(
        &mut self,
        index: usize,
        magic: FrameMagic,
        chunk_id: ChunkId,
        write_time_ms: u64,
    ) -> Result<Range<usize>, FrameError> {
        if !matches!(magic, FrameMagic::RepoSmallV2 | FrameMagic::RepoLargeV2) {
            return Err(FrameError::InvalidRole);
        }
        let payload_len = self
            .frame_payload_len(index)
            .ok_or(FrameError::InvalidLocationRange)?;
        let frame_offset = index
            .checked_mul(MAX_FRAME_BYTES)
            .ok_or(FrameError::LengthOverflow)?;
        let payload_offset = frame_offset + FRAME_V2_HEADER_BYTES;
        let frame_len = FRAME_V2_HEADER_BYTES + payload_len + FRAME_FOOTER_BYTES;
        if let Some(previous) = self.finalized_chunks[index] {
            if previous == chunk_id && self.magic == magic {
                return Ok(frame_offset..frame_offset + frame_len);
            }
            return Err(FrameError::InvalidLocationRange);
        }
        if self.magic != magic {
            self.prepare_frames(magic, write_time_ms)?;
        }
        // SAFETY: the provider never exposes the reserved header/footer bytes.
        // This consumed slot is their only writer; payload views alias only
        // the disjoint initialized payload range.
        unsafe {
            let header = std::slice::from_raw_parts_mut(
                self.owner.pointer.as_ptr().add(frame_offset),
                FRAME_V2_HEADER_BYTES,
            );
            let payload =
                std::slice::from_raw_parts(self.owner.pointer.as_ptr().add(payload_offset), payload_len);
            let footer = std::slice::from_raw_parts_mut(
                self.owner.pointer.as_ptr().add(payload_offset + payload_len),
                FRAME_FOOTER_BYTES,
            );
            if self.prepared {
                set_frame_chunk_id(chunk_id, footer)?;
            } else {
                encode_frame_regions(magic, chunk_id, payload, write_time_ms, header, footer)?;
            }
        }
        self.finalized_chunks[index] = Some(chunk_id);
        Ok(frame_offset..frame_offset + frame_len)
    }

    fn views(&self, range: Range<usize>) -> Result<Vec<Bytes>, FrameError> {
        if range.start >= range.end || range.end > self.physical_len {
            return Err(FrameError::InvalidLocationRange);
        }
        let first = range.start / MAX_FRAME_BYTES;
        let last = (range.end - 1) / MAX_FRAME_BYTES;
        if self.finalized_chunks[first..=last].iter().any(Option::is_none) {
            return Err(FrameError::InvalidLocationRange);
        }
        Ok(vec![Bytes::from_owner(NativePhysicalFrameView {
            owner: Arc::clone(&self.owner),
            frame_offset: range.start,
            frame_len: range.len(),
        })])
    }
}
