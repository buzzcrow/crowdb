// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded strip repair images assembled directly from immutable frame views.

use crate::storage::MirrorStripImage;
use crate::{Result, StreamError};
use bytes::{Bytes, BytesMut};
use crowdb_chunk_client::STREAM_STRIP_BYTES;
use crowdb_protocol::common::ChunkId;

struct ShadowStrip {
    start: u64,
    bytes: BytesMut,
}

struct PendingShadow {
    chunk_id: ChunkId,
    complete_end: u64,
    strips: Vec<(u64, Bytes)>,
}

#[derive(Default)]
pub(crate) struct MirrorShadow {
    chunk_id: Option<ChunkId>,
    complete_end: u64,
    retained: Vec<ShadowStrip>,
    pending: Option<PendingShadow>,
}

impl MirrorShadow {
    pub(crate) fn stage(
        &mut self,
        chunk_id: ChunkId,
        physical_offset: u64,
        complete_end: u64,
        views: &[Bytes],
    ) -> Result<Vec<MirrorStripImage>> {
        if self.pending.is_some() {
            return Err(StreamError::Internal(
                "mirror shadow has an unfinished write".into(),
            ));
        }
        let length = views
            .iter()
            .try_fold(0_u64, |sum, view| sum.checked_add(view.len() as u64))
            .ok_or_else(|| StreamError::InvalidRequest("stream write length overflows".into()))?;
        let end = physical_offset
            .checked_add(length)
            .ok_or_else(|| StreamError::InvalidRequest("stream write offset overflows".into()))?;
        if length == 0
            || complete_end > end
            || complete_end <= physical_offset
            || end - complete_end > crate::storage::EMPTY_TAIL.len() as u64
            || (self.chunk_id == Some(chunk_id) && self.complete_end != physical_offset)
        {
            return Err(StreamError::Corruption(
                "mirror views differ from complete append bounds".into(),
            ));
        }
        let mut view_index = 0;
        let mut view_offset = 0;
        let mut cursor = physical_offset;
        let mut images = Vec::new();
        while cursor < end {
            let strip_start = cursor / STREAM_STRIP_BYTES * STREAM_STRIP_BYTES;
            let block_offset = usize::try_from(cursor - strip_start).expect("bounded strip offset");
            let count = usize::try_from((end - cursor).min(STREAM_STRIP_BYTES - block_offset as u64))
                .expect("bounded strip image");
            let mut buffer = self.take_prefix(chunk_id, strip_start, block_offset)?;
            let mut remaining = count;
            while remaining > 0 {
                let view = views
                    .get(view_index)
                    .ok_or_else(|| StreamError::Corruption("frame view chain ended early".into()))?;
                let take = remaining.min(view.len() - view_offset);
                buffer.extend_from_slice(&view[view_offset..view_offset + take]);
                remaining -= take;
                view_offset += take;
                if view_offset == view.len() {
                    view_index += 1;
                    view_offset = 0;
                }
            }
            let full_image = buffer.freeze();
            images.push(MirrorStripImage {
                block_offset: block_offset as u64,
                data: full_image.slice(block_offset..),
                full_image,
            });
            cursor += count as u64;
        }
        let strips = images
            .iter()
            .enumerate()
            .filter_map(|(index, image)| {
                let start = physical_offset / STREAM_STRIP_BYTES * STREAM_STRIP_BYTES
                    + index as u64 * STREAM_STRIP_BYTES;
                (start + STREAM_STRIP_BYTES > complete_end).then(|| (start, image.full_image.clone()))
            })
            .collect();
        self.pending = Some(PendingShadow {
            chunk_id,
            complete_end,
            strips,
        });
        Ok(images)
    }

    fn take_prefix(&mut self, chunk_id: ChunkId, start: u64, offset: usize) -> Result<BytesMut> {
        let buffer = if self.chunk_id == Some(chunk_id) {
            self.retained
                .iter()
                .position(|strip| strip.start == start)
                .map(|index| self.retained.remove(index).bytes)
        } else {
            None
        };
        let mut buffer = match buffer {
            Some(buffer) => buffer,
            None if offset == 0 => {
                BytesMut::with_capacity(usize::try_from(STREAM_STRIP_BYTES).expect("bounded strip"))
            }
            None => {
                return Err(StreamError::Corruption(
                    "new mirror strip starts after its beginning".into(),
                ))
            }
        };
        if buffer.len() < offset
            || buffer.len() - offset > crate::storage::EMPTY_TAIL.len()
            || buffer[offset..].iter().any(|byte| *byte != 0)
        {
            return Err(StreamError::Corruption(
                "mirror shadow does not match synchronized cursor".into(),
            ));
        }
        buffer.truncate(offset);
        Ok(buffer)
    }

    pub(crate) fn finish(&mut self) {
        if let Some(pending) = self.pending.take() {
            self.chunk_id = Some(pending.chunk_id);
            self.complete_end = pending.complete_end;
            self.retained = pending
                .strips
                .into_iter()
                .map(|(start, image)| ShadowStrip {
                    start,
                    bytes: image
                        .try_into_mut()
                        .unwrap_or_else(|shared| BytesMut::from(shared.as_ref())),
                })
                .collect();
        }
    }
}
