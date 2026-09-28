use bytes::{Bytes, BytesMut};
use crowdb_chunk_client::chunk::mirror_chunk_writer::STREAM_STRIP_BYTES;
use crowdb_protocol::common::ChunkId;

use crate::storage::MirrorStripImage;
use crate::{Result, StreamError};

#[derive(Default)]
pub(crate) struct MirrorShadow {
    chunk_id: Option<ChunkId>,
    strip_start: u64,
    retained: Option<BytesMut>,
    inflight: Option<Bytes>,
}

impl MirrorShadow {
    pub(crate) fn stage(
        &mut self,
        chunk_id: ChunkId,
        physical_offset: u64,
        data: &Bytes,
    ) -> Result<Vec<MirrorStripImage>> {
        if self.inflight.is_some() {
            return Err(StreamError::Internal(
                "mirror shadow has an unfinished write".into(),
            ));
        }
        let strip_bytes = usize::try_from(STREAM_STRIP_BYTES)
            .map_err(|_| StreamError::Internal("stream strip exceeds addressable memory".into()))?;
        let mut copied = 0_usize;
        let mut images = Vec::new();
        while copied < data.len() {
            let offset = physical_offset
                .checked_add(copied as u64)
                .ok_or_else(|| StreamError::InvalidRequest("stream write offset overflows".into()))?;
            let strip_start = offset / STREAM_STRIP_BYTES * STREAM_STRIP_BYTES;
            let block_offset = usize::try_from(offset - strip_start)
                .map_err(|_| StreamError::Internal("mirror block offset overflows".into()))?;
            let length = (strip_bytes - block_offset).min(data.len() - copied);
            let view = data.slice(copied..copied + length);
            let mut buffer = if self.chunk_id == Some(chunk_id) && self.strip_start == strip_start {
                self.retained.take().ok_or_else(|| {
                    StreamError::Corruption("active mirror strip has no retained prefix".into())
                })?
            } else {
                if block_offset != 0 {
                    return Err(StreamError::Corruption(
                        "new mirror strip starts after its beginning".into(),
                    ));
                }
                BytesMut::with_capacity(strip_bytes)
            };
            if buffer.len() != block_offset {
                return Err(StreamError::Corruption(
                    "mirror shadow does not match acknowledged cursor".into(),
                ));
            }
            buffer.extend_from_slice(&view);
            let full_image = buffer.freeze();
            self.chunk_id = Some(chunk_id);
            self.strip_start = strip_start;
            self.inflight = (block_offset + length < strip_bytes).then(|| full_image.clone());
            images.push(MirrorStripImage {
                block_offset: block_offset as u64,
                data: view,
                full_image,
            });
            copied += length;
        }
        Ok(images)
    }

    pub(crate) fn finish(&mut self) {
        self.retained = self.inflight.take().map(|image| {
            image
                .try_into_mut()
                .unwrap_or_else(|shared| BytesMut::from(shared.as_ref()))
        });
    }
}
