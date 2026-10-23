// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::key::encoding::{check_exact, decode_header, decode_u64, encode_header, encode_u64};
use crate::key::{BinaryKey, KeyError};
use crate::{chunk_stream::StreamName, common::ChunkId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamSealedChunkSpanKey {
    pub stream_name: StreamName,
    pub chunk_id: ChunkId,
}

impl StreamSealedChunkSpanKey {
    #[must_use]
    pub fn stream_prefix(name: StreamName) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(19);
        encode_header(&mut bytes, Self::TYPE_TAG);
        encode_u64(&mut bytes, name.high);
        encode_u64(&mut bytes, name.low);
        bytes
    }
}

impl BinaryKey for StreamSealedChunkSpanKey {
    const TYPE_TAG: u16 = 0x001a;

    fn encode_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend(Self::stream_prefix(self.stream_name));
        encode_u64(bytes, self.chunk_id.high);
        encode_u64(bytes, self.chunk_id.low);
    }

    fn decode(bytes: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(bytes, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        let (chunk_high, offset) = decode_u64(fields, offset)?;
        let (chunk_low, offset) = decode_u64(fields, offset)?;
        check_exact(fields, offset)?;
        Ok(Self {
            stream_name: StreamName { high, low },
            chunk_id: ChunkId {
                high: chunk_high,
                low: chunk_low,
            },
        })
    }
}
