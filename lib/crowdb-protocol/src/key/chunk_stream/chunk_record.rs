// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::chunk_stream::StreamName;
use crate::key::encoding::{check_exact, decode_header, decode_u64, encode_header, encode_u64};
use crate::key::{BinaryKey, KeyError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamChunkRecordOrder {
    Attached(u64),
    Candidate(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamChunkRecordKey {
    pub stream_id: StreamName,
    pub order: StreamChunkRecordOrder,
}

impl StreamChunkRecordKey {
    #[must_use]
    pub fn stream_prefix(id: StreamName) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(19);
        encode_header(&mut bytes, Self::TYPE_TAG);
        encode_u64(&mut bytes, id.high);
        encode_u64(&mut bytes, id.low);
        bytes
    }

    #[must_use]
    pub fn attached_prefix(id: StreamName) -> Vec<u8> {
        let mut bytes = Self::stream_prefix(id);
        bytes.push(0);
        bytes
    }

    #[must_use]
    pub fn candidate_prefix(id: StreamName) -> Vec<u8> {
        let mut bytes = Self::stream_prefix(id);
        bytes.push(1);
        bytes
    }
}

impl BinaryKey for StreamChunkRecordKey {
    const TYPE_TAG: u16 = 0x001c;
    fn encode_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend(Self::stream_prefix(self.stream_id));
        let (kind, value) = match self.order {
            StreamChunkRecordOrder::Attached(start) => (0, start),
            StreamChunkRecordOrder::Candidate(sequence) => (1, sequence),
        };
        bytes.push(kind);
        encode_u64(bytes, value);
    }

    fn decode(bytes: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(bytes, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        let kind = *fields.get(offset).ok_or(KeyError::ShortInput)?;
        let (value, offset) = decode_u64(fields, offset + 1)?;
        check_exact(fields, offset)?;
        let order = match kind {
            0 => StreamChunkRecordOrder::Attached(value),
            1 => StreamChunkRecordOrder::Candidate(value),
            _ => return Err(KeyError::BadTag),
        };
        Ok(Self {
            stream_id: StreamName { high, low },
            order,
        })
    }
}
