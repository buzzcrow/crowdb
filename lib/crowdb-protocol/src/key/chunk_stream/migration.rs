// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::chunk_stream::{StreamLegacySourceId, StreamName};
use crate::key::encoding::{check_exact, decode_header, decode_u64, encode_header, encode_u64};
use crate::key::{BinaryKey, KeyError};

/// Exact retained head bytes, separate from independently retained indexed roots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamLegacySourceKey {
    pub stream_name: StreamName,
    pub source: StreamLegacySourceId,
}

impl StreamLegacySourceKey {
    #[must_use]
    pub fn stream_prefix(stream_name: StreamName) -> Vec<u8> {
        let mut out = Vec::with_capacity(19);
        encode_header(&mut out, Self::TYPE_TAG);
        encode_u64(&mut out, stream_name.high);
        encode_u64(&mut out, stream_name.low);
        out
    }
}

impl BinaryKey for StreamLegacySourceKey {
    const TYPE_TAG: u16 = 0x001c;

    fn encode_to(&self, out: &mut Vec<u8>) {
        out.extend(Self::stream_prefix(self.stream_name));
        out.extend_from_slice(&self.source.0);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        let source = fields.get(offset..offset + 32).ok_or(KeyError::ShortInput)?;
        check_exact(fields, offset + 32)?;
        Ok(Self {
            stream_name: StreamName { high, low },
            source: StreamLegacySourceId(source.try_into().map_err(|_| KeyError::ShortInput)?),
        })
    }
}

/// Bounded conversion state for one exact frozen source, fenced by the stable head.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamMigrationProgressKey {
    pub stream_name: StreamName,
    pub source: StreamLegacySourceId,
}

impl StreamMigrationProgressKey {
    #[must_use]
    pub fn stream_prefix(stream_name: StreamName) -> Vec<u8> {
        let mut out = Vec::with_capacity(19);
        encode_header(&mut out, Self::TYPE_TAG);
        encode_u64(&mut out, stream_name.high);
        encode_u64(&mut out, stream_name.low);
        out
    }
}

impl BinaryKey for StreamMigrationProgressKey {
    const TYPE_TAG: u16 = 0x001a;

    fn encode_to(&self, out: &mut Vec<u8>) {
        out.extend(Self::stream_prefix(self.stream_name));
        out.extend_from_slice(&self.source.0);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        let source = fields.get(offset..offset + 32).ok_or(KeyError::ShortInput)?;
        check_exact(fields, offset + 32)?;
        Ok(Self {
            stream_name: StreamName { high, low },
            source: StreamLegacySourceId(source.try_into().map_err(|_| KeyError::ShortInput)?),
        })
    }
}
