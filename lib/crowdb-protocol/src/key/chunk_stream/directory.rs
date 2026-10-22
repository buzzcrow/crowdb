// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::chunk_stream::{StreamDirectoryPageId, StreamName, StreamRootIdentity};
use crate::key::encoding::{check_exact, decode_header, decode_u64, encode_header, encode_u64};
use crate::key::{BinaryKey, KeyError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamDirectoryPageKey {
    pub stream_name: StreamName,
    pub page_id: StreamDirectoryPageId,
}

impl StreamDirectoryPageKey {
    #[must_use]
    pub fn stream_prefix(stream_name: StreamName) -> Vec<u8> {
        let mut out = Vec::with_capacity(19);
        encode_header(&mut out, Self::TYPE_TAG);
        encode_u64(&mut out, stream_name.high);
        encode_u64(&mut out, stream_name.low);
        out
    }
}

impl BinaryKey for StreamDirectoryPageKey {
    const TYPE_TAG: u16 = 0x0016;

    fn encode_to(&self, out: &mut Vec<u8>) {
        out.extend(Self::stream_prefix(self.stream_name));
        out.extend_from_slice(&self.page_id.0);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        let id = fields.get(offset..offset + 32).ok_or(KeyError::ShortInput)?;
        let page_id = StreamDirectoryPageId(id.try_into().map_err(|_| KeyError::ShortInput)?);
        check_exact(fields, offset + 32)?;
        Ok(Self {
            stream_name: StreamName { high, low },
            page_id,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamDirectoryRootKey {
    pub stream_name: StreamName,
    pub identity: StreamRootIdentity,
}

impl StreamDirectoryRootKey {
    #[must_use]
    pub fn stream_prefix(stream_name: StreamName) -> Vec<u8> {
        let mut out = Vec::with_capacity(19);
        encode_header(&mut out, Self::TYPE_TAG);
        encode_u64(&mut out, stream_name.high);
        encode_u64(&mut out, stream_name.low);
        out
    }
}

impl BinaryKey for StreamDirectoryRootKey {
    const TYPE_TAG: u16 = 0x0017;

    fn encode_to(&self, out: &mut Vec<u8>) {
        out.extend(Self::stream_prefix(self.stream_name));
        encode_u64(out, self.identity.writer_epoch);
        encode_u64(out, self.identity.generation);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        let (writer_epoch, offset) = decode_u64(fields, offset)?;
        let (generation, offset) = decode_u64(fields, offset)?;
        check_exact(fields, offset)?;
        Ok(Self {
            stream_name: StreamName { high, low },
            identity: StreamRootIdentity {
                writer_epoch,
                generation,
            },
        })
    }
}
