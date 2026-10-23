// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::chunk_stream::{StreamName, StreamRootPinId};
use crate::key::encoding::{check_exact, decode_header, decode_u64, encode_header, encode_u64};
use crate::key::{BinaryKey, KeyError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamRootPinKey {
    pub stream_name: StreamName,
    pub pin_id: StreamRootPinId,
}

impl StreamRootPinKey {
    #[must_use]
    pub fn stream_prefix(name: StreamName) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(19);
        encode_header(&mut bytes, Self::TYPE_TAG);
        encode_u64(&mut bytes, name.high);
        encode_u64(&mut bytes, name.low);
        bytes
    }
}

impl BinaryKey for StreamRootPinKey {
    const TYPE_TAG: u16 = 0x0019;

    fn encode_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend(Self::stream_prefix(self.stream_name));
        bytes.extend_from_slice(&self.pin_id.0);
    }

    fn decode(bytes: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(bytes, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        let id = fields.get(offset..offset + 16).ok_or(KeyError::ShortInput)?;
        check_exact(fields, offset + 16)?;
        Ok(Self {
            stream_name: StreamName { high, low },
            pin_id: StreamRootPinId(id.try_into().map_err(|_| KeyError::ShortInput)?),
        })
    }
}
