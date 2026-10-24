// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Stream identity, ordered chunk-record and owner keys.
use super::encoding::{
    check_exact, decode_header, decode_u64, encode_header, encode_u64, BinaryKey, KeyError,
};
use crate::chunk_stream::StreamName;
mod chunk_record;
mod ownership;
pub use chunk_record::{StreamChunkRecordKey, StreamChunkRecordOrder};
pub use ownership::{StreamProcessLeaseKey, StreamWriterOwnerKey};
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamRecordKey {
    pub stream_name: StreamName,
}
impl BinaryKey for StreamRecordKey {
    const TYPE_TAG: u16 = 0x001d;
    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.stream_name.high);
        encode_u64(out, self.stream_name.low);
    }
    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        check_exact(fields, offset)?;
        Ok(Self {
            stream_name: StreamName { high, low },
        })
    }
}
