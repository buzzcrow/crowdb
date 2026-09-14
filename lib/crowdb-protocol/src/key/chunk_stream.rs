// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 binding and metadata-group keys for chunk streams.

use std::fmt::Write;

use crate::chunk_stream::StreamName;

use super::encoding::{
    check_exact, check_path_exact, decode_header, decode_u64, encode_header, encode_path_header, encode_u64,
    BinaryKey, KeyError, TextKey,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamBindingKey {
    pub stream_name: StreamName,
}

impl TextKey for StreamBindingKey {
    const PATH_MAGIC: &'static str = "/stream";
    const PATH_TYPE: &'static str = "binding";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        out.push('/');
        let _ = write!(out, "{:016x}{:016x}", self.stream_name.high, self.stream_name.low);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 1)?;
        let encoded = parts.first().ok_or(KeyError::ShortInput)?;
        if encoded.len() != 32 {
            return Err(KeyError::ShortInput);
        }
        let high = u64::from_str_radix(&encoded[..16], 16).map_err(|_| KeyError::ShortInput)?;
        let low = u64::from_str_radix(&encoded[16..], 16).map_err(|_| KeyError::ShortInput)?;
        Ok(Self {
            stream_name: StreamName { high, low },
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamManifestKey {
    pub stream_name: StreamName,
    pub writer_epoch: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamManifestHeadKey {
    pub stream_name: StreamName,
}

impl BinaryKey for StreamManifestHeadKey {
    const TYPE_TAG: u16 = 0x0012;

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

impl BinaryKey for StreamManifestKey {
    const TYPE_TAG: u16 = 0x0010;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.stream_name.high);
        encode_u64(out, self.stream_name.low);
        encode_u64(out, self.writer_epoch);
        encode_u64(out, self.generation);
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
            writer_epoch,
            generation,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamExtentPageKey {
    pub stream_name: StreamName,
    pub writer_epoch: u64,
    pub generation: u64,
    pub page_index: u64,
}

impl StreamExtentPageKey {
    /// Encodes the exact key prefix shared by all extent pages for one stream.
    #[must_use]
    pub fn stream_prefix(stream_name: StreamName) -> Vec<u8> {
        let mut out = Vec::with_capacity(19);
        encode_header(&mut out, Self::TYPE_TAG);
        encode_u64(&mut out, stream_name.high);
        encode_u64(&mut out, stream_name.low);
        out
    }
}

impl BinaryKey for StreamExtentPageKey {
    const TYPE_TAG: u16 = 0x0011;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.stream_name.high);
        encode_u64(out, self.stream_name.low);
        encode_u64(out, self.writer_epoch);
        encode_u64(out, self.generation);
        encode_u64(out, self.page_index);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (high, offset) = decode_u64(fields, 0)?;
        let (low, offset) = decode_u64(fields, offset)?;
        let (writer_epoch, offset) = decode_u64(fields, offset)?;
        let (generation, offset) = decode_u64(fields, offset)?;
        let (page_index, offset) = decode_u64(fields, offset)?;
        check_exact(fields, offset)?;
        Ok(Self {
            stream_name: StreamName { high, low },
            writer_epoch,
            generation,
            page_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_keys_round_trip() {
        let name = StreamName { high: 7, low: 9 };
        let binding = StreamBindingKey { stream_name: name };
        assert_eq!(StreamBindingKey::from_path(&binding.to_path()).unwrap(), binding);

        let manifest = StreamManifestKey {
            stream_name: name,
            writer_epoch: 11,
            generation: 13,
        };
        assert_eq!(
            StreamManifestKey::from_bytes(&manifest.to_bytes()).unwrap(),
            manifest
        );

        let head = StreamManifestHeadKey { stream_name: name };
        assert_eq!(StreamManifestHeadKey::from_bytes(&head.to_bytes()).unwrap(), head);

        let extent = StreamExtentPageKey {
            stream_name: name,
            writer_epoch: 11,
            generation: 13,
            page_index: 17,
        };
        assert_eq!(
            StreamExtentPageKey::from_bytes(&extent.to_bytes()).unwrap(),
            extent
        );
    }
}
