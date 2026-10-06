// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned bitmap-map keys, separate from legacy chunkdb range records.

use super::encoding::{
    check_path_exact, decode_path_u64, encode_path_header, encode_path_u64, KeyError, TextKey,
};
use crate::chunk_slot::{ChunkSlot, ChunkSlotAuthority};

/// Bitmap for one incarnation and per-slot epoch, separate from endpoint routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkServiceAuthorityKey {
    pub authority: ChunkSlotAuthority,
}

impl TextKey for ChunkServiceAuthorityKey {
    const PATH_MAGIC: &'static str = "/chunkdb";
    const PATH_TYPE: &'static str = "slot_authority";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        out.push('/');
        for byte in self.authority.to_fence_value() {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 1)?;
        let text = parts[0].as_bytes();
        if text.len() != 66 {
            return Err(KeyError::BadTag);
        }
        let mut value = [0; 33];
        for (index, pair) in text.chunks_exact(2).enumerate() {
            let digit = |byte| match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => Err(KeyError::BadTag),
            };
            value[index] = digit(pair[0])? * 16 + digit(pair[1])?;
        }
        Ok(Self {
            authority: ChunkSlotAuthority::from_fence_value(&value).map_err(|_| KeyError::BadTag)?,
        })
    }
}

/// One active service handoff cohort in group zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkServiceHandoffKey;

impl TextKey for ChunkServiceHandoffKey {
    const PATH_MAGIC: &'static str = "/chunkdb";
    const PATH_TYPE: &'static str = "slot_handoff";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        out.push_str("/service");
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 1)?;
        if parts[0] == "service" {
            Ok(Self)
        } else {
            Err(KeyError::BadTag)
        }
    }
}

/// One persistent execution fence per slot in the selected data KV group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkSlotFenceKey {
    pub slot: ChunkSlot,
}

impl TextKey for ChunkSlotFenceKey {
    const PATH_MAGIC: &'static str = "/chunkdb";
    const PATH_TYPE: &'static str = "ownership-fence";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, u64::from(self.slot.value()));
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 1)?;
        let text = parts[0];
        if text.is_empty()
            || (text.len() > 1 && text.starts_with('0'))
            || !text.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(KeyError::BadTag);
        }
        let value = text.parse::<u16>().map_err(|_| KeyError::BadTag)?;
        Ok(Self {
            slot: ChunkSlot::try_from(value).map_err(|_| KeyError::BadTag)?,
        })
    }
}

/// One record per service instance, not per slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkServiceSlotsKey {
    pub instance_id: u64,
}

impl TextKey for ChunkServiceSlotsKey {
    const PATH_MAGIC: &'static str = "/chunkdb";
    const PATH_TYPE: &'static str = "slot_service";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.instance_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 1)?;
        Ok(Self {
            instance_id: decode_path_u64(parts[0])?,
        })
    }
}

/// One record per eligible storage group, not per slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkStorageSlotsKey {
    pub store_id: u64,
    pub group_id: u64,
}

impl TextKey for ChunkStorageSlotsKey {
    const PATH_MAGIC: &'static str = "/chunkdb";
    const PATH_TYPE: &'static str = "slot_storage";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.store_id);
        encode_path_u64(out, self.group_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 2)?;
        Ok(Self {
            store_id: decode_path_u64(parts[0])?,
            group_id: decode_path_u64(parts[1])?,
        })
    }
}

/// Heads are outside owner scan prefixes; each has its own CAS revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkSlotMapHeadKey {
    Service,
    Storage,
    Authority,
}

impl TextKey for ChunkSlotMapHeadKey {
    const PATH_MAGIC: &'static str = "/chunkdb";
    const PATH_TYPE: &'static str = "slot_head";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        out.push('/');
        out.push_str(match self {
            Self::Service => "service",
            Self::Storage => "storage",
            Self::Authority => "authority",
        });
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 1)?;
        match parts[0] {
            "service" => Ok(Self::Service),
            "storage" => Ok(Self::Storage),
            "authority" => Ok(Self::Authority),
            _ => Err(KeyError::ShortInput),
        }
    }
}
