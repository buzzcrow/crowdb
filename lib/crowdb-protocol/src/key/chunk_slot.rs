// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned bitmap-map keys, separate from legacy chunkdb range records.

use super::encoding::{
    check_path_exact, decode_path_u64, encode_path_header, encode_path_u64, KeyError, TextKey,
};

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
        });
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 1)?;
        match parts[0] {
            "service" => Ok(Self::Service),
            "storage" => Ok(Self::Storage),
            _ => Err(KeyError::ShortInput),
        }
    }
}
