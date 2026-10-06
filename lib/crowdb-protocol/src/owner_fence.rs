// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reserved KV ownership namespaces shared by server and client validation.

use crate::chunk_slot::ChunkSlotAuthority;
use crate::key::{ChunkSlotFenceKey, TextKey};

const DISKDB_PREFIX: &[u8] = b"/diskdb/ownership-fence/";
const CHUNKDB_PREFIX: &[u8] = b"/chunkdb/ownership-fence/";

/// Reserve malformed keys too: normal writes cannot bypass handover admission.
#[must_use]
pub fn is_owner_fence_key(key: &[u8]) -> bool {
    key.starts_with(DISKDB_PREFIX) || key.starts_with(CHUNKDB_PREFIX)
}

/// Slot authority belongs to a nonzero data group, never group-0 control records.
#[must_use]
pub fn valid_owner_fence_scope(key: &[u8], group_id: u64) -> bool {
    !key.starts_with(CHUNKDB_PREFIX) || group_id != 0
}

/// Preserve the `DiskDB` contract; `ChunkDB` requires a canonical slot and identity.
#[must_use]
pub fn valid_owner_fence(key: &[u8], value: &[u8]) -> bool {
    if key.starts_with(DISKDB_PREFIX) {
        return !value.is_empty();
    }
    key.strip_prefix(CHUNKDB_PREFIX)
        .and_then(|suffix| std::str::from_utf8(suffix).ok())
        .and_then(|suffix| ChunkSlotFenceKey::decode_path(&[suffix]).ok())
        .is_some()
        && ChunkSlotAuthority::from_fence_value(value).is_ok()
}
