// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskio_client::DiskId as DioDiskId;

fn xorshift64(mut state: u64) -> u64 {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

fn hash_seed(disk_id: DioDiskId) -> u64 {
    disk_id.high ^ disk_id.low.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Compute the deterministic pattern `NullDisk` returns for a read at
/// (`disk_id`, `phys_offset`, size). Mirrors the C++ `fill_pattern`.
pub fn null_disk_pattern(disk_id: DioDiskId, phys_offset: u64, size: usize) -> Vec<u8> {
    let mut state = hash_seed(disk_id);
    let skip = phys_offset / 8;
    for _ in 0..skip {
        state = xorshift64(state);
    }
    let mut buf = vec![0u8; size];
    let mut pos = 0;
    while pos < size {
        state = xorshift64(state);
        let val_bytes = state.to_le_bytes();
        let n = std::cmp::min(8, size - pos);
        buf[pos..pos + n].copy_from_slice(&val_bytes[..n]);
        pos += n;
    }
    buf
}
