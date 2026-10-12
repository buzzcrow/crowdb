// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

pub(super) fn key(index: u64) -> Vec<u8> {
    format!("native-load-{index:04}").into_bytes()
}

pub(super) fn value(index: u64) -> Vec<u8> {
    let mut state = index + 1;
    let mut bytes = Vec::with_capacity(64 * 1024);
    for _ in 0..8 * 1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes
}
