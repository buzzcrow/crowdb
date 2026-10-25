// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{
    encode_append_group_views, FramePrefixScanner, FrameScanConfig, FrameScanError, FrameScanMode,
    FrameScanStop, ScannedFrameExtent,
};

pub struct TestFramePrefix;

impl TestFramePrefix {
    pub const CHUNK: ChunkId = ChunkId {
        high: 4 << 56,
        low: 17,
    };
    pub fn config(limit: usize) -> FrameScanConfig {
        FrameScanConfig {
            chunk_id: Self::CHUNK,
            mode: FrameScanMode::Grouped,
            physical_start: 0,
            physical_limit: limit as u64,
            required_end: 0,
            logical_start: 100,
            max_append_bytes: 64 * 1024 * 1024,
        }
    }
    pub fn group(offset: usize, length: usize) -> Vec<u8> {
        encode_append_group_views(Self::CHUNK, offset as u64, &Bytes::from(vec![0x5a; length]), 1)
            .unwrap()
            .concat()
    }
    pub fn feed(
        scanner: &mut FramePrefixScanner,
        bytes: &[u8],
        fragment: usize,
        max_extents: usize,
    ) -> Result<Vec<ScannedFrameExtent>, FrameScanError> {
        let mut offset = 0;
        let mut result = Vec::new();
        while offset < bytes.len() && scanner.stop() == FrameScanStop::Running {
            let end = (offset + fragment).min(bytes.len());
            let batch = scanner.push(offset as u64, &bytes[offset..end], max_extents)?;
            assert!(batch.extents.len() <= max_extents);
            assert!(batch.consumed > 0 || scanner.stop() != FrameScanStop::Running);
            offset += batch.consumed;
            result.extend(batch.extents);
        }
        Ok(result)
    }
}
