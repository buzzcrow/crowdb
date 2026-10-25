// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn fresh_colocated_mirrors_require_disjoint_allocations_before_initialization() {
    for offset in [0, 255, 256] {
        let allocator = Arc::new(Allocator::new());
        allocator.colocated_offset.store(offset, Ordering::Release);
        let disks = Arc::new(Disks::default());
        let store =
            ProductionStreamChunkStore::new(allocator, disks.clone(), 30_000, ChunkReadPolicy::default())
                .unwrap();
        let result = store
            .allocate_mirrored(
                StreamName { high: 191, low: 193 },
                9,
                crowdb_chunk_stream::StreamPurpose::Stream,
            )
            .await;
        assert_eq!(
            result.is_ok(),
            offset == 256,
            "replica unit offset {offset}: {result:?}"
        );
        assert_eq!(
            disks.fsyncs.load(Ordering::Relaxed),
            if offset == 256 { 2 } else { 0 }
        );
    }
}
