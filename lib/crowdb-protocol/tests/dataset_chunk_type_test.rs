// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_protocol::chunkdb_fb::FBChunkType;

#[test]
fn dataset_chunk_tag_preserves_existing_allocation_types() {
    for (tag, kind) in [
        (1, ChunkType::Wal),
        (2, ChunkType::BtreePage),
        (3, ChunkType::PageIndex),
        (4, ChunkType::Stream),
        (5, ChunkType::S3),
        (6, ChunkType::IcebergTable),
        (7, ChunkType::Dataset),
    ] {
        assert_eq!(kind as i32, tag);
        assert_eq!(ChunkType::try_from(tag).unwrap(), kind);
    }
    assert_eq!(FBChunkType::Dataset.0, 7);
    assert!(ChunkType::try_from(0).is_err());
    assert!(ChunkType::try_from(8).is_err());
}
