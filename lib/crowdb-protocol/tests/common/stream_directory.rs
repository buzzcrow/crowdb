// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    StreamDirectoryHead, StreamDirectoryLeaf, StreamDirectoryPage, StreamDirectoryRef, StreamExtentLocation,
    StreamName, StreamPurpose,
};
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::ChunkLocation;

pub struct TestDirectory;

impl TestDirectory {
    pub const NAME: StreamName = StreamName { high: 7, low: 9 };
    pub const CHUNK: ChunkId = ChunkId {
        high: (ChunkType::Stream as u64) << 56,
        low: 11,
    };

    pub fn leaf(start: u64, physical: u64, length: u64) -> StreamDirectoryPage {
        StreamDirectoryPage::Leaf(StreamDirectoryLeaf {
            stream_name: Self::NAME,
            purpose: StreamPurpose::Stream,
            locations: vec![StreamExtentLocation {
                logical_start: start,
                location: ChunkLocation {
                    chunk_id: Self::CHUNK,
                    frame_offset: physical,
                    logical_length: length,
                },
            }],
        })
    }

    pub fn head(root: StreamDirectoryRef) -> StreamDirectoryHead {
        StreamDirectoryHead {
            stream_name: Self::NAME,
            purpose: StreamPurpose::Stream,
            metadata_group_id: 1,
            writer_epoch: 9,
            owner_session: [1; 16],
            generation: 1,
            trim_offset: root.first_logical,
            published_tail: root.end_logical,
            root: Some(root),
            active: None,
            previous: None,
            closed: false,
        }
    }
}
