// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_stream::{DirectoryIndex, DirectoryMutation};
use crowdb_protocol::chunk_stream::{StreamDirectoryHead, StreamExtentLocation, StreamName, StreamPurpose};
use crowdb_protocol::frame::ChunkLocation;
use crowdb_protocol::{chunkdb::rpc::ChunkType, common::ChunkId};

pub struct TestIndexedMetadata;

impl TestIndexedMetadata {
    pub const NAME: StreamName = StreamName { high: 29, low: 11 };
    pub fn empty() -> DirectoryIndex {
        DirectoryIndex::new(Self::NAME, StreamPurpose::Stream, None).unwrap()
    }
    pub fn locations(start: u64, count: u64) -> Vec<StreamExtentLocation> {
        (start..start + count)
            .map(|offset| StreamExtentLocation {
                logical_start: offset,
                location: ChunkLocation {
                    chunk_id: ChunkId {
                        high: (ChunkType::Stream as u64) << 56,
                        low: offset + 1,
                    },
                    frame_offset: 0,
                    logical_length: 1,
                },
            })
            .collect()
    }
    pub fn head(mutation: &DirectoryMutation, base: Option<&StreamDirectoryHead>) -> StreamDirectoryHead {
        StreamDirectoryHead {
            stream_name: Self::NAME,
            purpose: StreamPurpose::Stream,
            metadata_group_id: 1,
            writer_epoch: base.map_or(1, |base| base.writer_epoch),
            owner_session: [1; 16],
            generation: base.map_or(1, |base| base.generation + 1),
            trim_offset: 0,
            published_tail: mutation.index().root().map_or(0, |root| root.end_logical),
            root: mutation.index().root(),
            active: None,
            previous: base.map(StreamDirectoryHead::identity),
            closed: false,
        }
    }
}
