// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable indexed directory records, independent of head ownership/generation.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{StreamName, StreamPurpose};
use crate::common::ChunkId;
use crate::frame::{ChunkLocation, FrameLayout};

mod bounded;
mod codec;
mod publication;
mod validation;

pub use codec::{decode_stream_directory_page, encode_stream_directory_page, stream_directory_page_id};
pub use publication::{
    decode_stream_publication_guard, encode_stream_publication_guard, stream_publication_id,
    StreamDirectoryControl, StreamPublicationGuard, StreamPublicationId, MAX_STREAM_PUBLICATION_PAGES,
};

pub const MAX_STREAM_DIRECTORY_LOCATIONS: usize = 1024;
pub const MAX_STREAM_DIRECTORY_CHILDREN: usize = 128;
pub const MAX_STREAM_DIRECTORY_HEIGHT: u8 = 8;
pub const MAX_STREAM_DIRECTORY_RECORD_BYTES: usize = 64 * 1024;
pub const MAX_STREAM_CHUNK_PHYSICAL_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamDirectoryPageId(pub [u8; 32]);

/// A checked fence for one immutable page. Level zero identifies a leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDirectoryRef {
    pub id: StreamDirectoryPageId,
    pub level: u8,
    pub first_logical: u64,
    pub end_logical: u64,
    pub extent_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamFrameFormat {
    LegacyV1,
    GroupedV2,
}

impl StreamFrameFormat {
    #[must_use]
    pub const fn layout(self) -> FrameLayout {
        match self {
            Self::LegacyV1 => FrameLayout::Legacy,
            Self::GroupedV2 => FrameLayout::StreamGroupV2,
        }
    }
}

/// One complete append location; retained legacy short frames keep their boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamExtentLocation {
    pub logical_start: u64,
    pub location: ChunkLocation,
    pub format: StreamFrameFormat,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDirectoryLeaf {
    pub stream_name: StreamName,
    pub purpose: StreamPurpose,
    #[serde(deserialize_with = "bounded::locations")]
    pub locations: Vec<StreamExtentLocation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDirectoryBranch {
    pub stream_name: StreamName,
    pub purpose: StreamPurpose,
    pub level: u8,
    #[serde(deserialize_with = "bounded::children")]
    pub children: Vec<StreamDirectoryRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamDirectoryPage {
    Leaf(StreamDirectoryLeaf),
    Branch(StreamDirectoryBranch),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamRootIdentity {
    pub writer_epoch: u64,
    pub generation: u64,
}

/// Persisted progress is a checkpoint; current capacity comes from the chunk layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamActiveAttachment {
    pub chunk_id: ChunkId,
    pub logical_start: u64,
    pub checkpoint_physical: u64,
    pub checkpoint_logical: u64,
}

/// Bounded published head. Ordinary append progress may be ahead in its owner's view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDirectoryHead {
    pub stream_name: StreamName,
    pub purpose: StreamPurpose,
    pub metadata_group_id: u64,
    pub writer_epoch: u64,
    pub owner_session: [u8; 16],
    pub generation: u64,
    pub trim_offset: u64,
    pub published_tail: u64,
    pub root: Option<StreamDirectoryRef>,
    pub active: Option<StreamActiveAttachment>,
    pub previous: Option<StreamRootIdentity>,
    pub closed: bool,
}

#[derive(Debug, Error)]
pub enum StreamDirectoryError {
    #[error("stream directory page identity or fence does not match")]
    IdentityMismatch,
    #[error("stream directory references a different stream or chunk purpose")]
    WrongPurpose,
    #[error("stream directory bounds, coverage or geometry are invalid")]
    InvalidBounds,
    #[error("stream directory record is invalid: {0}")]
    Codec(String),
}
