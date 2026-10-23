// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded immutable directory traversal and path-copy mutations.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    decode_stream_directory_page, encode_stream_directory_page, StreamDirectoryBranch, StreamDirectoryLeaf,
    StreamDirectoryPage, StreamDirectoryPageId, StreamDirectoryRef, StreamExtentLocation, StreamName,
    StreamPurpose, MAX_STREAM_DIRECTORY_CHILDREN, MAX_STREAM_DIRECTORY_HEIGHT,
    MAX_STREAM_DIRECTORY_LOCATIONS,
};

use crate::{Result, StreamError};

mod append;
mod metadata;
pub(crate) use metadata::DirectoryRecords;
pub use metadata::{
    DirectoryCacheSnapshot, DirectoryHeadSnapshot, DirectoryKeyScan, DirectoryMetadataStore,
    DirectoryRootPinSnapshot, DIRECTORY_PAGE_CACHE_SLOTS, MAX_DIRECTORY_SCAN_KEYS,
};
mod publication;
mod read;
mod trim;

pub use read::DirectoryScan;

/// Upper bound on one prepared checkpoint. Larger tails are checkpointed in parts.
pub const MAX_DIRECTORY_MUTATION_EXTENTS: usize = 64 * MAX_STREAM_DIRECTORY_LOCATIONS;

/// Read access to independently addressed records. The index verifies every record.
#[async_trait]
pub trait StreamDirectoryStore: Send + Sync {
    async fn load_directory_record(
        &self,
        stream_name: StreamName,
        page_id: StreamDirectoryPageId,
    ) -> Result<Option<Bytes>>;
}

#[derive(Clone, Debug)]
pub struct DirectoryIndex {
    stream_name: StreamName,
    purpose: StreamPurpose,
    root: Option<StreamDirectoryRef>,
}

/// Prepared immutable records; creation does not write or publish any KV state.
/// Publication must protect these records before storing them and replacing a root.
pub struct DirectoryMutation {
    index: DirectoryIndex,
    base: Option<StreamDirectoryRef>,
    pages: Vec<DirectoryRecord>,
}

pub struct DirectoryRecord {
    reference: StreamDirectoryRef,
    bytes: Bytes,
}

impl DirectoryRecord {
    #[must_use]
    pub const fn reference(&self) -> StreamDirectoryRef {
        self.reference
    }

    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

impl DirectoryMutation {
    #[must_use]
    pub const fn index(&self) -> &DirectoryIndex {
        &self.index
    }

    #[must_use]
    pub const fn base(&self) -> Option<StreamDirectoryRef> {
        self.base
    }

    #[must_use]
    pub fn pages(&self) -> &[DirectoryRecord] {
        &self.pages
    }

    fn record(&mut self, page: &StreamDirectoryPage) -> Result<StreamDirectoryRef> {
        let (reference, bytes) = encode_stream_directory_page(page).map_err(invalid_directory)?;
        self.pages.push(DirectoryRecord {
            reference,
            bytes: Bytes::from(bytes),
        });
        Ok(reference)
    }

    fn leaf(&mut self, locations: Vec<StreamExtentLocation>) -> Result<StreamDirectoryRef> {
        self.record(&StreamDirectoryPage::Leaf(StreamDirectoryLeaf {
            stream_name: self.index.stream_name,
            purpose: self.index.purpose,
            locations,
        }))
    }

    fn branch(&mut self, level: u8, children: Vec<StreamDirectoryRef>) -> Result<StreamDirectoryRef> {
        self.record(&StreamDirectoryPage::Branch(StreamDirectoryBranch {
            stream_name: self.index.stream_name,
            purpose: self.index.purpose,
            level,
            children,
        }))
    }
}

impl DirectoryIndex {
    /// # Errors
    /// Rejects malformed root fences before any IO.
    pub fn new(
        stream_name: StreamName,
        purpose: StreamPurpose,
        root: Option<StreamDirectoryRef>,
    ) -> Result<Self> {
        if let Some(root) = root {
            root.validate().map_err(invalid_directory)?;
        }
        Ok(Self {
            stream_name,
            purpose,
            root,
        })
    }

    #[must_use]
    pub const fn root(&self) -> Option<StreamDirectoryRef> {
        self.root
    }

    #[must_use]
    pub const fn stream_name(&self) -> StreamName {
        self.stream_name
    }

    #[must_use]
    pub const fn purpose(&self) -> StreamPurpose {
        self.purpose
    }

    fn mutation(&self) -> DirectoryMutation {
        DirectoryMutation {
            index: self.clone(),
            base: self.root,
            pages: Vec::new(),
        }
    }

    async fn load(
        &self,
        store: &dyn StreamDirectoryStore,
        reference: StreamDirectoryRef,
    ) -> Result<StreamDirectoryPage> {
        let bytes = store
            .load_directory_record(self.stream_name, reference.id)
            .await?
            .ok_or_else(|| StreamError::ReadUnavailable("directory page is absent".into()))?;
        decode_stream_directory_page(&bytes, reference, self.stream_name, self.purpose)
            .map_err(invalid_directory)
    }
}

fn invalid_directory(error: impl std::fmt::Display) -> StreamError {
    StreamError::Corruption(format!("invalid stream directory: {error}"))
}

fn checked_join(previous: StreamExtentLocation, next: StreamExtentLocation) -> Result<()> {
    let logical = previous.end_logical().map_err(invalid_directory)?;
    let physical = previous.end_physical().map_err(invalid_directory)?;
    if logical != next.logical_start
        || if previous.location.chunk_id == next.location.chunk_id {
            physical != next.location.frame_offset
        } else {
            next.location.frame_offset != 0
        }
    {
        return Err(invalid_directory("locations are not contiguous"));
    }
    Ok(())
}
