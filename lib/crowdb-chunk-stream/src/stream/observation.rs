// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded views of the already-published stream manifest; no chunk reads.

use crate::{ActiveChunkDescriptor, Result, StreamError, StreamExtentPageFence};

use super::ChunkStream;

#[derive(Clone, Debug)]
pub struct StreamMetadataObservation {
    pub generation: u64,
    pub writer_epoch: u64,
    pub metadata_group_id: u64,
    pub trim_offset: u64,
    pub sealed_tail: u64,
    pub closed: bool,
    pub active: Option<ActiveChunkDescriptor>,
    pub extent_pages: Vec<StreamExtentPageFence>,
    pub offset: usize,
    pub next_offset: Option<usize>,
}

impl ChunkStream {
    /// Copies at most 100 extent-page fences from one published manifest.
    ///
    /// # Errors
    /// Rejects continuations without a generation, stale generations and invalid offsets.
    pub fn observe_metadata(
        &self,
        generation: Option<u64>,
        offset: usize,
    ) -> Result<StreamMetadataObservation> {
        let manifest = self.manifest.load();
        if generation.is_some_and(|value| value != manifest.generation) {
            return Err(StreamError::StaleWriter);
        }
        if (offset > 0 && generation.is_none()) || offset > manifest.extent_pages.len() {
            return Err(StreamError::InvalidRequest(
                "invalid extent index continuation".into(),
            ));
        }
        let end = offset.saturating_add(100).min(manifest.extent_pages.len());
        Ok(StreamMetadataObservation {
            generation: manifest.generation,
            writer_epoch: manifest.writer_epoch,
            metadata_group_id: manifest.metadata_group_id,
            trim_offset: manifest.trim_offset,
            sealed_tail: manifest.sealed_tail,
            closed: manifest.closed,
            active: manifest.active.clone(),
            extent_pages: manifest.extent_pages[offset..end].to_vec(),
            offset,
            next_offset: (end < manifest.extent_pages.len()).then_some(end),
        })
    }
}
