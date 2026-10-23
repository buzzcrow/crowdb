// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Published directory plus append-only local mappings since that checkpoint.

use std::ops::Bound::Included;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use crossbeam_skiplist::SkipMap;
use tokio::sync::oneshot;

use super::{durable_tail, ChunkStream, Command, Extent};
use crate::metadata::resolve_group;
use crate::{ExtentLocation, Result, StreamError, StreamManifest};

/// The source of a handle's finite or live read boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamViewSource {
    LiveOwner,
    OwnerCheckpoint,
    PersistedCheckpoint,
}

pub(super) struct ReadView {
    pub(super) manifest: Arc<StreamManifest>,
    extents: SkipMap<u64, Extent>,
    end: AtomicU64,
}

impl ReadView {
    pub(super) fn new(manifest: Arc<StreamManifest>) -> Self {
        Self {
            end: AtomicU64::new(durable_tail(&manifest).expect("validated checkpoint")),
            manifest,
            extents: SkipMap::new(),
        }
    }

    pub(super) fn end(&self) -> u64 {
        self.end.load(Ordering::Acquire)
    }

    pub(super) fn append(&self, extents: &[Extent], end: u64) {
        for extent in extents {
            self.extents.insert(extent.logical_start, extent.clone());
        }
        // Readers acquire the end only after every immutable mapping is visible.
        self.end.store(end, Ordering::Release);
    }

    pub(super) fn resolve(&self, offset: u64) -> Result<ExtentLocation> {
        let entry = self
            .extents
            .upper_bound(Included(&offset))
            .ok_or_else(|| StreamError::Corruption("local append mapping is absent".into()))?;
        let extent = entry.value();
        resolve_group(
            extent.chunk_id,
            extent.logical_start,
            extent.logical_end,
            extent.frame_start,
            offset,
        )
    }
}

impl ChunkStream {
    #[must_use]
    pub fn view_source(&self) -> StreamViewSource {
        self.view_source
    }

    /// Orders a directory checkpoint after earlier queued appends and captures
    /// that finite view. Later appends do not extend the returned handle.
    ///
    /// # Errors
    /// Returns a publication, stale-writer or stalled-worker error.
    pub async fn capture_read_only(&self) -> Result<Self> {
        if self.view_source != StreamViewSource::LiveOwner {
            return Ok(self.clone());
        }
        if self.closed.load(Ordering::Acquire) {
            let view = self.read_view.load_full();
            return if view.manifest.closed {
                Ok(self.captured(view))
            } else {
                Err(StreamError::WriteStalled)
            };
        }
        let (completion, result) = oneshot::channel();
        self.sender
            .send(Command::Capture(completion))
            .await
            .map_err(|_| StreamError::WriteStalled)?;
        let view = result.await.map_err(|_| StreamError::WriteStalled)??;
        Ok(self.captured(view))
    }

    fn captured(&self, view: Arc<ReadView>) -> Self {
        let mut reader = self.clone();
        reader.manifest = Arc::new(ArcSwap::from(Arc::clone(&view.manifest)));
        reader.read_view = Arc::new(ArcSwap::from(view));
        reader.closed = Arc::new(AtomicBool::new(true));
        reader.view_source = StreamViewSource::OwnerCheckpoint;
        reader
    }

    /// Persists the finite directory prefix captured by an ordered owner barrier.
    /// # Errors
    /// Returns the same errors as `capture_read_only`.
    pub async fn checkpoint(&self) -> Result<u64> {
        Ok(self.capture_read_only().await?.tail())
    }
}
