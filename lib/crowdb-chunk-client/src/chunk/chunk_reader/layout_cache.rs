// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded frame-read reuse of the original server-granted layout deadline.

use arc_swap::ArcSwapOption;
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;

use super::{ChunkReader, LayoutSnapshot, StripFailureObservation};
use crate::ReadResult;

const FRAME_LAYOUT_CACHE_SLOTS: usize = 64;

pub(super) struct FrameLayoutCache {
    slots: [ArcSwapOption<LayoutSnapshot>; FRAME_LAYOUT_CACHE_SLOTS],
}

impl Default for FrameLayoutCache {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| ArcSwapOption::empty()),
        }
    }
}

impl FrameLayoutCache {
    fn slot(&self, id: ChunkId) -> &ArcSwapOption<LayoutSnapshot> {
        let hash = id.high ^ id.low.rotate_left(17) ^ id.low;
        &self.slots[(hash % FRAME_LAYOUT_CACHE_SLOTS as u64) as usize]
    }

    fn get(&self, id: ChunkId, required_end: u64, synchronized: bool) -> Option<LayoutSnapshot> {
        self.slot(id)
            .load_full()
            .filter(|layout| {
                layout.chunk_id == id
                    && layout.usable()
                    && if synchronized {
                        layout.chunk.strips.last().is_some_and(|strip| {
                            u64::from(strip.chunk_offset + strip.capacity) * 1024 >= required_end
                        })
                    } else {
                        layout.chunk.acknowledged_cursor >= required_end
                    }
            })
            .map(|layout| (*layout).clone())
    }

    fn insert(&self, layout: &LayoutSnapshot) {
        self.slot(layout.chunk_id).store(Some(Arc::new(layout.clone())));
    }
}

impl LayoutSnapshot {
    pub(super) fn has_new_failures(&self, observations: &[StripFailureObservation]) -> bool {
        observations.iter().any(|observation| {
            !observation.failed_segments.is_empty()
                && self
                    .chunk
                    .strips
                    .iter()
                    .find(|strip| strip.strip_sequence == observation.strip_sequence)
                    .map_or(true, |strip| {
                        observation
                            .failed_segments
                            .iter()
                            .any(|segment| !strip.unavailable_segments.contains(segment))
                    })
        })
    }
}

impl ChunkReader {
    pub(super) async fn frame_layout(
        &self,
        id: ChunkId,
        required_end: u64,
        synchronized: bool,
    ) -> ReadResult<LayoutSnapshot> {
        if let Some(layout) = self.frame_layouts.get(id, required_end, synchronized) {
            self.flow_metrics.layout_cache_hits.inc();
            return Ok(layout);
        }
        let layout = self.query_layout(id).await?;
        self.frame_layouts.insert(&layout);
        Ok(layout)
    }
}
