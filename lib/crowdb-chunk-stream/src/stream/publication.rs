// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional manifest and immutable directory publication.

use super::{Extent, ReadView, WorkerState};
use crate::perf::{perf, PhaseTimer};
use crate::{Result, StreamError, StreamExtentPage, StreamExtentPageFence, StreamName};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

pub(super) async fn publish_state(state: &mut WorkerState) -> Result<()> {
    state
        .authority
        .as_ref()
        .ok_or(StreamError::WriteStalled)?
        .check()?;
    super::checkpoint::flush(state).await?;
    let _timer = PhaseTimer::start(&perf().publish);
    let build_timer = PhaseTimer::start(&perf().directory_build);
    let expected = state.manifest.generation;
    state.manifest.previous_generation = Some(expected);
    state.manifest.generation = expected + 1;
    let pages = build_extent_pages(
        &state.extents,
        state.stream_name,
        state.writer_epoch,
        state.manifest.generation,
        state.config.extent_page_entries,
    );
    state.manifest.extent_pages = fences_for(&pages);
    drop(build_timer);
    loop {
        state
            .authority
            .as_ref()
            .ok_or(StreamError::WriteStalled)?
            .check()?;
        match state
            .metadata
            .publish(
                Some((state.writer_epoch, expected)),
                state.manifest.clone(),
                pages.clone(),
            )
            .await
        {
            Ok(()) => break,
            Err(error) => {
                match state.metadata.load_current(state.stream_name).await {
                    Ok(Some(current)) if current == state.manifest => break,
                    Ok(Some(current))
                        if current.writer_epoch == state.writer_epoch && current.generation == expected =>
                    {
                        if matches!(error, StreamError::InvalidRequest(_) | StreamError::Corruption(_)) {
                            state.stalled = true;
                            return Err(error);
                        }
                    }
                    Ok(Some(current)) if current.writer_epoch > state.writer_epoch => {
                        state.stalled = true;
                        return Err(StreamError::StaleWriter);
                    }
                    Ok(_) => {
                        state.stalled = true;
                        return Err(StreamError::Corruption(
                            "stream manifest head differs from both prior and candidate state".into(),
                        ));
                    }
                    Err(load_error) => {
                        tracing::warn!(%load_error, "chunk-stream manifest publication remains unresolved");
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    state
        .metrics
        .metadata_publications
        .fetch_add(1, Ordering::Relaxed);
    let manifest = Arc::new(state.manifest.clone());
    state
        .read_view
        .store(Arc::new(ReadView::new(Arc::clone(&manifest))));
    state.manifest_view.store(manifest);
    Ok(())
}

pub(super) fn fences_for(pages: &[StreamExtentPage]) -> Vec<StreamExtentPageFence> {
    pages
        .iter()
        .map(|page| StreamExtentPageFence {
            page_index: page.page_index,
            first_logical: page.logical_offsets[0],
            end_logical: *page
                .logical_offsets
                .last()
                .expect("built extent page is nonempty"),
        })
        .collect()
}

pub(super) fn build_extent_pages(
    extents: &[Extent],
    stream_name: StreamName,
    writer_epoch: u64,
    generation: u64,
    page_entries: usize,
) -> Vec<StreamExtentPage> {
    extents
        .chunks(page_entries)
        .map(|chunk| {
            let mut logical_offsets = Vec::with_capacity(chunk.len() + 1);
            logical_offsets.push(chunk[0].logical_start);
            for extent in chunk {
                logical_offsets.push(extent.logical_end);
            }
            StreamExtentPage {
                stream_name,
                writer_epoch,
                generation,
                page_index: chunk[0].logical_start,
                chunk_ids: chunk.iter().map(|extent| extent.chunk_id).collect(),
                logical_offsets,
                physical_offsets: chunk.iter().map(|extent| extent.frame_start).collect(),
                frame_lengths: chunk.iter().map(|extent| extent.frame_length).collect(),
            }
        })
        .collect()
}
