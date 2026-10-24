// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{io_error, ProductionStreamChunkStore};
use crate::perf::{perf, PhaseTimer};
use crate::{Result, StreamError, StreamWriteContext};
use crowdb_protocol::chunkdb::rpc::{Chunk, Strip};
use futures::{stream, StreamExt};

impl ProductionStreamChunkStore {
    pub(super) async fn drain_for_seal(&self, chunk: &Chunk, context: &StreamWriteContext) -> Result<()> {
        let _timer = PhaseTimer::start(&perf().seal_drain);
        let mut segments = Vec::new();
        for strip in &chunk.strips {
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return Err(StreamError::Corruption(
                    "stream seal has non-mirror layout".into(),
                ));
            };
            segments.extend(
                mirror
                    .segments
                    .iter()
                    .map(|segment| (*segment, u64::from(strip.unit_kb) * 1024)),
            );
        }
        // Each segment has its own ordered IO stream. Wait for every submitted
        // drain, including after a peer fails, before returning to its lifecycle.
        let results = stream::iter(segments)
            .map(|(segment, unit_bytes)| async move {
                context.check_io()?;
                self.disk_writer
                    .drain_segment(&segment, unit_bytes, context.deadline())
                    .await
                    .map_err(io_error)
            })
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result?;
        }
        context.check_io()
    }
}
