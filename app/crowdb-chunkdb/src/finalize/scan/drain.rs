// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Ordered completion of old physical writes before an abandoned-tail scan.

use crowdb_protocol::chunkdb::rpc::{Chunk, Strip};
use futures::{stream, StreamExt, TryStreamExt};
use tokio::time::{timeout_at, Instant};

use crate::conversion::io::ConversionDiskIo;

pub(super) async fn drain_layout(
    io: &ConversionDiskIo,
    chunk: &Chunk,
    deadline: Instant,
) -> Result<(), String> {
    let targets = chunk.strips.iter().flat_map(|strip| {
        let segments = match &strip.strip {
            Some(Strip::MirrorStrip(mirror)) => mirror.segments.as_slice(),
            Some(Strip::EcStrip(ec)) => ec.segments.as_slice(),
            None => &[],
        };
        segments
            .iter()
            .copied()
            .map(move |segment| (segment, u64::from(strip.unit_kb) * 1024))
    });
    let drain =
        stream::iter(targets)
            .map(Ok)
            .try_for_each_concurrent(8, |(segment, unit_bytes)| async move {
                io.drain_segment(&segment, unit_bytes, deadline.into_std())
                    .await
                    .map_err(|error| format!("cannot drain physical segment before finalization: {error}"))
            });
    timeout_at(deadline, drain)
        .await
        .map_err(|_| "physical segment drain deadline expired".to_string())?
}
