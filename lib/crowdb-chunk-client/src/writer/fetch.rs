// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `run_fetch_stage` — free function for the async-stream fetch stage.
//!
//! Pure IO glue — reads from `AsyncRead` in ≤ `read_buffer_size`
//! chunks, accumulates to full blocks, sends `Bytes` on the block
//! channel. On EOF, sends any partial last block, then returns (drops
//! the sender → drive loop sees EOF). No state, stays a free function.

use bytes::Bytes;
use bytes::BytesMut;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

#[derive(Debug, Default)]
pub struct FetchStats {
    pub source_reads: u64,
    pub source_read_time: Duration,
    pub assembly_copies: u64,
    pub assembly_copy_bytes: u64,
    pub assembly_copy_time: Duration,
}

/// Run the fetch stage: reads from `reader` in ≤ `read_buffer_size`
/// chunks, accumulates to full blocks, and sends `Bytes` to the block
/// channel. On EOF, sends any partial last block, then returns (drops
/// the sender → drive loop sees EOF).
pub async fn run_fetch_stage<R>(
    mut reader: R,
    block_tx: mpsc::Sender<Bytes>,
    read_buffer_size: usize,
) -> std::io::Result<FetchStats>
where
    R: tokio::io::AsyncRead + Unpin + Send,
{
    let mut buf = BytesMut::with_capacity(read_buffer_size);
    let mut stats = FetchStats::default();

    loop {
        buf.reserve(read_buffer_size.saturating_sub(buf.len()));
        let read_started = Instant::now();
        let result = reader.read_buf(&mut buf).await;
        stats.source_read_time += read_started.elapsed();
        match result {
            Ok(0) => break,
            Ok(_) => {
                stats.source_reads += 1;
                while buf.len() >= read_buffer_size {
                    let block = buf.split_to(read_buffer_size);
                    if block_tx.send(block.freeze()).await.is_err() {
                        return Ok(stats);
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }
    if !buf.is_empty() {
        let _ = block_tx.send(buf.freeze()).await;
    }
    Ok(stats)
}
