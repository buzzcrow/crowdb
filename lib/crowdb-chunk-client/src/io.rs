// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkIoWriter` — shared push-based async interface for chunk
//! data-path writers.

use std::ops::Range;

use bytes::Bytes;

use crate::Result;
use crowdb_protocol::chunkdb::rpc::Location as ProtoLocation;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{FrameError, FrameMagic};

/// One owner-backed logical payload whose physical frame regions can be
/// finalized in place and sliced into immutable write views.
pub trait FramedWriteBuffer: Send {
    /// Total logical payload bytes across every frame slot.
    fn logical_len(&self) -> u64;
    /// Number of populated physical-frame slots.
    fn frame_count(&self) -> usize;
    /// Logical payload bytes in one frame slot.
    fn frame_payload_len(&self, index: usize) -> Option<usize>;
    /// Fill one slot's reserved frame bytes for its actual destination chunk.
    fn finalize_frame(
        &mut self,
        index: usize,
        magic: FrameMagic,
        chunk_id: ChunkId,
        write_time_ms: u64,
    ) -> std::result::Result<Range<usize>, FrameError>;
    /// Return an immutable zero-copy view over a finalized physical range.
    fn view(&self, range: Range<usize>) -> std::result::Result<Bytes, FrameError>;
}

/// Result of `on_data` — does the writer need more data?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedStatus {
    /// Buffer stored; writer has capacity — send more data.
    Continue,
    /// Buffer stored; writer is at capacity — pause feeding.
    /// Poll `require_data()` before resuming.
    Pause,
}

/// Caller-side backpressure strategy. Selects how to react when
/// `require_data()` returns false. Not a property of the writer.
#[derive(Debug, Clone, Copy)]
pub enum BackpressurePolicy {
    /// Dedicated upload task: ignore `require_data`, call `on_data`
    /// directly — it blocks until capacity. Use when the task has
    /// nothing else to do.
    Blocking,
    /// Shared handler task: check `require_data()` first; if false,
    /// return 503 / apply TCP flow control — never block the handler.
    NonBlocking,
}

/// Push-based chunk IO writer trait.
///
/// Contract:
/// - `on_data` **always stores the buffer** after it returns `Ok` (awaits until
///   internal capacity is available). A terminal validation error may reject
///   the offending buffer. Returns `Continue` if the
///   next push would not block, `Pause` if the writer is now at
///   capacity.
/// - `require_data` is a cheap non-async hint: `true` if `on_data`
///   would not block now.
/// - `on_finish` signals end of input: flush, seal, return
///   `Vec<Location>`.
/// - `on_error` aborts: return `Location`s of already-sealed chunks
///   for cleanup.
///
/// Two caller strategies (selected by `BackpressurePolicy` on the
/// caller side):
/// - **Blocking**: ignore `require_data`, call `on_data` directly.
/// - **Non-blocking**: check `require_data()` first; if false, back
///   off (yield / return 503).
#[async_trait::async_trait]
pub trait ChunkIoWriter: Send {
    /// Push a data buffer. An `Ok` result always means the buffer was stored.
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus>;
    /// Push one owner-backed framed buffer. Large writers override this path;
    /// other writers reject it rather than copying framed bytes back out.
    async fn on_framed_data(&mut self, _buffer: Box<dyn FramedWriteBuffer>) -> Result<FeedStatus> {
        Err(crate::IoError::WriteFailed(
            "writer does not accept owner-backed framed data".into(),
        ))
    }
    /// End of input: flush, seal, return the `Location` array.
    async fn on_finish(&mut self) -> Result<Vec<ProtoLocation>>;
    /// Abort: return `Location`s of already-sealed chunks for cleanup.
    async fn on_error(&mut self) -> Result<Vec<ProtoLocation>>;
    /// Non-async pre-check: `true` if `on_data` would not block now.
    fn require_data(&self) -> bool;
    /// True only when a writer with a trusted declared length has accepted its
    /// complete input. Callers may finish without polling a further body frame.
    fn input_complete(&self) -> bool {
        false
    }
    /// Wait for a capacity change without polling more network input. Writers
    /// with no external notifier use the short default recheck.
    async fn wait_for_capacity(&mut self) {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}
