// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Snapshot reads and bounded sequential prefetch.

use super::{ChunkStream, PhysicalRead, ReadHint, ReadSegment, ReadView};
use crate::metadata::resolve_group;

use crate::perf::{perf, PhaseTimer};
use crate::{ChunkId, Result, StreamError};
use bytes::{Bytes, BytesMut};
use crowdb_protocol::frame::{parse_frame, FrameLayout, FrameMagic, FrameRole, FRAME_GROUP_HEADER_BYTES};
use std::sync::atomic::Ordering;
use std::time::Instant;

impl ChunkStream {
    /// Reads an exact retained durable logical range.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid range, corrupt metadata, or unavailable data.
    pub async fn read_at(&self, offset: u64, length: usize) -> Result<Bytes> {
        let segments = self.read_at_with_provenance(offset, length).await?;
        let mut output = BytesMut::with_capacity(length);
        for segment in segments {
            output.extend_from_slice(&segment.data);
        }
        Ok(output.freeze())
    }

    /// Reads an exact range while retaining the physical chunk identity for
    /// every returned logical segment.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid range, corrupt metadata, or unavailable data.
    pub async fn read_at_with_provenance(&self, offset: u64, length: usize) -> Result<Vec<ReadSegment>> {
        let _timer = PhaseTimer::start(&perf().read);
        let plan_timer = PhaseTimer::start(&perf().read_plan);
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| StreamError::InvalidRequest("read range overflows".into()))?;
        let view = self.read_view.load_full();
        view.pin.check()?;
        let state = &view.state;
        if offset < state.trim_offset || end > view.end() {
            return Err(StreamError::InvalidRequest(
                "read is outside retained durable range".into(),
            ));
        }
        let mut cursor = offset;
        let mut reads = Vec::new();
        while cursor < end {
            let location = if cursor >= state.published_tail {
                view.resolve(cursor)?
            } else {
                let (record, cached) = self.metadata.locate_for_read(state.stream_name, cursor).await?;
                let counter = if cached {
                    &self.metrics.record_cache_hits
                } else {
                    &self.metrics.record_cache_misses
                };
                counter.fetch_add(1, Ordering::Relaxed);
                if record.record().purpose != state.purpose {
                    return Err(StreamError::Corruption(
                        "Stream chunk record purpose differs".into(),
                    ));
                }
                let mapping = record
                    .index()
                    .locate(&record.record().mappings, cursor)
                    .map_err(|error| StreamError::Corruption(error.to_string()))?;
                resolve_group(
                    record.record().chunk_id,
                    mapping.logical_start,
                    mapping.logical_end,
                    mapping.physical_start,
                    cursor,
                )?
            };
            let read_len = usize::try_from(location.available.min(end - cursor))
                .map_err(|_| StreamError::InvalidRequest("read length exceeds addressable range".into()))?;
            let read_len = read_len.min(self.config.read_request_bytes);
            reads.push(PhysicalRead {
                chunk_id: location.chunk_id,
                logical_start: cursor,
                frame_start: location.frame_offset,
                frame_length: usize::try_from(location.frame_length).map_err(|_| {
                    StreamError::Corruption("stream frame length exceeds address space".into())
                })?,
                payload_start: usize::try_from(location.payload_offset).map_err(|_| {
                    StreamError::Corruption("stream frame payload offset exceeds address space".into())
                })?,
                length: read_len,
                group_start: location.group_offset,
                group_length: location.group_length,
                ordinal: location.ordinal,
            });
            cursor += read_len as u64;
        }
        drop(plan_timer);
        let io_timer = PhaseTimer::start(&perf().read_io);
        let output = self.read_physical_ranges(&reads, &view).await?;
        view.pin.check()?;
        drop(io_timer);
        let output_len = output
            .iter()
            .fold(0_usize, |total, segment| total.saturating_add(segment.data.len()));
        if output_len != length {
            return Err(StreamError::Corruption(
                "chunk reads returned an unexpected length".into(),
            ));
        }
        self.metrics
            .read_bytes
            .fetch_add(length as u64, Ordering::Relaxed);
        Ok(output)
    }

    async fn read_physical_ranges(
        &self,
        reads: &[PhysicalRead],
        view: &std::sync::Arc<ReadView>,
    ) -> Result<Vec<ReadSegment>> {
        self.metrics
            .physical_read_requests
            .fetch_add(reads.len() as u64, Ordering::Relaxed);
        if let [read] = reads {
            return Ok(vec![self.read_physical(*read, view).await?]);
        }
        let mut output = vec![None; reads.len()];
        let mut tasks = tokio::task::JoinSet::new();
        let mut next = 0;
        while next < reads.len() || !tasks.is_empty() {
            while next < reads.len() && tasks.len() < self.config.read_concurrency {
                let index = next;
                let read = reads[index];
                let stream = self.clone();
                let view = std::sync::Arc::clone(view);
                tasks.spawn(async move {
                    stream
                        .read_physical(read, &view)
                        .await
                        .map(|value| (index, value))
                });
                next += 1;
            }
            let (index, value) = tasks
                .join_next()
                .await
                .ok_or_else(|| StreamError::Internal("physical read task set ended early".into()))?
                .map_err(|error| StreamError::Internal(format!("physical read task failed: {error}")))??;
            output[index] = Some(value);
        }
        output
            .into_iter()
            .map(|value| value.ok_or_else(|| StreamError::Internal("physical read result is missing".into())))
            .collect()
    }

    async fn read_physical(&self, read: PhysicalRead, view: &ReadView) -> Result<ReadSegment> {
        view.pin.check()?;
        let group_bytes = FrameLayout::StreamGroupV2
            .physical_length(read.group_length)
            .map_err(|error| StreamError::Corruption(error.to_string()))?;
        let synchronized_end = read
            .group_start
            .checked_add(group_bytes)
            .ok_or_else(|| StreamError::Corruption("captured group boundary overflows".into()))?;
        let frame = self
            .read_chunk_with_watchdog(
                read.chunk_id,
                read.frame_start,
                read.frame_length,
                read.logical_start,
                synchronized_end,
                &view.pin,
            )
            .await?;
        decode_segment(read, &frame)
    }

    /// Creates a bounded sequential reader ending at the current durable tail.
    ///
    /// # Errors
    ///
    /// Returns an error if `offset` is outside the retained durable range.
    pub fn read_from(&self, offset: u64) -> Result<StreamReader> {
        self.reader(offset, ReadHint::ToEnd)
    }

    /// Creates a seekable reader with a finite or captured-tail hint.
    ///
    /// # Errors
    ///
    /// Returns an error if `offset` or its hint is outside the retained range.
    pub fn reader(&self, offset: u64, hint: ReadHint) -> Result<StreamReader> {
        let view = self.read_view.load();
        let state = &view.state;
        let tail = view.end();
        if offset < state.trim_offset || offset > tail {
            return Err(StreamError::InvalidRequest(
                "read cursor is outside retained range".into(),
            ));
        }
        let end = match hint {
            ReadHint::ToEnd => tail,
            ReadHint::Bytes(length) => offset
                .checked_add(length)
                .ok_or_else(|| StreamError::InvalidRequest("reader hint overflows".into()))?
                .min(tail),
        };
        let mut captured = self.clone();
        captured.state = std::sync::Arc::new(arc_swap::ArcSwap::from(std::sync::Arc::clone(&view.state)));
        captured.read_view = std::sync::Arc::new(arc_swap::ArcSwap::from(std::sync::Arc::clone(&view)));
        Ok(StreamReader {
            stream: captured,
            offset,
            end,
            pending: None,
        })
    }

    async fn read_chunk_with_watchdog(
        &self,
        chunk_id: ChunkId,
        physical_offset: u64,
        length: usize,
        logical_offset: u64,
        synchronized_end: u64,
        pin: &crate::StreamReadPin,
    ) -> Result<Bytes> {
        let read = self
            .chunks
            .read_verified_frame(chunk_id, physical_offset, length, synchronized_end);
        tokio::pin!(read);
        let started = Instant::now();
        loop {
            tokio::select! {
                result = &mut read => return result,
                () = tokio::time::sleep(pin.remaining()) => pin.check()?,
                () = tokio::time::sleep(self.config.watchdog_interval) => {
                    pin.check()?;
                    self.metrics.watchdog_observations.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        stream_high = self.stream_name.high,
                        stream_low = self.stream_name.low,
                        writer_epoch = self.writer_epoch,
                        logical_offset,
                        read_bytes = length,
                        operation_age_ms = started.elapsed().as_millis(),
                        stage = "chunk_read",
                        "chunk-stream read window remains in flight"
                    );
                }
            }
        }
    }
}

fn decode_segment(read: PhysicalRead, frame: &Bytes) -> Result<ReadSegment> {
    let parsed = parse_frame(frame, read.chunk_id)
        .map_err(|error| StreamError::Corruption(format!("invalid stream frame: {error}")))?;
    let expected_role = if read.ordinal == 0 {
        FrameRole::Head {
            logical_length: read.group_length,
            frame_count: u32::try_from(
                read.group_length
                    .div_ceil(FrameLayout::StreamGroupV2.payload_capacity()),
            )
            .map_err(|_| StreamError::Corruption("stream group count exceeds u32".into()))?,
        }
    } else {
        FrameRole::Follower {
            head_offset: read.group_start,
            ordinal: read.ordinal,
        }
    };
    if parsed.header.magic != FrameMagic::StreamV2
        || parsed.role != expected_role
        || parsed.physical_length != read.frame_length
    {
        return Err(StreamError::Corruption(
            "stream extent has the wrong frame kind".into(),
        ));
    }
    let payload_end = read
        .payload_start
        .checked_add(read.length)
        .ok_or_else(|| StreamError::Corruption("stream frame payload range overflows".into()))?;
    if read.payload_start < FRAME_GROUP_HEADER_BYTES
        || payload_end > FRAME_GROUP_HEADER_BYTES + parsed.payload.len()
    {
        return Err(StreamError::Corruption(
            "stream extent exceeds frame payload".into(),
        ));
    }
    let data = frame.slice(read.payload_start..payload_end);
    Ok(ReadSegment {
        chunk_id: read.chunk_id,
        logical_start: read.logical_start,
        data,
    })
}

type PrefetchResult = tokio::task::JoinHandle<Result<(u64, Vec<ReadSegment>)>>;

pub struct StreamReader {
    pub(super) stream: ChunkStream,
    pub(super) offset: u64,
    pub(super) end: u64,
    pub(super) pending: Option<PrefetchResult>,
}

impl StreamReader {
    /// Moves the reader within its captured retained range.
    ///
    /// # Errors
    ///
    /// Returns an error when `offset` is outside the captured range.
    pub fn seek(&mut self, offset: u64) -> Result<()> {
        let trim = self.stream.state.load().trim_offset;
        if offset < trim || offset > self.end {
            return Err(StreamError::InvalidRequest(
                "seek is outside the reader range".into(),
            ));
        }
        if let Some(pending) = self.pending.take() {
            pending.abort();
        }
        self.offset = offset;
        Ok(())
    }

    /// Returns the next bounded logical window, or `None` at the captured tail.
    ///
    /// # Errors
    ///
    /// Returns a typed metadata or chunk-read error.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        let Some(segments) = self.next_with_provenance().await? else {
            return Ok(None);
        };
        let length = segments.iter().map(|segment| segment.data.len()).sum();
        let mut bytes = BytesMut::with_capacity(length);
        for segment in segments {
            bytes.extend_from_slice(&segment.data);
        }
        Ok(Some(bytes.freeze()))
    }

    /// Returns the next bounded logical window split by physical chunk.
    ///
    /// # Errors
    ///
    /// Returns a typed metadata, task, or chunk-read error.
    pub async fn next_with_provenance(&mut self) -> Result<Option<Vec<ReadSegment>>> {
        if self.offset == self.end {
            return Ok(None);
        }
        self.start_prefetch()?;
        let pending = self
            .pending
            .take()
            .ok_or_else(|| StreamError::Internal("reader prefetch was not installed".into()))?;
        let (offset, segments) = pending
            .await
            .map_err(|error| StreamError::Internal(format!("reader prefetch task failed: {error}")))??;
        if offset != self.offset {
            return Err(StreamError::Internal(
                "reader prefetch completed for a stale offset".into(),
            ));
        }
        let length = segments.iter().try_fold(0_u64, |length, segment| {
            length.checked_add(segment.data.len() as u64)
        });
        self.offset = self
            .offset
            .checked_add(length.ok_or_else(|| StreamError::Corruption("reader length overflows".into()))?)
            .ok_or_else(|| StreamError::Corruption("reader offset overflows".into()))?;
        self.start_prefetch()?;
        Ok(Some(segments))
    }

    fn start_prefetch(&mut self) -> Result<()> {
        if self.pending.is_some() || self.offset == self.end {
            return Ok(());
        }
        let offset = self.offset;
        let length = usize::try_from((self.end - offset).min(self.stream.config.read_window_bytes as u64))
            .map_err(|_| StreamError::InvalidRequest("read window exceeds addressable range".into()))?;
        let stream = self.stream.clone();
        self.pending = Some(tokio::spawn(async move {
            stream
                .read_at_with_provenance(offset, length)
                .await
                .map(|segments| (offset, segments))
        }));
        Ok(())
    }
}
