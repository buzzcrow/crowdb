// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded object-body handoff shared by S3 and Iceberg uploads.

use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use crowdb_chunk_client::{ChunkIoWriter, FramedWriteBuffer, IoError};
use hyper::body::Bytes;
use tokio::sync::mpsc;

pub(crate) mod digest_pipe;

pub(crate) enum UploadBuffer {
    Framed(Box<dyn FramedWriteBuffer>),
    Data(Bytes),
}

pub(crate) enum OfferStatus {
    Continue,
    Pause,
}

pub(crate) struct WriteFlow<'a> {
    sender: mpsc::Sender<UploadBuffer>,
    progress: &'a AtomicU64,
}

impl<'a> WriteFlow<'a> {
    pub(crate) fn new(sender: mpsc::Sender<UploadBuffer>, progress: &'a AtomicU64) -> Self {
        Self { sender, progress }
    }

    pub(crate) async fn offer(&self, buffer: UploadBuffer) -> Result<OfferStatus, ()> {
        self.sender.send(buffer).await.map_err(|_| ())?;
        self.progress.fetch_add(1, Ordering::Relaxed);
        Ok(if self.sender.capacity() == 0 {
            OfferStatus::Pause
        } else {
            OfferStatus::Continue
        })
    }

    pub(crate) async fn wait_ready(&self) -> Result<(), ()> {
        let permit = self.sender.reserve().await.map_err(|_| ())?;
        drop(permit);
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct WriteStats {
    pub(crate) feeds: u64,
    pub(crate) feed_time: Duration,
    pub(crate) capacity_waits: u64,
    pub(crate) capacity_wait_time: Duration,
}

pub(crate) async fn drive_transfer<R, W, E>(
    receive: R,
    write: W,
    progress: &AtomicU64,
) -> Result<(u64, WriteStats), E>
where
    R: Future<Output = Result<u64, E>>,
    W: Future<Output = Result<WriteStats, E>>,
{
    let mut receive = Some(Box::pin(receive));
    let mut write = Some(Box::pin(write));
    let mut length = None;
    let mut written = None;
    poll_fn(|cx| {
        for _ in 0..32 {
            let before = progress.load(Ordering::Relaxed);
            if let Some(Poll::Ready(result)) = receive.as_mut().map(|future| future.as_mut().poll(cx)) {
                match result {
                    Ok(value) => length = Some(value),
                    Err(error) => return Poll::Ready(Err(error)),
                }
                receive = None;
            }
            if let Some(Poll::Ready(result)) = write.as_mut().map(|future| future.as_mut().poll(cx)) {
                match result {
                    Ok(value) => written = Some(value),
                    Err(error) => return Poll::Ready(Err(error)),
                }
                write = None;
            }
            if let Some(length) = length {
                if let Some(written) = written.take() {
                    return Poll::Ready(Ok((length, written)));
                }
            }
            if progress.load(Ordering::Relaxed) == before {
                return Poll::Pending;
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

pub(crate) async fn write_buffers<W, E>(
    writer: &mut W,
    mut receiver: mpsc::Receiver<UploadBuffer>,
    progress: &AtomicU64,
    map_error: fn(IoError) -> E,
) -> Result<WriteStats, E>
where
    W: ChunkIoWriter + ?Sized,
{
    let mut stats = WriteStats::default();
    loop {
        while !writer.require_data() && !writer.input_complete() {
            let started = Instant::now();
            writer.wait_for_capacity().await;
            stats.capacity_waits += 1;
            stats.capacity_wait_time += started.elapsed();
        }
        let Some(buffer) = receiver.recv().await else {
            break;
        };
        progress.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        match buffer {
            UploadBuffer::Framed(owner) => writer.on_framed_data(owner).await,
            UploadBuffer::Data(bytes) => writer.on_data(bytes).await,
        }
        .map_err(map_error)?;
        stats.feed_time += started.elapsed();
        stats.feeds += 1;
    }
    Ok(stats)
}
